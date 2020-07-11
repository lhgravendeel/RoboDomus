package nl.gravendeeldesign.robodomus;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigValue;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import java.io.File;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple application to receive zigbee2mqtt button presses and convert it into lamp actions.
 */
public class Application {

  /**
   * Queue of outbound msgs.
   */
  BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(1024);

  IMqttClient client;

  boolean isRunning = true;

  Config config;

  public static void main(String[] args) throws Exception {
    new Application().run();
  }

  Application() throws Exception {
    File myConfigFile = new File("robodomus.conf");
    Config fileConfig = ConfigFactory.parseFile(myConfigFile);
    config = ConfigFactory.load(fileConfig);
  }

  /**
   * Main run loop.
   * @throws Exception
   */
  public void run() throws Exception {
    String publisherId = UUID.randomUUID().toString();
    client = new MqttClient(config.getString("mqtt.host"), publisherId, new MemoryPersistence());

    MqttConnectOptions options = new MqttConnectOptions();
    options.setAutomaticReconnect(true);
    options.setCleanSession(true);
    options.setConnectionTimeout(60);
    options.setKeepAliveInterval(30);
    options.setAutomaticReconnect(true);
    String user = config.getString("mqtt.user");
    String password = config.getString("mqtt.password");
    if(user != null && !user.trim().isEmpty()) {
      options.setUserName(user);
    }
    if(password != null && !password.trim().isEmpty()) {
      options.setPassword(password.toCharArray());
    }
    client.connect(options);

    startSendThread();
    ConfigObject groups = config.getObject("groups");
    for(Map.Entry<String, ConfigValue> group : groups.entrySet()) {
      String groupName = group.getKey();
      System.out.println("Connecting to MQTT for lighting group: " + groupName);
      Config groupConf = config.getConfig("groups." + groupName);
      List<String> buttons = groupConf.getStringList("buttons");
      List<String> lamps = groupConf.getStringList("lamps");
      for(String button : buttons) {
        startConsumer(button, lamps);
      }
    }

    ConfigObject shellies = config.getObject("shellies");
    for(Map.Entry<String, ConfigValue> shelly : shellies.entrySet()) {
      String shellyName = shelly.getKey();
      Config shellyConf = config.getConfig("shellies." + shellyName);
      String power = shellyConf.getString("power");
      String energy = shellyConf.getString("energy");
      System.out.println("Connecting to MQTT for Shelly: " + shellyName + ", " + power + ", " + energy);
      String url = shellyConf.getString("url");
      startShellyConsumer(power, energy, url);
    }

    while(isRunning) {
      try {
        Thread.sleep(1000);
      } catch(InterruptedException ex) {}
    }

    client.disconnect();
  }

  /**
   * Start the consumer thread that listens for button presses
   * @throws MqttException
   */
  void startConsumer(String id, List<String> lamps) throws MqttException {
    client.subscribe("zigbee2mqtt/" + id, (topic, message) -> {
      try {
        String msg = new String(message.getPayload(), StandardCharsets.UTF_8);
        JsonObject obj = JsonParser.parseString(msg).getAsJsonObject();
        System.out.println(id + " in " + msg);
        if(obj.get("action") == null) {
          return;// Ignore this type of message
        }
        String action = obj.get("action").getAsString();
        if(action.startsWith("on") || action.startsWith("off") || action.startsWith("up") || action.startsWith("down")) {
          boolean on = !action.startsWith("off");
          int brightness = 0;
          if(on) {
            if(obj.get("brightness") == null) {
              brightness = 255;
            } else {
              brightness = obj.get("brightness").getAsInt();
            }
          }
          sendAsync(lamps, on, brightness);
        }
      } catch(Exception ex) {
        System.err.println("Problem while sending!");
        ex.printStackTrace();
      }
    });
  }

  /**
   * Start a shelly consumer
   * @param powerTopic Power topic
   * @param energyTopic
   * @param urlPattern
   * @throws MqttException
   */
  void startShellyConsumer(String powerTopic, String energyTopic, String urlPattern) throws MqttException {
    AtomicLong powermW = new AtomicLong(-1);// Power in milliwatt
    AtomicLong energyWm = new AtomicLong(-1);// Energy in Watt-minutes
    AtomicLong lastPower = new AtomicLong(0);
    AtomicLong lastEnergy = new AtomicLong(0);

    client.subscribe(powerTopic, (topic, message) -> {
      String msg = new String(message.getPayload(), StandardCharsets.UTF_8);
      if(msg != null && !msg.trim().isEmpty()) {
        powermW.set(Math.round(Double.valueOf(msg) * 1000.0));
      }
      long age = System.currentTimeMillis() - lastPower.get();
      if(powermW.get() >= 0 && energyWm.get() >= 0 && age >= 5_000) {// Don't send more often than once per 5 seconds
        lastPower.set(System.currentTimeMillis());
        sendShellyStatsAsync(urlPattern, powermW.get() / 1000.0, energyWm.get() / 60.0);
      }
    });
    client.subscribe(energyTopic, (topic, message) -> {
      String msg = new String(message.getPayload(), StandardCharsets.UTF_8);
      if(msg != null && !msg.trim().isEmpty()) {
        energyWm.set(Math.round(Double.valueOf(msg)));
      }
      long age = System.currentTimeMillis() - lastEnergy.get();
      if(powermW.get() >= 0 && energyWm.get() >= 0 && age >= 5_000) {// Don't send more often than once per 5 seconds
        lastEnergy.set(System.currentTimeMillis());
        sendShellyStatsAsync(urlPattern, powermW.get() / 1000.0, energyWm.get() / 60.0);
      }
    });
  }

  /**
   * Send Shelly stats asynchronously.
   * @param urlPattern
   * @param powerW
   * @param energyWh
   */
  void sendShellyStatsAsync(String urlPattern, double powerW, double energyWh) {
    try {
      String url = urlPattern
          .replace("%POWER_WATT%", String.format("%.0f", powerW))
          .replace("%ENERGY_WATTHOUR%", String.format("%.0f", energyWh));

      String authHeader = null;
      if(url.startsWith("http") && url.contains("@")) {
        String authStr = url.substring(url.indexOf("//") + 2);
        authStr = authStr.substring(0, authStr.indexOf("@"));
        byte[] encodedAuth = Base64.getEncoder().encode(authStr.getBytes(StandardCharsets.UTF_8));
        authHeader = "Basic " + new String(encodedAuth, StandardCharsets.UTF_8);

        url = url.substring(0, url.indexOf("//")) + "//" + url.substring(url.indexOf("@") + 1);
      }
      final String urlStr = url;
      final String authHeaderValue = authHeader;

      queue.add(() -> {
        try {
          URL urlObj = new URL(urlStr);

          HttpURLConnection conn = (HttpURLConnection) urlObj.openConnection();
          conn.setRequestMethod("GET");
          conn.setConnectTimeout(5000);
          conn.setReadTimeout(5000);
          conn.setInstanceFollowRedirects(false);
          if(authHeaderValue != null) {
            conn.setRequestProperty("Authorization", authHeaderValue);
          }
          int responseCode = conn.getResponseCode();
          conn.disconnect();

        } catch (Exception ex) {
          ex.printStackTrace();//TODO: Handle
        }
      });
    } catch(Exception ex) {
      ex.printStackTrace();
    }
  }

  /**
   * Start a send thread that executes runnables from the local queue.
   */
  void startSendThread() {
    Thread sendThread = new Thread(() -> {
      while(isRunning) {
        try {
          Runnable message = queue.poll(1, TimeUnit.SECONDS);
          if(message != null) {
            message.run();
          }
        } catch(Exception ex) {
          ex.printStackTrace();//TODO
          try {
            Thread.sleep(100);
          } catch (InterruptedException e) {}
        }
      }
    });
    sendThread.setName("send-thread");
    sendThread.start();
  }

  /**
   * Send a command asynchronously
   * @param ids IDs of lamps to control
   * @param on
   * @param brightness
   */
  void sendAsync(List<String> ids, boolean on, int brightness) {
    queue.add(() -> {
      try {
        for(String id : ids) {
          send(id, on, brightness);
        }
      } catch (Exception ex) {
        ex.printStackTrace();//TODO: Handle
      }
    });
  }

  /**
   * Send a light command immediately (blocking).
   * @param id
   * @param on
   * @param brightness
   * @throws MqttException
   */
  void send(String id, boolean on, int brightness) throws MqttException {
    if(!client.isConnected()) {
      return;
    }
    Map<String, Object> msg = new HashMap<>();
    msg.put("state", on ? "ON" : "OFF");
    msg.put("brightness", brightness);

    String json = new Gson().toJson(msg);
    System.out.println(id + " out " + json);
    MqttMessage message = new MqttMessage(json.getBytes(StandardCharsets.UTF_8));
    message.setQos(0);// At most once
    message.setRetained(false);
    String topic = "zigbee2mqtt/" + id + "/set";

    client.publish(topic, message);
  }

}
