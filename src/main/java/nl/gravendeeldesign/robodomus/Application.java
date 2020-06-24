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
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

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
      Config groupConf = config.getConfig("groups." + groupName);
      List<String> buttons = groupConf.getStringList("buttons");
      List<String> lamps = groupConf.getStringList("lamps");
      for(String button : buttons) {
        startConsumer(button, lamps);
      }
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
            brightness = obj.get("brightness").getAsInt();
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
