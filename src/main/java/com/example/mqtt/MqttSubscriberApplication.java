package com.example.mqtt;

import com.example.config.ApplicationConfig;
import com.example.config.MqttConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import spark.Spark;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MqttSubscriberApplication {
    private MqttClient client;
    private ApplicationConfig config;
    private DataManager dataManager;
    private final Map<String, JsonNode> topicData = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        int port = 8080; // 默认端口
        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]); // 从命令行参数获取端口
            } catch (NumberFormatException e) {
                log.warn("Invalid port number provided, using default port 8080");
            }
        }
        new MqttSubscriberApplication().start(port);
    }

    public void start(int port) {
        try {
            // 加载配置
            config = ApplicationConfig.load();

            // 初始化数据管理器
            dataManager = new DataManager(config);

            // 初始化Web服务
            initWebServer(port);

            // 连接MQTT Broker
            connectToMqttBroker();

            // 订阅主题
            subscribeToTopics();

            log.info("MQTT Subscriber Application started successfully!");
        } catch (Exception e) {
//            e.printStackTrace();
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            String stackTrace = sw.toString();
            log.error("Error starting MQTT Subscriber Application, ex={}, trace={}", e.getMessage(), stackTrace);
        }
    }

    private void initWebServer(int port) {
        Spark.port(port);

        // 为每个topic动态注册API路由
        config.getTopics().forEach((topic, topicConfig) -> {
            Spark.get(topicConfig.getApi(), (req, res) -> {
                JsonNode data = topicData.get(topic);
                if (data != null) {
                    log.info("Returning data for topic: " + topic);
                    return data.toString();
                } else {
                    res.status(404);
                    return "No data available for topic: " + topic;
                }
            });
            log.info("Registered API: GET " + topicConfig.getApi() + " for topic: " + topic);
        });
    }

    private void connectToMqttBroker() throws MqttException {
        MqttConfig mqttConfig = config.getMqtt();
        String broker = mqttConfig.getBroker();
        String clientId = mqttConfig.getClientId();
        String username = mqttConfig.getUsername();
        String password = mqttConfig.getPassword();

        client = new MqttClient(broker, clientId, new MemoryPersistence());

        MqttConnectOptions options = new MqttConnectOptions();
        options.setCleanSession(true);
        options.setAutomaticReconnect(true);
        options.setConnectionTimeout(30);
        options.setKeepAliveInterval(60);

        if (username != null && !username.isEmpty()) {
            options.setUserName(username);
            if (password != null) {
                options.setPassword(password.toCharArray());
            }
        }

        client.setCallback(new MqttMessageCallback(config, dataManager, topicData));
        client.connect(options);
        log.info("Connected to MQTT broker: " + broker);
    }

    private void subscribeToTopics() throws MqttException {
        for (String topic : config.getTopics().keySet()) {
            client.subscribe(topic);
            log.info("Subscribed to topic: " + topic);
        }
    }
}
