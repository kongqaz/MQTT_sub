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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.io.File;

@Slf4j
public class MqttSubscriberApplication {
    private MqttClient client;
    private ApplicationConfig config;
    private DataManager dataManager;
    private final Map<String, JsonNode> topicData = new ConcurrentHashMap<>();
    private final Map<String, JsonNode> filteredTopicData = new ConcurrentHashMap<>();
    private Map<String, Set<String>> httpApiKeyFilter;

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

            try{
                // 加载http-api-key-filter配置
                loadHttpApiKeyFilter();
            } catch (Exception e) {
                log.error("loadHttpApiKeyFilter error:", e);
            }

            // 初始化数据管理器
            dataManager = new DataManager();
            while (true) {
                boolean bInit = dataManager.init(config);
                if(bInit){
                    break;
                }
                try {
                    Thread.sleep(5000);
                } catch (Exception e) {
                    log.error("Thread sleep error:", e);
                }
            }

            // 初始化Web服务
            initWebServer(port);

            while (true) {
                try {
                    // 连接MQTT Broker
                    connectToMqttBroker();
                    break;
                } catch (Exception e) {
                    log.error("Connect to MQTT broker fail:", e);
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException ex){
                        log.error("sleep error:", ex);
                    }
                }
            }

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

    private void loadHttpApiKeyFilter() throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Map<String, List<String>> rawFilter = mapper.readValue(
                new File("config/http-api-key-filter.json"),
                mapper.getTypeFactory().constructMapType(Map.class, String.class, List.class));

        // 转换为Set以提高查找效率
        httpApiKeyFilter = rawFilter.entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> {
                            Set<String> set = new HashSet<>();
                            set.addAll(e.getValue());
                            return set;
                        }
                ));
    }

    private void initWebServer(int port) {
        Spark.port(port);

        // 为每个topic动态注册API路由
        config.getTopics().forEach((topic, topicConfig) -> {
            Spark.get(topicConfig.getApi(), (req, res) -> {
                res.type("application/json");
                JsonNode data = filteredTopicData.get(topic);
                if (data != null) {
                    log.info("Returning filtered data for topic: " + topic);
                    return data.toString();
                } else {
                    data = topicData.get(topic);
                    if (data != null) {
                        log.info("Returning data for topic: " + topic);
                        return data.toString();
                    } else {
//                    res.status(404);
//                    return "No data available for topic: " + topic;
                        return "[]";
                    }
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

        try{
            client = new MqttClient(broker, clientId, new MemoryPersistence());

            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            options.setAutomaticReconnect(true); // 启用自动重连
            options.setConnectionTimeout(15); // 15秒连接超时
            options.setKeepAliveInterval(30); // 30秒心跳间隔

            if (username != null && !username.isEmpty()) {
                options.setUserName(username);
                if (password != null) {
                    options.setPassword(password.toCharArray());
                }
            }

            client.setCallback(new MqttMessageCallback(config, dataManager, topicData, filteredTopicData, httpApiKeyFilter, client));
            client.connect(options);
            log.info("Connected to MQTT broker: " + broker);
        } catch (Exception e){
            log.error("Connect to MQTT broker fail:", e);
            if(client != null){
                client.close();
                log.info("MQTT connection closed");
            }
            throw e;
        }
    }

    private void subscribeToTopics() throws MqttException {
        for (String topic : config.getTopics().keySet()) {
            client.subscribe(topic);
            log.info("Subscribed to topic: " + topic);
        }
    }
}
