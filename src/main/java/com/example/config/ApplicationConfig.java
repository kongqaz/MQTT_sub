package com.example.config;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.Map;

public class ApplicationConfig {
    private Map<String, TopicConfig> topics;
    private MqttConfig mqtt;
    private DatabaseConfig database;

    public static ApplicationConfig load() throws IOException {
        ApplicationConfig config = new ApplicationConfig();
        ObjectMapper mapper = new ObjectMapper();

        // 加载MQTT配置
        config.mqtt = mapper.readValue(new File("config/mqtt-config.json"), MqttConfig.class);

        // 加载数据库配置
        config.database = mapper.readValue(new File("config/database-config.json"), DatabaseConfig.class);

        // 加载主题配置
        config.topics = mapper.readValue(new File("config/config.json"),
                mapper.getTypeFactory().constructMapType(Map.class, String.class, TopicConfig.class));

        return config;
    }

    // Getters
    public Map<String, TopicConfig> getTopics() {
        return topics;
    }

    public MqttConfig getMqtt() {
        return mqtt;
    }

    public DatabaseConfig getDatabase() {
        return database;
    }
}
