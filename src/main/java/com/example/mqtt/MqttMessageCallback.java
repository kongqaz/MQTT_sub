package com.example.mqtt;

import com.example.config.ApplicationConfig;
import com.example.config.TopicConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MqttMessageCallback implements MqttCallbackExtended {
    private final ApplicationConfig config;
    private final DataManager dataManager;
    private final Map<String, JsonNode> topicData;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final Logger loggerDebug = LoggerFactory.getLogger("logger.DEBUG_MSG");

    public MqttMessageCallback(ApplicationConfig config, DataManager dataManager,
                               Map<String, JsonNode> topicData) {
        this.config = config;
        this.dataManager = dataManager;
        this.topicData = topicData;
    }

    @Override
    public void connectComplete(boolean reconnect, String serverURI) {
        log.info("MQTT connection completed. Reconnect: " + reconnect);
    }

    @Override
    public void connectionLost(Throwable cause) {
        log.info("MQTT connection lost: " + cause.getMessage());
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        log.info("Message arrived on topic: " + topic);
        String payload = new String(message.getPayload(), "UTF-8");
        loggerDebug.info("Recv on topic:{}, msg:{}", topic, payload);

        try {
            JsonNode jsonData = objectMapper.readTree(payload);
            TopicConfig topicConfig = config.getTopics().get(topic);

            if (topicConfig != null) {
                // 处理数据存储和更新
                processTopicData(topic, jsonData, topicConfig);
            }
        } catch (Exception e) {
            log.error("Error processing message: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void processTopicData(String topic, JsonNode jsonData, TopicConfig topicConfig) {
        try {
            // 获取或创建该主题的现有数据
            JsonNode existingData = topicData.get(topic);

            // 如果是第一次接收数据，从数据库加载现有数据
            if (existingData == null) {
                existingData = dataManager.loadInitialData(topicConfig.getTable(), topicConfig.getKey());
            }

            // 合并新数据与现有数据
            JsonNode mergedData = mergeJsonData(existingData, jsonData, topicConfig.getKey());

            // 更新内存中的数据
            topicData.put(topic, mergedData);

            // 保存到数据库
            dataManager.saveToDatabase(topicConfig.getTable(), topicConfig.getKey(), mergedData);

            log.info("Processed data for topic: " + topic);
        } catch (Exception e) {
            log.error("Error processing topic data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private JsonNode mergeJsonData(JsonNode existingData, JsonNode newData, String keyField) {
        if (existingData == null) {
            return newData;
        }

        // 创建合并后的数据副本
        JsonNode mergedData = newData.deepCopy();

        // 递归查找并更新指定key的数据
        updateByKey(mergedData, existingData, keyField);

        return mergedData;
    }

    private void updateByKey(JsonNode target, JsonNode source, String keyField) {
        if (target.isObject() && source.isObject()) {
            ObjectNode targetObj = (ObjectNode) target;
            ObjectNode sourceObj = (ObjectNode) source;

            // 如果当前层级包含key字段，则更新该层级的数据
            if (targetObj.has(keyField) && sourceObj.has(keyField) &&
                    targetObj.get(keyField).asText().equals(sourceObj.get(keyField).asText())) {
                // 以target中的数据为准，source中不存在的字段才从source合并进来
                Iterator<Map.Entry<String, JsonNode>> fields = sourceObj.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    if (!targetObj.has(field.getKey())) {
                        targetObj.set(field.getKey(), field.getValue());
                    }
                }
            } else {
                // 递归处理子对象
                Iterator<Map.Entry<String, JsonNode>> targetFields = targetObj.fields();
                while (targetFields.hasNext()) {
                    Map.Entry<String, JsonNode> field = targetFields.next();
                    String fieldName = field.getKey();
                    if (sourceObj.has(fieldName)) {
                        updateByKey(field.getValue(), sourceObj.get(fieldName), keyField);
                    }
                }

                // 将source中target没有的字段合并进来
                Iterator<Map.Entry<String, JsonNode>> sourceFields = sourceObj.fields();
                while (sourceFields.hasNext()) {
                    Map.Entry<String, JsonNode> field = sourceFields.next();
                    String fieldName = field.getKey();
                    if (!targetObj.has(fieldName)) {
                        targetObj.set(fieldName, field.getValue());
                    }
                }
            }
        } else if (target.isArray() && source.isArray()) {
            // 处理数组情况
//            for (int i = 0; i < target.size() && i < source.size(); i++) {
//                updateByKey(target.get(i), source.get(i), keyField);
//            }

            // 基于keyField的数组合并逻辑
            for (JsonNode sourceElement : source) {
                if (sourceElement.isObject() && ((ObjectNode) sourceElement).has(keyField)) {
                    String sourceKey = ((ObjectNode) sourceElement).get(keyField).asText();
                    boolean found = false;

                    // 在target数组中查找具有相同keyField值的元素
                    for (JsonNode targetElement : target) {
                        if (targetElement.isObject() && ((ObjectNode) targetElement).has(keyField)) {
                            String targetKey = ((ObjectNode) targetElement).get(keyField).asText();
                            if (sourceKey.equals(targetKey)) {
                                // 找到匹配元素，递归合并
                                updateByKey(targetElement, sourceElement, keyField);
                                found = true;
                                break;
                            }
                        }
                    }

                    // 如果未找到匹配元素，则添加到target数组
                    if (!found) {
                        ((ArrayNode) target).add(sourceElement);
                    }
                } else {
                    // 对于没有keyField的元素，需要递归处理子孙层
                    if (sourceElement.isObject()) {
                        boolean foundInChildren = false;
                        ObjectNode sourceObj = (ObjectNode) sourceElement;

                        // 在target数组中查找结构相似的元素
                        for (JsonNode targetElement : target) {
                            if (targetElement.isObject() && isStructureSimilar(targetElement, sourceElement)) {
                                // 结构相似，递归处理
                                updateByKey(targetElement, sourceElement, keyField);
                                foundInChildren = true;
                                break;
                            }
                        }

                        // 如果未找到结构相似的元素，则添加到target数组
                        if (!foundInChildren) {
                            ((ArrayNode) target).add(sourceElement);
                        }
                    } else if (sourceElement.isArray()) {
                        // 如果元素本身是数组，递归处理数组中的元素
                        for (JsonNode targetElement : target) {
                            if (targetElement.isArray()) {
                                // 递归处理子数组
                                updateByKey(targetElement, sourceElement, keyField);
                                break;
                            }
                        }
                    } else {
                        // 对于基本类型（字符串、数字等），如果不存在则添加
                        boolean exists = false;
                        for (JsonNode targetElement : target) {
                            if (targetElement.equals(sourceElement)) {
                                exists = true;
                                break;
                            }
                        }
                        if (!exists) {
                            ((ArrayNode) target).add(sourceElement);
                        }
                    }
                }
            }
        }
    }

    /**
     * 判断两个JSON对象结构是否相似
     * @param node1 第一个节点
     * @param node2 第二个节点
     * @return 是否结构相似
     */
    private boolean isStructureSimilar(JsonNode node1, JsonNode node2) {
        if (node1.isObject() && node2.isObject()) {
            ObjectNode obj1 = (ObjectNode) node1;
            ObjectNode obj2 = (ObjectNode) node2;

            // 比较字段数量和字段名
            if (obj1.size() != obj2.size()) {
                return false;
            }

            Iterator<String> fieldNames1 = obj1.fieldNames();
            while (fieldNames1.hasNext()) {
                if (!obj2.has(fieldNames1.next())) {
                    return false;
                }
            }
            return true;
        } else if (node1.isArray() && node2.isArray()) {
            return true;
        }
        return node1.getNodeType() == node2.getNodeType();
    }


    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        // 不需要处理
    }
}
