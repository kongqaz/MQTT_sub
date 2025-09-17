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
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MqttMessageCallback implements MqttCallbackExtended {
    private final ApplicationConfig config;
    private final DataManager dataManager;
    private final Map<String, JsonNode> topicData;
    private final Map<String, JsonNode> filteredTopicData;
    private final Map<String, Map<String, Integer>> filteredDataIndexMap; // 用于快速查找过滤数据中的项
    private final Map<String, Set<String>> httpApiKeyFilter;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private MqttClient client;

    private static final Logger loggerDebug = LoggerFactory.getLogger("logger.DEBUG_MSG");

    public MqttMessageCallback(ApplicationConfig config, DataManager dataManager,
                               Map<String, JsonNode> topicData,
                               Map<String, JsonNode> filteredTopicData,
                               Map<String, Set<String>> httpApiKeyFilter,
                               MqttClient client) {
        this.config = config;
        this.dataManager = dataManager;
        this.topicData = topicData;
        this.filteredTopicData = filteredTopicData;
        this.filteredDataIndexMap = new ConcurrentHashMap<>();
        this.httpApiKeyFilter = httpApiKeyFilter;
        this.client = client;
    }

    @Override
    public void connectComplete(boolean reconnect, String serverURI) {
        log.info("MQTT connection completed. Reconnect: " + reconnect);

        if (reconnect) {
            // 重新订阅所有主题
            resubscribeTopics();
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        log.info("MQTT connection lost: " + cause.getMessage());
    }

    private void resubscribeTopics() {
        try {
            String[] topics = config.getTopics().keySet().toArray(new String[0]);
            int[] qosLevels = java.util.Arrays.stream(topics)
                    .mapToInt(s -> 1)
                    .toArray();

            client.subscribe(topics, qosLevels);

            log.info("Resubscribed to topics: " + config.getTopics().keySet());
        } catch (Exception e) {
            log.error("Failed to resubscribe topics: " + e.getMessage());
        }
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
            log.info("step1");
            // 获取或创建该主题的现有数据
            JsonNode existingData = topicData.get(topic);

            // 如果是第一次接收数据，从数据库加载现有数据
            if (existingData == null) {
                existingData = dataManager.loadInitialData(topicConfig.getTable(), topicConfig.getKey());
                // 同时构建初始的过滤数据
                if (existingData != null && httpApiKeyFilter != null && httpApiKeyFilter.containsKey(topic)) {
                    buildInitialFilteredData(topic, existingData, topicConfig);
                }
            }

            log.info("step2");

            final JsonNode finalExistingData = existingData;
            // 异步合并新数据与现有数据
            CompletableFuture<JsonNode> mergedDataFuture = CompletableFuture.supplyAsync(() ->
                    mergeJsonData(finalExistingData, jsonData, topicConfig.getKey()));

            mergedDataFuture.thenAccept(mergedData -> {
                // 更新内存中的数据
                topicData.put(topic, mergedData);

                // 如果该topic需要过滤，增量更新过滤后的数据
                if (httpApiKeyFilter != null && httpApiKeyFilter.containsKey(topic)) {
                    incrementallyUpdateFilteredData(topic, jsonData, topicConfig);
                }

                log.info("Start save data for topic:{}", topic);
                // 保存到数据库
                dataManager.saveToDatabase(topicConfig.getTable(), topicConfig.getKey(), jsonData);

                log.info("Processed data for topic: " + topic);
            }).exceptionally(throwable -> {
                log.error("Error1 processing topic data: " + throwable.getMessage());
                return null;
            });
        } catch (Exception e) {
            log.error("Error2 processing topic data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private JsonNode mergeJsonData(JsonNode existingData, JsonNode newData, String keyField) {
        if (existingData == null) {
            return newData;
        }

        log.info("step2.1");
        // 创建合并后的数据副本
        JsonNode mergedData = newData.deepCopy();
        log.info("step2.2");

        // 递归查找并更新指定key的数据
        updateByKey(mergedData, existingData, keyField);
        log.info("step2.3");

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
            ArrayNode targetArray = (ArrayNode) target;
            ArrayNode sourceArray = (ArrayNode) source;

            // 构建target数组的索引映射
            Map<String, JsonNode> targetKeyMap = new HashMap<>();
            Map<String, JsonNode> targetStructureMap = new HashMap<>();

            for (JsonNode targetElement : targetArray) {
                if (targetElement.isObject()) {
                    ObjectNode obj = (ObjectNode) targetElement;
                    if (obj.has(keyField)) {
                        targetKeyMap.put(obj.get(keyField).asText(), targetElement);
                    } else {
                        // 为没有keyField的对象建立结构映射
                        String structureKey = generateStructureKey(targetElement);
                        targetStructureMap.put(structureKey, targetElement);
                    }
                }
            }

            // 遍历source数组进行高效匹配
            for (JsonNode sourceElement : sourceArray) {
                if (sourceElement.isObject()) {
                    ObjectNode sourceObj = (ObjectNode) sourceElement;
                    if (sourceObj.has(keyField)) {
                        String sourceKey = sourceObj.get(keyField).asText();
                        JsonNode matchingTarget = targetKeyMap.get(sourceKey);

                        if (matchingTarget != null) {
                            // 找到匹配元素，递归合并
                            updateByKey(matchingTarget, sourceElement, keyField);
                        } else {
                            // 如果未找到匹配元素，则添加到target数组
                            targetArray.add(sourceElement);
                            targetKeyMap.put(sourceKey, sourceElement);
                        }
                    } else {
                        // 处理没有keyField的对象
                        String structureKey = generateStructureKey(sourceElement);
                        JsonNode matchingTarget = targetStructureMap.get(structureKey);

                        if (matchingTarget != null) {
                            updateByKey(matchingTarget, sourceElement, keyField);
                        } else {
                            targetArray.add(sourceElement);
                            targetStructureMap.put(structureKey, sourceElement);
                        }
                    }
                }
            }
        }
    }

    // 生成结构键值的辅助方法
    private String generateStructureKey(JsonNode node) {
        if (node.isObject()) {
            StringBuilder keyBuilder = new StringBuilder();
            ObjectNode obj = (ObjectNode) node;
            Iterator<String> fieldNames = obj.fieldNames();
            while (fieldNames.hasNext()) {
                keyBuilder.append(fieldNames.next()).append("|");
            }
            return keyBuilder.toString();
        }
        return node.getNodeType().toString();
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

    private void buildInitialFilteredData(String topic, JsonNode initialData, TopicConfig topicConfig) {
        // 创建过滤后的数据
        ArrayNode filteredArray = objectMapper.createArrayNode();
        Map<String, Integer> indexMap = new HashMap<>();

        // 获取过滤规则
        Set<String> filterValues = httpApiKeyFilter.get(topic);
        String keyField = topicConfig.getKey();

        if (filterValues != null && keyField != null && initialData.isArray()) {
            // 遍历初始数据，只保留符合过滤条件的项
            int index = 0;
            for (JsonNode item : initialData) {
                if (item.isObject()) {
                    ObjectNode obj = (ObjectNode) item;
                    if (obj.has(keyField)) {
                        String keyValue = obj.get(keyField).asText();
                        // 如果项符合过滤条件，添加到过滤数据中
                        if (filterValues.contains(keyValue)) {
                            filteredArray.add(item);
                            indexMap.put(keyValue, index);
                            index++;
                        }
                    }
                }
            }
        }

        // 存储过滤后的数据和索引映射
        if(filteredArray.size() > 0) {
            filteredTopicData.put(topic, filteredArray);
            filteredDataIndexMap.put(topic, indexMap);
        }
    }

    private void incrementallyUpdateFilteredData(String topic, JsonNode newData, TopicConfig topicConfig) {
        // 获取现有的过滤数据
        JsonNode existingFilteredData = filteredTopicData.get(topic);
        ArrayNode filteredArray;

        // 如果还没有过滤数据，创建一个新的数组
        if (existingFilteredData == null || !existingFilteredData.isArray()) {
            filteredArray = objectMapper.createArrayNode();
            // 初始化索引映射
            filteredDataIndexMap.put(topic, new HashMap<>());
        } else {
            // 使用现有的过滤数据
            filteredArray = (ArrayNode) existingFilteredData;
            // 确保索引映射存在
            filteredDataIndexMap.computeIfAbsent(topic, k -> new HashMap<>());
        }

        // 获取索引映射
        Map<String, Integer> indexMap = filteredDataIndexMap.get(topic);

        // 获取过滤规则
        Set<String> filterValues = httpApiKeyFilter.get(topic);
        String keyField = topicConfig.getKey();

        if (filterValues != null && keyField != null) {
            // 处理新数据并增量添加到过滤数据中
            if (newData.isArray()) {
                // 如果新数据是数组，逐个检查元素
                for (JsonNode item : newData) {
                    if (item.isObject()) {
                        ObjectNode obj = (ObjectNode) item;
                        if (obj.has(keyField)) {
                            String keyValue = obj.get(keyField).asText();
                            // 如果新项符合过滤条件，添加到过滤数据中
                            if (filterValues.contains(keyValue)) {
                                // 检查是否已存在相同key的项，如果存在则替换，否则添加
                                Integer index = indexMap.get(keyValue);
                                if (index != null) {
                                    // 替换已存在的项
                                    filteredArray.set(index, item);
                                } else {
                                    // 添加新项并更新索引
                                    int newIndex = filteredArray.size();
                                    filteredArray.add(item);
                                    indexMap.put(keyValue, newIndex);
                                }
                            }
                        }
                    }
                }
            } else if (newData.isObject()) {
                // 如果新数据是单个对象
                ObjectNode obj = (ObjectNode) newData;
                if (obj.has(keyField)) {
                    String keyValue = obj.get(keyField).asText();
                    // 如果新项符合过滤条件，添加到过滤数据中
                    if (filterValues.contains(keyValue)) {
                        // 检查是否已存在相同key的项，如果存在则替换，否则添加
                        Integer index = indexMap.get(keyValue);
                        if (index != null) {
                            // 替换已存在的项
                            filteredArray.set(index, newData);
                        } else {
                            // 添加新项并更新索引
                            int newIndex = filteredArray.size();
                            filteredArray.add(newData);
                            indexMap.put(keyValue, newIndex);
                        }
                    }
                }
            }

            // 更新过滤数据
            if(filteredArray.size() > 0) {
                filteredTopicData.put(topic, filteredArray);
            }
        }
    }
}
