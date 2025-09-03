package com.example.mqtt;

import com.example.config.ApplicationConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.util.*;

@Slf4j
public class DataManager {
    private final HikariDataSource dataSource;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public DataManager(ApplicationConfig config) {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(config.getDatabase().getUrl());
        hikariConfig.setUsername(config.getDatabase().getUsername());
        hikariConfig.setPassword(config.getDatabase().getPassword());
        hikariConfig.setMaximumPoolSize(10);
        this.dataSource = new HikariDataSource(hikariConfig);

        log.info("Database connection pool initialized");
    }

    public JsonNode loadInitialData(String table, String keyField) {
        try (Connection conn = dataSource.getConnection()) {
            String sql = "SELECT * FROM " + table;
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql)) {

                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                ArrayNode rootNode = objectMapper.createArrayNode();

                // 读取所有行数据
                while (rs.next()) {
                    ObjectNode rowNode = objectMapper.createObjectNode();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnName(i);
                        Object value = rs.getObject(i);
                        if (value != null) {
                            if (value instanceof String) {
                                rowNode.put(columnName, (String) value);
                            } else if (value instanceof Integer) {
                                rowNode.put(columnName, (Integer) value);
                            } else if (value instanceof Long) {
                                rowNode.put(columnName, (Long) value);
                            } else if (value instanceof Double) {
                                rowNode.put(columnName, (Double) value);
                            } else if (value instanceof Boolean) {
                                rowNode.put(columnName, (Boolean) value);
                            } else {
                                rowNode.put(columnName, value.toString());
                            }
                        }
                    }
                    rootNode.add(rowNode);
                }

                return rootNode.size() > 0 ? rootNode : null;
            }
        } catch (SQLException e) {
            log.error("Error loading initial data: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    public void saveToDatabase(String table, String keyField, JsonNode data) {
        try {
            // 解析表名
//            String[] parts = table.split("\\.");
//            String tableName = parts.length > 1 ? parts[1] : parts[0];
            String tableName = table;

            // 构建INSERT ... ON DUPLICATE KEY UPDATE语句
            if (data.isObject()) {
                ObjectNode obj = (ObjectNode) data;
                insertOrUpdateRecord(tableName, obj);
            } else if (data.isArray()) {
//                for (JsonNode item : data) {
//                    if (item.isObject()) {
//                        insertOrUpdateRecord(tableName, (ObjectNode) item);
//                    }
//                }
                // 批量处理数组数据
                if (data.size() > 0) {
                    batchInsertOrUpdateRecords(tableName, (ArrayNode) data);
                }
            }
        } catch (Exception e) {
            log.error("Error saving data to database: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void batchInsertOrUpdateRecords(String tableName, ArrayNode dataArray) throws SQLException {
        if (dataArray.size() == 0) return;

        // 获取第一条数据来构建SQL语句
        ObjectNode firstItem = (ObjectNode) dataArray.get(0);

        // 获取表的列信息
        Map<String, String> columnTypes = getTableColumns(tableName);
        if (columnTypes.isEmpty()) {
            log.error("Cannot get table columns for: " + tableName);
            return;
        }

        // 构建INSERT ... ON DUPLICATE KEY UPDATE语句
        StringBuilder insertSQL = new StringBuilder();
        insertSQL.append("INSERT INTO ").append(tableName).append(" (");

        // 收集列名
        StringBuilder columns = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();
        StringBuilder updates = new StringBuilder();

        List<String> fieldNames = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> fields = firstItem.fields();
        while (fields.hasNext()) {
            fieldNames.add(fields.next().getKey());
        }

        boolean first = true;
        for (String fieldName : fieldNames) {
            if (!first) {
                columns.append(", ");
                placeholders.append(", ");
                updates.append(", ");
            }

            columns.append("`").append(fieldName).append("`");
            placeholders.append("?");
            updates.append("`").append(fieldName).append("` = VALUES(`").append(fieldName).append("`)");

            first = false;
        }

        insertSQL.append(columns.toString())
                .append(") VALUES (")
                .append(placeholders.toString())
                .append(") ON DUPLICATE KEY UPDATE ")
                .append(updates.toString());

        // 执行批量SQL
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(insertSQL.toString())) {

            // 关闭自动提交以提高性能
            conn.setAutoCommit(false);

            for (JsonNode item : dataArray) {
                if (item.isObject()) {
                    ObjectNode obj = (ObjectNode) item;

                    int index = 1;
                    for (String fieldName : fieldNames) {
                        JsonNode value = obj.get(fieldName);
                        if (value == null || value.isNull()) {
                            pstmt.setObject(index++, null);
                        } else if (value.isTextual()) {
                            pstmt.setString(index++, value.asText());
                        } else if (value.isInt()) {
                            pstmt.setInt(index++, value.asInt());
                        } else if (value.isLong()) {
                            pstmt.setLong(index++, value.asLong());
                        } else if (value.isDouble()) {
                            pstmt.setDouble(index++, value.asDouble());
                        } else if (value.isBoolean()) {
                            pstmt.setBoolean(index++, value.asBoolean());
                        } else {
                            pstmt.setString(index++, value.asText());
                        }
                    }

                    pstmt.addBatch();
                }
            }

            pstmt.executeBatch();
            conn.commit();
            log.info("Batch saved {} records to database table: {}", dataArray.size(), tableName);

            // 恢复自动提交
            conn.setAutoCommit(true);
        } catch (SQLException e) {
            log.error("Error in batch insert: " + e.getMessage());
            throw e;
        }
    }

    private void insertOrUpdateRecord(String tableName, ObjectNode data) throws SQLException {
        // 获取表的列信息
        Map<String, String> columnTypes = getTableColumns(tableName);

        if (columnTypes.isEmpty()) {
            log.error("Cannot get table columns for: " + tableName);
            return;
        }

        // 构建INSERT ... ON DUPLICATE KEY UPDATE语句
        StringBuilder insertSQL = new StringBuilder();
        insertSQL.append("INSERT INTO ").append(tableName).append(" (");

        // 收集列名和值
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();
        StringBuilder updates = new StringBuilder();

        List<String> fieldNames = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> fields = data.fields();
        while (fields.hasNext()) {
            fieldNames.add(fields.next().getKey());
        }

        boolean first = true;
        for (String fieldName : fieldNames) {
            if (!first) {
                columns.append(", ");
                values.append(", ");
                updates.append(", ");
            }

            columns.append("`").append(fieldName).append("`");
            values.append("?");
            updates.append("`").append(fieldName).append("` = VALUES(`").append(fieldName).append("`)");

            first = false;
        }

        insertSQL.append(columns.toString())
                .append(") VALUES (")
                .append(values.toString())
                .append(") ON DUPLICATE KEY UPDATE ")
                .append(updates.toString());

        // 执行SQL
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(insertSQL.toString())) {

            int index = 1;
            for (String fieldName : fieldNames) {
                JsonNode value = data.get(fieldName);
                if (value.isTextual()) {
                    pstmt.setString(index++, value.asText());
                } else if (value.isInt()) {
                    pstmt.setInt(index++, value.asInt());
                } else if (value.isLong()) {
                    pstmt.setLong(index++, value.asLong());
                } else if (value.isDouble()) {
                    pstmt.setDouble(index++, value.asDouble());
                } else if (value.isBoolean()) {
                    pstmt.setBoolean(index++, value.asBoolean());
                } else {
                    pstmt.setString(index++, value.asText());
                }
            }

            pstmt.executeUpdate();
            log.info("Data saved to database table: " + tableName);
        }
    }

    private Map<String, String> getTableColumns(String tableName) throws SQLException {
        Map<String, String> columns = new HashMap<>();
        try (Connection conn = dataSource.getConnection();
             ResultSet rs = conn.getMetaData().getColumns(null, null, tableName, null)) {

            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                String typeName = rs.getString("TYPE_NAME");
                columns.put(columnName, typeName);
            }
        }
        return columns;
    }

    public void close() {
        if (dataSource != null) {
            dataSource.close();
        }
    }
}
