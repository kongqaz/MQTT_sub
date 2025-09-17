package com.example.mqtt;

import com.example.config.ApplicationConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class DataManager {
    private HikariDataSource dataSource;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private ApplicationConfig config;
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    public boolean init(ApplicationConfig config) {
        this.config = config;
        initializeDataSource();
        return initialized.get();
    }

    private void initializeDataSource() {
        try {
            log.info("initializeDataSource begin");
            HikariConfig hikariConfig = new HikariConfig();
            String strUrl = config.getDatabase().getUrl();
            hikariConfig.setJdbcUrl(strUrl);
            hikariConfig.setUsername(config.getDatabase().getUsername());
            hikariConfig.setPassword(config.getDatabase().getPassword());
            hikariConfig.setMaximumPoolSize(3);
            // 显式指定驱动类名
            if (strUrl.startsWith("jdbc:mysql")) {
                hikariConfig.setDriverClassName("com.mysql.cj.jdbc.Driver");
            } else if (strUrl.startsWith("jdbc:kingbase8")) {
                hikariConfig.setDriverClassName("com.kingbase8.Driver");
            }

            // 设置连接超时时间，避免长时间阻塞
            hikariConfig.setConnectionTimeout(3000);
            hikariConfig.setValidationTimeout(3000);

            // 连接验证查询
            hikariConfig.setConnectionTestQuery("SELECT 1");

            // 启用连接池的自动重连功能
            hikariConfig.setKeepaliveTime(30000); // 30秒检查空闲连接
            hikariConfig.setIdleTimeout(600000);   // 10分钟空闲超时
            hikariConfig.setMaxLifetime(1800000);  // 30分钟最大生存期
            hikariConfig.setMinimumIdle(1);         // 最少保持1个空闲连接
            hikariConfig.setLeakDetectionThreshold(60000); // 1分钟检测连接泄漏

            this.dataSource = new HikariDataSource(hikariConfig);
            initialized.set(true);

            log.info("Database connection pool initialized");
        } catch (Exception e) {
            log.error("Failed to initialize database connection pool: ", e);
            initialized.set(false);
        }
    }

    // 添加重连方法
    private boolean reconnect() {
        try {
            // 先检查网络是否可达
//            if (!isNetworkReachable()) {
//                log.warn("Database server is not reachable, skipping reconnect");
//                return false;
//            }

//            if (dataSource != null) {
//                try {
//                    dataSource.close();
//                } catch (Exception e) {
//                    log.warn("Error closing existing datasource: ", e);
//                } finally {
//                    dataSource = null;
//                }
//            }
            if(dataSource == null) {
                initializeDataSource();
            }
            return initialized.get();
        } catch (Exception e) {
            log.error("Failed to reconnect to database: ", e);
            return false;
        }
    }

    private boolean isNetworkReachable() {
        try {
            String jdbcUrl = config.getDatabase().getUrl();
            // 解析 JDBC URL 获取主机和端口
            // 示例: jdbc:kingbase8://localhost:54321/testdb
            if (jdbcUrl.startsWith("jdbc:")) {
                String[] parts = jdbcUrl.split("://");
                if (parts.length > 1) {
                    String hostPort = parts[1].split("/")[0];
                    String[] hostPortParts = hostPort.split(":");
                    if (hostPortParts.length >= 2) {
                        String host = hostPortParts[0];
                        int port = Integer.parseInt(hostPortParts[1]);

                        // 使用 Socket 检查网络连通性
                        try (Socket socket = new Socket()) {
                            socket.connect(new InetSocketAddress(host, port), 3000); // 3秒超时
                            log.info("Database server is reachable: {}:{}", host, port);
                            return true;
                        } catch (IOException e) {
                            log.warn("Database server is not reachable: {}:{}, error: {}", host, port, e.getMessage());
                            return false;
                        }
                    }
                }
            }
            log.warn("Unable to parse JDBC URL for network check: {}", jdbcUrl);
            return true; // 如果无法解析URL，则假设网络可达
        } catch (Exception e) {
            log.error("Error checking network reachability: ", e);
            return false;
        }
    }

    // 检查数据库连接是否可用
    private boolean isConnectionValid() {
        if (!initialized.get() || dataSource == null) {
            return false;
        }

        try (Connection conn = dataSource.getConnection()) {
            return conn.isValid(3); // 3秒超时验证
        } catch (SQLException e) {
            log.warn("Database connection validation failed: ", e);
            return false;
        } catch (Exception e) {
            log.error("Database connection validation failed2: ", e);
            return false;
        }
    }

    public JsonNode loadInitialData(String table, String keyField) {
        // 如果未初始化或连接无效，尝试重连
        if (!initialized.get() || !isConnectionValid()) {
            log.info("Database connection not available");
            return null;
//            if (!reconnect()) {
//                log.error("Failed to reconnect to database");
//                return null;
//            }
        }

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
            log.error("Error loading initial data: ", e);
            e.printStackTrace();
            return null;
        } catch (Exception e) {
            log.error("Error loading initial data2: ", e);
            return null;
        }
    }

    public void saveToDatabase(String table, String keyField, JsonNode data) {
        // 如果未初始化或连接无效，尝试重连
        if (!initialized.get() || !isConnectionValid()) {
            log.info("Database connection not available");
            return;
//            if (!reconnect()) {
//                log.error("Failed to reconnect to database");
//                return;
//            }
        }

        try {
            // 解析表名
            String[] parts = table.split("\\.");
            String tableName = parts.length > 1 ? parts[1] : parts[0];
//            String tableName = table;

            // 构建INSERT ... ON DUPLICATE KEY UPDATE语句
            if (data.isObject()) {
                ObjectNode obj = (ObjectNode) data;
                insertOrUpdateRecord(tableName, obj, keyField);
            } else if (data.isArray()) {
//                for (JsonNode item : data) {
//                    if (item.isObject()) {
//                        insertOrUpdateRecord(tableName, (ObjectNode) item);
//                    }
//                }
                // 批量处理数组数据
                if (data.size() > 0) {
                    batchInsertOrUpdateRecords(tableName, (ArrayNode) data, keyField);
                }
            }
        } catch (Exception e) {
            log.error("Error saving data to database: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void batchInsertOrUpdateRecords(String tableName, ArrayNode dataArray, String keyField) throws SQLException {
        if (dataArray.size() == 0) return;

        // 获取第一条数据来构建SQL语句
        ObjectNode firstItem = (ObjectNode) dataArray.get(0);

        // 获取表的列信息
        Map<String, String> columnTypes = getTableColumns(tableName);
        if (columnTypes.isEmpty()) {
            log.error("Cannot get table columns for: " + tableName);
            return;
        }

        // 根据数据库类型确定标识符转义字符
        String identifierQuote = "\""; // 默认使用双引号（标准SQL）
        boolean isMySQL = dataSource.getJdbcUrl().startsWith("jdbc:mysql");
        if (isMySQL) {
            identifierQuote = "`"; // MySQL使用反引号
        }

        List<String> fieldNames = new ArrayList<>();
        List<String> fieldNames2 = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> fields = firstItem.fields();
        while (fields.hasNext()) {
            String fieldName = fields.next().getKey();
            fieldNames2.add(fieldName);
            if(!isMySQL) {
                fieldName = fieldName.toLowerCase();
            }
            fieldNames.add(fieldName);
        }

        String sql;
        if (isMySQL) {
            sql = buildMySQLBatchInsertSQL(tableName, identifierQuote, fieldNames);
        } else {
            sql = buildKingbaseBatchInsertSQL(tableName, identifierQuote, fieldNames, keyField);
        }
        log.info("SQL:" + sql);

        // 执行批量SQL
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            // 关闭自动提交以提高性能
            conn.setAutoCommit(false);

            for (JsonNode item : dataArray) {
                if (item.isObject()) {
                    ObjectNode obj = (ObjectNode) item;

                    int index = 1;
                    for (String fieldName : fieldNames2) {
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

    private void insertOrUpdateRecord(String tableName, ObjectNode data, String keyField) throws SQLException {
        // 获取表的列信息
        Map<String, String> columnTypes = getTableColumns(tableName);

        if (columnTypes.isEmpty()) {
            log.error("Cannot get table columns for: " + tableName);
            return;
        }

        // 根据数据库类型确定标识符转义字符
        String identifierQuote = "\""; // 默认使用双引号（标准SQL）
        boolean isMySQL = dataSource.getJdbcUrl().startsWith("jdbc:mysql");
        if (isMySQL) {
            identifierQuote = "`"; // MySQL使用反引号
        }

        List<String> fieldNames = new ArrayList<>();
        List<String> fieldNames2 = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> fields = data.fields();
        while (fields.hasNext()) {
            String fieldName = fields.next().getKey();
            fieldNames2.add(fieldName);
            if(!isMySQL) {
                fieldName = fieldName.toLowerCase();
            }
            fieldNames.add(fieldName);
        }

        String sql;
        if (isMySQL) {
            sql = buildMySQLInsertSQL(tableName, identifierQuote, fieldNames);
        } else {
            sql = buildKingbaseInsertSQL(tableName, identifierQuote, fieldNames, keyField);
        }

        // 执行SQL
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            int index = 1;
            for (String fieldName : fieldNames2) {
                JsonNode value = data.get(fieldName);
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

    private String buildMySQLBatchInsertSQL(String tableName, String identifierQuote, List<String> fieldNames) {
        StringBuilder insertSQL = new StringBuilder();
        insertSQL.append("INSERT INTO ").append(tableName).append(" (");

        // 收集列名
        StringBuilder columns = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();
        StringBuilder updates = new StringBuilder();

        boolean first = true;
        for (String fieldName : fieldNames) {
            if (!first) {
                columns.append(", ");
                placeholders.append(", ");
                updates.append(", ");
            }

            columns.append(identifierQuote).append(fieldName).append(identifierQuote);
            placeholders.append("?");
            updates.append(identifierQuote).append(fieldName).append(identifierQuote)
                    .append(" = VALUES(").append(identifierQuote).append(fieldName).append(identifierQuote).append(")");

            first = false;
        }

        insertSQL.append(columns.toString())
                .append(") VALUES (")
                .append(placeholders.toString())
                .append(") ON DUPLICATE KEY UPDATE ")
                .append(updates.toString());

        return insertSQL.toString();
    }

    private String buildKingbaseBatchInsertSQL(String tableName, String identifierQuote, List<String> fieldNames, String keyField) {
        StringBuilder insertSQL = new StringBuilder();
        insertSQL.append("INSERT INTO ").append(tableName).append(" (");

        // 收集列名
        StringBuilder columns = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();

        boolean first = true;
        for (String fieldName : fieldNames) {
            if (!first) {
                columns.append(", ");
                placeholders.append(", ");
            }

            columns.append(identifierQuote).append(fieldName).append(identifierQuote);
            placeholders.append("?");

            first = false;
        }

        insertSQL.append(columns.toString())
                .append(") VALUES (")
                .append(placeholders.toString())
                .append(") ON CONFLICT ("); // 假设第一个字段是主键或唯一约束字段

        insertSQL.append(identifierQuote).append(keyField).append(identifierQuote);

        insertSQL.append(") DO UPDATE SET ");

        // 构建更新部分
        StringBuilder updates = new StringBuilder();
        first = true;
        for (String fieldName : fieldNames) {
            if (!first) {
                updates.append(", ");
            }
            updates.append(identifierQuote).append(fieldName).append(identifierQuote)
                    .append(" = EXCLUDED.").append(identifierQuote).append(fieldName).append(identifierQuote);
            first = false;
        }

        insertSQL.append(updates.toString());

        return insertSQL.toString();
    }

    private String buildMySQLInsertSQL(String tableName, String identifierQuote, List<String> fieldNames) {
        StringBuilder insertSQL = new StringBuilder();
        insertSQL.append("INSERT INTO ").append(tableName).append(" (");

        // 收集列名和值
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();
        StringBuilder updates = new StringBuilder();

        boolean first = true;
        for (String fieldName : fieldNames) {
            if (!first) {
                columns.append(", ");
                values.append(", ");
                updates.append(", ");
            }

            columns.append(identifierQuote).append(fieldName).append(identifierQuote);
            values.append("?");
            updates.append(identifierQuote).append(fieldName).append(identifierQuote)
                    .append(" = VALUES(").append(identifierQuote).append(fieldName).append(identifierQuote).append(")");

            first = false;
        }

        insertSQL.append(columns.toString())
                .append(") VALUES (")
                .append(values.toString())
                .append(") ON DUPLICATE KEY UPDATE ")
                .append(updates.toString());

        return insertSQL.toString();
    }

    private String buildKingbaseInsertSQL(String tableName, String identifierQuote, List<String> fieldNames, String keyField) {
        StringBuilder insertSQL = new StringBuilder();
        insertSQL.append("INSERT INTO ").append(tableName).append(" (");

        // 收集列名和值
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();

        boolean first = true;
        for (String fieldName : fieldNames) {
            if (!first) {
                columns.append(", ");
                values.append(", ");
            }

            columns.append(identifierQuote).append(fieldName).append(identifierQuote);
            values.append("?");

            first = false;
        }

        insertSQL.append(columns.toString())
                .append(") VALUES (")
                .append(values.toString())
                .append(") ON CONFLICT ("); // 假设第一个字段是主键或唯一约束字段

        insertSQL.append(identifierQuote).append(keyField).append(identifierQuote);

        insertSQL.append(") DO UPDATE SET ");

        // 构建更新部分
        StringBuilder updates = new StringBuilder();
        first = true;
        for (String fieldName : fieldNames) {
            if (!first) {
                updates.append(", ");
            }
            updates.append(identifierQuote).append(fieldName).append(identifierQuote)
                    .append(" = EXCLUDED.").append(identifierQuote).append(fieldName).append(identifierQuote);
            first = false;
        }

        insertSQL.append(updates.toString());

        return insertSQL.toString();
    }
}
