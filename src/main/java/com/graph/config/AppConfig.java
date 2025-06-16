package com.graph.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppConfig {
    private static final Logger logger = LoggerFactory.getLogger(AppConfig.class);
    private static final Properties properties = new Properties();
    private static DataSource dataSource;

    static {
        try (InputStream input = AppConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new RuntimeException("Unable to find application.properties");
            }
            properties.load(input);
            initializeDataSource();
        } catch (IOException e) {
            logger.error("Failed to load configuration", e);
            throw new RuntimeException("Failed to load configuration", e);
        }
    }

    private static void initializeDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(properties.getProperty("db.url"));
        config.setUsername(properties.getProperty("db.username"));
        config.setPassword(properties.getProperty("db.password"));
        
        // 增加连接池大小以支持并行加载
        config.setMaximumPoolSize(Integer.parseInt(properties.getProperty("db.pool.size", "20")));
        config.setMinimumIdle(Integer.parseInt(properties.getProperty("db.pool.min.idle", "10")));
        
        // 优化连接池配置
        config.setIdleTimeout(Long.parseLong(properties.getProperty("db.pool.idle.timeout", "300000")));
        config.setConnectionTimeout(Long.parseLong(properties.getProperty("db.pool.connection.timeout", "30000")));
        config.setMaxLifetime(Long.parseLong(properties.getProperty("db.pool.max.lifetime", "1800000")));
        
        // 添加性能优化配置
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        config.addDataSourceProperty("useServerPrepStmts", "true");
        
        dataSource = new HikariDataSource(config);
    }

    public static DataSource getDataSource() {
        return dataSource;
    }

    public static int getThreadPoolSize() {
        return Integer.parseInt(properties.getProperty("thread.pool.size", 
            String.valueOf(Runtime.getRuntime().availableProcessors())));
    }

    public static int getBatchSize() {
        return Integer.parseInt(properties.getProperty("batch.size", "1000"));
    }

    public static String getTableName() {
        return properties.getProperty("table.name", "RELATION");
    }

    public static String getSourceColumn() {
        return properties.getProperty("column.source", "source_id");
    }

    public static String getTargetColumn() {
        return properties.getProperty("column.target", "target_id");
    }

    public static String getDataSourceType() {
        return properties.getProperty("data.source.type", "database");
    }

    public static String getInputCsvFilename() {
        return properties.getProperty("input.csv.filename", "graph_data.csv");
    }
    public static String getCsvFilename() {
        return properties.getProperty("output.csv.filename", "graph_data.csv");
    }
}