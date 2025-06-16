package com.graph.core;

import com.graph.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class CycleFinder {
    private static final Logger logger = LoggerFactory.getLogger(CycleFinder.class);
    private final ExecutorService executorService;
    private final Map<Long, Set<Long>> adjacencyList;
    private final AtomicLong processedNodes = new AtomicLong(0);
    private final AtomicLong foundCycles = new AtomicLong(0);
    private final Set<List<Long>> cycles = Collections.synchronizedSet(new HashSet<>());
    private final int batchSize;
    private final String tableName;
    private final String sourceColumn;
    private final String targetColumn;

    public CycleFinder() {
        this.executorService = Executors.newFixedThreadPool(AppConfig.getThreadPoolSize());
        this.adjacencyList = new ConcurrentHashMap<>();
        this.batchSize = AppConfig.getBatchSize();
        this.tableName = AppConfig.getTableName();
        this.sourceColumn = AppConfig.getSourceColumn();
        this.targetColumn = AppConfig.getTargetColumn();
    }

    public void findCycles() {
        try {
            loadGraphData();
            logger.info("Graph data loaded successfully. Starting cycle detection...");
            
            // 获取所有节点
            Set<Long> nodes = new HashSet<>(adjacencyList.keySet());
            logger.info("Total nodes to process: {}", nodes.size());

            // 使用CompletableFuture并行处理每个起始节点
            List<CompletableFuture<Void>> futures = nodes.stream()
                .map(node -> CompletableFuture.runAsync(() -> findCyclesFromNode(node), executorService))
                .collect(Collectors.toList());

            // 等待所有任务完成
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            logger.info("Cycle detection completed. Found {} cycles", foundCycles.get());
            logger.info("Processed {} nodes", processedNodes.get());
            
            // 输出找到的环
            cycles.forEach(cycle -> logger.info("Found cycle: {}", cycle));

        } catch (Exception e) {
            logger.error("Error during cycle detection", e);
        } finally {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private void loadGraphData() throws SQLException, IOException {
        if ("csv".equalsIgnoreCase(AppConfig.getDataSourceType())) {
            loadFromCsv();
        } else {
            loadFromDatabase();
        }
    }

    private void loadFromCsv() throws IOException {
        String csvFilename = AppConfig.getInputCsvFilename();
        int batchSize = AppConfig.getBatchSize();
        List<String> lines = new ArrayList<>();
        
        // 读取CSV文件
        try (BufferedReader reader = new BufferedReader(new FileReader(csvFilename))) {
            String line;
            // 跳过头部
            reader.readLine();
            
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
        }

        // 并行处理数据
        int numThreads = AppConfig.getThreadPoolSize();
        ExecutorService loadExecutor = Executors.newFixedThreadPool(numThreads);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (int i = 0; i < lines.size(); i += batchSize) {
            final int start = i;
            final int end = Math.min(start + batchSize, lines.size());
            List<String> batch = lines.subList(start, end);

            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                for (String line : batch) {
                    String[] parts = line.split(",");
                    if (parts.length >= 2) {
                        try {
                            long source = Long.parseLong(parts[0].trim());
                            long target = Long.parseLong(parts[1].trim());
                            
                            adjacencyList.computeIfAbsent(source, k -> ConcurrentHashMap.newKeySet()).add(target);
                            adjacencyList.computeIfAbsent(target, k -> ConcurrentHashMap.newKeySet());
                        } catch (NumberFormatException e) {
                            logger.error("Error parsing line: {}", line, e);
                        }
                    }
                }
            }, loadExecutor);
            
            futures.add(future);
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } finally {
            loadExecutor.shutdown();
        }
        
        logger.info("Loaded {} lines from CSV file", lines.size());
    }

    private void loadFromDatabase() throws SQLException {
        // 原有的数据库加载代码
        String countSql = String.format("SELECT COUNT(*) FROM %s", tableName);
        String sql = String.format("SELECT %s, %s FROM %s WHERE ROWNUM >= ? AND ROWNUM < ?", 
            sourceColumn, targetColumn, tableName);
        long totalRows;
        
        try (Connection conn = AppConfig.getDataSource().getConnection();
             PreparedStatement countStmt = conn.prepareStatement(countSql);
             ResultSet countRs = countStmt.executeQuery()) {
            countRs.next();
            totalRows = countRs.getLong(1);
        }

        // 使用配置文件中的batch.size
        int batchSize = AppConfig.getBatchSize();
        int numThreads = AppConfig.getThreadPoolSize();
        ExecutorService loadExecutor = Executors.newFixedThreadPool(numThreads);
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (long start = 1; start <= totalRows; start += batchSize) {
            final long batchStart = start;
            final long batchEnd = Math.min(start + batchSize, totalRows + 1);
            
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try (Connection conn = AppConfig.getDataSource().getConnection();
                     PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setLong(1, batchStart);
                    pstmt.setLong(2, batchEnd);
                    
                    try (ResultSet rs = pstmt.executeQuery()) {
                        while (rs.next()) {
                            long source = rs.getLong(1);
                            long target = rs.getLong(2);
                            
                            adjacencyList.computeIfAbsent(source, k -> ConcurrentHashMap.newKeySet()).add(target);
                            adjacencyList.computeIfAbsent(target, k -> ConcurrentHashMap.newKeySet());
                        }
                    }
                } catch (SQLException e) {
                    logger.error("Error loading batch data", e);
                    throw new CompletionException(e);
                }
            }, loadExecutor);
            
            futures.add(future);
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            // 数据加载完成后保存到CSV
            saveToCSV();
        } finally {
            loadExecutor.shutdown();
        }
    }

    private void findCyclesFromNode(Long startNode) {
        try {
            Set<Long> visited = new HashSet<>();
            List<Long> path = new ArrayList<>();
            findCyclesDFS(startNode, startNode, visited, path);
            processedNodes.incrementAndGet();
            
            if (processedNodes.get() % 1000 == 0) {
                logger.info("Processed {} nodes, found {} cycles", processedNodes.get(), foundCycles.get());
            }
        } catch (Exception e) {
            logger.error("Error processing node {}: {}", startNode, e.getMessage());
        }
    }

    private void findCyclesDFS(Long current, Long start, Set<Long> visited, List<Long> path) {
        visited.add(current);
        path.add(current);

        Set<Long> neighbors = adjacencyList.getOrDefault(current, Collections.emptySet());
        for (Long neighbor : neighbors) {
            if (neighbor.equals(start) && path.size() > 2) {
                // 找到环
                List<Long> cycle = new ArrayList<>(path);
                cycles.add(cycle);
                foundCycles.incrementAndGet();
            } else if (!visited.contains(neighbor)) {
                findCyclesDFS(neighbor, start, visited, path);
            }
        }

        visited.remove(current);
        path.remove(path.size() - 1);
    }

    public Set<List<Long>> getCycles() {
        return new HashSet<>(cycles);
    }

    public long getProcessedNodes() {
        return processedNodes.get();
    }

    public long getFoundCycles() {
        return foundCycles.get();
    }

    private void saveToCSV() {
        String csvFilename = AppConfig.getCsvFilename();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFilename))) {
            // 写入CSV头
            writer.write("source,target\n");
            
            // 写入数据
            for (Map.Entry<Long, Set<Long>> entry : adjacencyList.entrySet()) {
                Long source = entry.getKey();
                for (Long target : entry.getValue()) {
                    writer.write(source + "," + target + "\n");
                }
            }
            logger.info("CSV file saved successfully: {}", csvFilename);
        } catch (IOException e) {
            logger.error("Error saving CSV file", e);
        }
    }
}