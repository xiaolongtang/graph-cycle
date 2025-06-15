package com.graph.core;

import com.graph.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
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

    private void loadGraphData() throws SQLException {
        String sql = String.format("SELECT %s, %s FROM %s", sourceColumn, targetColumn, tableName);
        
        try (Connection conn = AppConfig.getDataSource().getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {

            while (rs.next()) {
                long source = rs.getLong(1);
                long target = rs.getLong(2);
                
                adjacencyList.computeIfAbsent(source, k -> ConcurrentHashMap.newKeySet()).add(target);
                // 确保目标节点也在图中
                adjacencyList.computeIfAbsent(target, k -> ConcurrentHashMap.newKeySet());
            }
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
} 