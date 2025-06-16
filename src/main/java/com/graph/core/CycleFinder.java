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
        // Use AppConfig.getBatchSize() as the number of lines to read into one chunk for processing
        int processingChunkSize = AppConfig.getBatchSize();

        int numThreads = AppConfig.getThreadPoolSize();
        ExecutorService loadExecutor = Executors.newFixedThreadPool(numThreads);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        List<String> lineChunk = new ArrayList<>(processingChunkSize);
        long linesReadCounter = 0; // Optional: for logging approximate lines read

        logger.info("Starting CSV load from '{}' with chunk size {}.", csvFilename, processingChunkSize);

        try (BufferedReader reader = new BufferedReader(new FileReader(csvFilename))) {
            String line;
            // Skip header if necessary - assuming first line is header
            String header = reader.readLine();
            if (header == null) {
                logger.warn("CSV file is empty or header is missing: {}", csvFilename);
                return;
            }
            logger.info("CSV Header: {}", header);

            while ((line = reader.readLine()) != null) {
                linesReadCounter++;
                lineChunk.add(line);
                if (lineChunk.size() >= processingChunkSize) {
                    submitCsvChunkForProcessing(lineChunk, loadExecutor, futures);
                    lineChunk = new ArrayList<>(processingChunkSize); // Reset for the next chunk
                }
            }
            
            // Process any remaining lines in the last partial chunk
            if (!lineChunk.isEmpty()) {
                submitCsvChunkForProcessing(lineChunk, loadExecutor, futures);
            }

            logger.info("All CSV chunks submitted for processing. Total lines read (approx, excluding header): {}. Waiting for completion...", linesReadCounter);
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            logger.info("All CSV data processed successfully.");

        } catch (IOException e) {
            logger.error("Error reading CSV file: {}", csvFilename, e);
            // If an IO error occurs, pending tasks might still be running.
            // The executor shutdown in finally will handle them.
            throw e;
        } catch (Exception e) {
            // Catches exceptions from CompletableFuture.allOf().join() such as CompletionException
            logger.error("Error processing CSV data or waiting for tasks to complete", e);
            if (e instanceof CompletionException && e.getCause() != null) {
                throw new IOException("Failed during CSV data processing: " + e.getCause().getMessage(), e.getCause());
            }
            throw new IOException("Failed during CSV data processing: " + e.getMessage(), e);
        } finally {
            loadExecutor.shutdown();
            try {
                if (!loadExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                    logger.warn("CSV load executor did not terminate gracefully, forcing shutdown.");
                    loadExecutor.shutdownNow();
                } else {
                    logger.info("CSV load executor terminated gracefully.");
                }
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for CSV load executor to terminate.", e);
                loadExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        logger.info("Finished loading data from CSV file: {}", csvFilename);
    }

    private void submitCsvChunkForProcessing(List<String> chunkToProcess,
                                             ExecutorService executor,
                                             List<CompletableFuture<Void>> futuresList) {
        // Pass a copy of the list to the async task to prevent modification issues if chunkToProcess were cleared instead of reassigned.
        List<String> batchForTask = new ArrayList<>(chunkToProcess);
        logger.debug("Submitting CSV chunk of size {} for processing.", batchForTask.size());

        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
            int successfullyProcessedLines = 0;
            for (String fileLine : batchForTask) {
                String[] parts = fileLine.split(","); // Assuming default comma delimiter
                if (parts.length >= 2) {
                    try {
                        long source = Long.parseLong(parts[0].trim());
                        long target = Long.parseLong(parts[1].trim());

                        adjacencyList.computeIfAbsent(source, k -> ConcurrentHashMap.newKeySet()).add(target);
                        adjacencyList.computeIfAbsent(target, k -> ConcurrentHashMap.newKeySet()); // Ensure target node exists
                        successfullyProcessedLines++;
                    } catch (NumberFormatException e) {
                        logger.error("Error parsing number in CSV line: '{}'. Skipping line.", fileLine, e);
                    } catch (Exception e) {
                        logger.error("Unexpected error processing CSV line: '{}'. Skipping line.", fileLine, e);
                    }
                } else {
                    logger.warn("Skipping malformed CSV line (expected at least 2 parts): '{}'", fileLine);
                }
            }
            logger.debug("Finished processing CSV chunk. Successfully processed {}/{} lines.", successfullyProcessedLines, batchForTask.size());
        }, executor);

        futuresList.add(future);
    }

    private void loadFromDatabase() throws SQLException {
        // Keyset pagination: Requires a unique, ordered column (e.g., primary key)
        // Assume AppConfig.getPrimaryKeyColumn() provides the name of this column, e.g., "ID"
        // This column must be of a type that can be ordered, e.g., NUMBER, TIMESTAMP.
        // For this example, we assume it's a Long.
        String primaryKeyColumnName = AppConfig.getPrimaryKeyColumn();
        if (primaryKeyColumnName == null || primaryKeyColumnName.trim().isEmpty()) {
            logger.error("Primary key column name is not configured. Cannot use keyset pagination.");
            throw new SQLException("Primary key column name not configured.");
        }

        String sql = String.format(
            "SELECT %s, %s, %s FROM (" +
            "  SELECT inner_select.%s, inner_select.%s, inner_select.%s " +
            "  FROM (SELECT %s, %s, %s FROM %s WHERE %s > ? ORDER BY %s) inner_select " +
            "  WHERE ROWNUM <= ?" +
            ")",
            sourceColumn, targetColumn, primaryKeyColumnName, // Outer select columns
            sourceColumn, targetColumn, primaryKeyColumnName, // Inner select columns (repeated for clarity in sub-sub-query)
            sourceColumn, targetColumn, primaryKeyColumnName, // Columns for the innermost select
            tableName,             // Table name
            primaryKeyColumnName,  // WHERE primaryKeyColumnName > ?
            primaryKeyColumnName,  // ORDER BY primaryKeyColumnName
            // batchSize is the second parameter for ROWNUM <= ?
        );
        logger.info("Using keyset pagination SQL: {}", sql);

        long lastSeenPkValue = 0L; // Initial value for PK > ? (assuming PKs are positive)
                                   // Adjust if PK can be 0 or negative, or use a different first query.

        int batchSize = AppConfig.getBatchSize();
        int numThreads = AppConfig.getThreadPoolSize(); // For processing loaded data
        ExecutorService loadExecutor = Executors.newFixedThreadPool(numThreads);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        boolean moreDataToFetch = true;

        logger.info("Starting database load with keyset pagination. Batch size: {}", batchSize);

        while (moreDataToFetch) {
            final long currentLastSeenPkValue = lastSeenPkValue; // Effective final for lambda
            List<Map.Entry<Long, Long>> batchData = new ArrayList<>();
            long maxPkInBatch = -1L; // Initialize to a value lower than any expected PK in the batch
            int rowCountInBatch = 0;

            // Log the attempt to fetch a batch
            logger.debug("Fetching batch with {} > {}", primaryKeyColumnName, currentLastSeenPkValue);

            try (Connection conn = AppConfig.getDataSource().getConnection();
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {

                pstmt.setLong(1, currentLastSeenPkValue); // For WHERE primaryKeyColumnName > ?
                pstmt.setInt(2, batchSize);              // For ROWNUM <= ?

                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        rowCountInBatch++;
                        long source = rs.getLong(sourceColumn); // Use column names for robustness
                        long target = rs.getLong(targetColumn);
                        long currentPk = rs.getLong(primaryKeyColumnName);

                        batchData.add(new java.util.AbstractMap.SimpleImmutableEntry<>(source, target));

                        if (currentPk > maxPkInBatch) {
                            maxPkInBatch = currentPk;
                        }
                    }
                }
            } catch (SQLException e) {
                logger.error("Error loading batch data with keyset pagination (PK > {})", currentLastSeenPkValue, e);
                // Propagate the exception to stop the process if a batch fails.
                // Depending on requirements, partial processing could be allowed, but generally, it's safer to stop.
                throw new CompletionException(e);
            }
            
            logger.debug("Batch fetched with {} rows. Max PK in batch: {}", rowCountInBatch, maxPkInBatch);

            if (rowCountInBatch > 0 && !batchData.isEmpty()) {
                CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                    logger.debug("Processing batch of {} entries.", batchData.size());
                    for (Map.Entry<Long, Long> entry : batchData) {
                        long source = entry.getKey();
                        long target = entry.getValue();
                        adjacencyList.computeIfAbsent(source, k -> ConcurrentHashMap.newKeySet()).add(target);
                        adjacencyList.computeIfAbsent(target, k -> ConcurrentHashMap.newKeySet());
                    }
                    logger.debug("Finished processing batch of {} entries.", batchData.size());
                }, loadExecutor);
                futures.add(future);
            }

            if (rowCountInBatch < batchSize) {
                moreDataToFetch = false; // This was the last batch
                logger.info("Last batch fetched ({} rows, less than batch size {}). No more data.", rowCountInBatch, batchSize);
            } else if (rowCountInBatch == 0) {
                moreDataToFetch = false; // No data found in this iteration
                logger.info("No rows returned in this batch. Assuming end of data.");
            } else {
                lastSeenPkValue = maxPkInBatch; // Update for the next iteration's WHERE clause
                logger.debug("Continuing to next batch. Next {} > {}", primaryKeyColumnName, lastSeenPkValue);
            }
        }

        logger.info("All database batches scheduled for processing. Waiting for completion...");
        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            logger.info("All batch processing completed.");
            // Data loaded, now save to CSV if needed (as per original logic)
            saveToCSV();
        } catch (Exception e) {
            logger.error("Error during parallel processing of loaded data or saving to CSV", e);
            // If CompletionException, unwrap it
            if (e instanceof CompletionException && e.getCause() != null) {
                throw new SQLException("Failed during data processing after load: " + e.getCause().getMessage(), e.getCause());
            }
            throw new SQLException("Failed during data processing after load: " + e.getMessage(), e);
        } finally {
            loadExecutor.shutdown();
            try {
                if (!loadExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                    loadExecutor.shutdownNow();
                }
            } catch (InterruptedException e) {
                loadExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("Load executor shut down.");
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