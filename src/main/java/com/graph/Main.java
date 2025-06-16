package com.graph;

import com.graph.core.CycleFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        logger.info("Starting cycle detection process...");
        
        long startTime = System.currentTimeMillis();
        
        CycleFinder finder = new CycleFinder();
        finder.findCycles();
        
        long endTime = System.currentTimeMillis();
        long duration = (endTime - startTime) / 1000; // 转换为秒
        
        logger.info("Process completed in {} seconds", duration);
        logger.info("Total cycles found: {}", finder.getFoundCycles());
        logger.info("Total nodes processed: {}", finder.getProcessedNodes());
        
        // 输出所有找到的环
        logger.info("Logging found cycles..."); // Added a header for this section
        finder.getCycles().forEach(cycle -> {
            if (cycle == null || cycle.isEmpty()) {
                logger.info("Cycle: (empty or null cycle)");
                return;
            }
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (Long nodeId : cycle) {
                if (!first) {
                    sb.append(" -> ");
                }
                sb.append(nodeId);
                first = false;
            }
            logger.info("Cycle: {}", sb.toString());
        });
        logger.info("Finished logging cycles."); // Added a footer for this section
    }
} 