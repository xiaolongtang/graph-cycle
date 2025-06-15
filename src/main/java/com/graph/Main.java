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
        finder.getCycles().forEach(cycle -> 
            logger.info("Cycle: {}", String.join(" -> ", cycle.stream().map(String::valueOf).toArray(String[]::new)))
        );
    }
} 