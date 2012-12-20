package edu.uci.ics.hyracks.imru.util;

import java.util.logging.Level;
import java.util.logging.Logger;

public class MemoryStatsLogger {

    public static void logHeapStats(Logger log, String msg) {
        if (log.isLoggable(Level.INFO)) {
            int mb = 1024*1024;

            //Getting the runtime reference from system
            Runtime runtime = Runtime.getRuntime();

            log.info("##### Heap utilization statistics [MB] #####");
            log.info(msg);

            //Print used memory
            log.info("Used Memory:"
                + (runtime.totalMemory() - runtime.freeMemory()) / mb);

            //Print free memory
            log.info("Free Memory:"
                + runtime.freeMemory() / mb);

            //Print total available memory
            log.info("Total Memory:" + runtime.totalMemory() / mb);

            //Print Maximum available memory
            log.info("Max Memory:" + runtime.maxMemory() / mb);
        }
    }
}
