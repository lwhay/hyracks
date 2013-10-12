/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
