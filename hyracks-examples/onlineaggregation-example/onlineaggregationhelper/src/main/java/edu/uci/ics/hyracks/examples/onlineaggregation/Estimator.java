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
package edu.uci.ics.hyracks.examples.onlineaggregation;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class Estimator {
    private final UUID jobId;
    private final int index;
    private Map<Integer, ResultFile> resultFiles;
    private EstimatorThread thread;
    private boolean done;

    private class ResultFile {
        File file;
        long timestamp;
    }

    public Estimator(UUID jobId, int index) {
        this.jobId = jobId;
        this.index = index;
        resultFiles = new Hashtable<Integer, ResultFile>();
        done = false;
        thread = new EstimatorThread();
        thread.start();
    }

    public void resultReceived(File textFile, int blockId) throws HyracksDataException {
        ResultFile rf = new ResultFile();
        rf.file = textFile;
        try {
            rf.timestamp = CentralQueueAccessor.getQueue().getTimestamp();
        } catch (Exception e) {
            throw new HyracksDataException(e);
        }
        synchronized (resultFiles) {
            resultFiles.put(blockId, rf);
            resultFiles.notifyAll();
        }
    }

    private class EstimatorThread extends Thread {
        private int counter;

        public EstimatorThread() {
            setDaemon(true);
        }

        public void run() {
            IInputSplitQueue queue = CentralQueueAccessor.getQueue();
            int lastNumResultFiles = 0;
            while (true) {
                Map<Integer, ResultFile> fileMapCopy;
                synchronized (resultFiles) {
                    while (true) {
                        int nResultFiles = resultFiles.size();
                        if (nResultFiles <= lastNumResultFiles && !done) {
                            try {
                                resultFiles.wait();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        } else {
                            if (nResultFiles <= lastNumResultFiles) {
                                return;
                            }
                            lastNumResultFiles = nResultFiles;
                            break;
                        }
                    }
                    fileMapCopy = new HashMap<Integer, ResultFile>(resultFiles);
                }
                try {
                    long timestamp = queue.getTimestamp();
                    Map<Integer, List<StatsRecord>> statistics = queue.getStatistics(jobId);
                    estimate(fileMapCopy, statistics, timestamp);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private void estimate(Map<Integer, ResultFile> fileMap, Map<Integer, List<StatsRecord>> statistics,
                long timestamp) throws IOException {
            File estimateFile = File.createTempFile(jobId.toString() + "_" + index + "_" + (counter++) + "_"
                    + timestamp + "_", ".est");
            PrintWriter out = new PrintWriter(estimateFile);
            for (Map.Entry<Integer, List<StatsRecord>> e : statistics.entrySet()) {
                for (StatsRecord sr : e.getValue()) {
                    ResultFile rf = fileMap.get(sr.blockId);
                    out.print(e.getKey());
                    out.print('|');
                    out.print(sr.mapLocation);
                    out.print('|');
                    out.print(sr.blockId);
                    out.print('|');
                    out.print(sr.startTime);
                    out.print('|');
                    out.print(sr.endTime);
                    out.print('|');
                    out.print(rf != null ? rf.timestamp : 0);
                    out.print('|');
                    out.print(sr.fileName);
                    out.print('|');
                    out.print(sr.startOffset);
                    out.print('|');
                    out.print(sr.length);
                    out.print('|');
                    out.print(sr.locations == null ? "" : Arrays.deepToString(sr.locations));
                    out.print('|');
                    String fileName = "";
                    if (rf != null) {
                        fileName = rf.file.getName();
                    }
                    out.println(fileName);
                }
            }
            out.close();
        }
    }

    public void done() {
        synchronized (resultFiles) {
            done = true;
            resultFiles.notifyAll();
        }
        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}