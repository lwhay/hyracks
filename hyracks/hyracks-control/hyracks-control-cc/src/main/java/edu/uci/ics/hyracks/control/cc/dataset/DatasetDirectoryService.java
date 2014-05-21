/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.control.cc.dataset;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.hyracks.api.comm.NetworkAddress;
import edu.uci.ics.hyracks.api.dataset.DatasetDirectoryRecord;
import edu.uci.ics.hyracks.api.dataset.DatasetJobRecord;
import edu.uci.ics.hyracks.api.dataset.DatasetJobRecord.Status;
import edu.uci.ics.hyracks.api.dataset.IDatasetStateRecord;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.dataset.ResultSetMetaData;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.IActivityClusterGraphGeneratorFactory;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.control.common.dataset.ResultStateSweeper;
import edu.uci.ics.hyracks.control.common.work.IResultCallback;

/**
 * TODO(madhusudancs): The potential perils of this global dataset directory service implementation is that, the jobs
 * location information is never evicted from the memory and the memory usage grows as the number of jobs in the system
 * grows. What we should possibly do is, add an API call for the client to say that it received everything it has to for
 * the job (after it receives all the results) completely. Then we can just get rid of the location information for that
 * job.
 */
public class DatasetDirectoryService implements IDatasetDirectoryService {
    private final long resultTTL;

    private final long resultSweepThreshold;

    private final Map<JobId, IDatasetStateRecord> jobResultLocations;

    private final Map<Pair<JobId, ResultSetId>, Pair<DatasetDirectoryRecord[], IResultCallback<DatasetDirectoryRecord[]>>> waiters;

    public DatasetDirectoryService(long resultTTL, long resultSweepThreshold) {
        this.resultTTL = resultTTL;
        this.resultSweepThreshold = resultSweepThreshold;
        jobResultLocations = new LinkedHashMap<JobId, IDatasetStateRecord>();
        waiters = new HashMap<Pair<JobId, ResultSetId>, Pair<DatasetDirectoryRecord[], IResultCallback<DatasetDirectoryRecord[]>>>();
    }

    @Override
    public void init(ExecutorService executor) {
        executor.execute(new ResultStateSweeper(this, resultTTL, resultSweepThreshold));
    }

    @Override
    public synchronized void notifyJobCreation(JobId jobId, IActivityClusterGraphGeneratorFactory acggf)
            throws HyracksException {
        DatasetJobRecord djr = (DatasetJobRecord) jobResultLocations.get(jobId);
        if (djr == null) {
            djr = new DatasetJobRecord();
            jobResultLocations.put(jobId, djr);
        }
    }

    @Override
    public void notifyJobStart(JobId jobId) throws HyracksException {
        // Auto-generated method stub
    }

    @Override
    public void notifyJobFinish(JobId jobId) throws HyracksException {
        // Auto-generated method stub
    }

    @Override
    public synchronized void registerResultPartitionLocation(JobId jobId, ResultSetId rsId, boolean orderedResult,
            boolean emptyResult, int partition, int nPartitions, NetworkAddress networkAddress) {
        DatasetJobRecord djr = (DatasetJobRecord) jobResultLocations.get(jobId);

        ResultSetMetaData resultSetMetaData = djr.get(rsId);
        if (resultSetMetaData == null) {
            resultSetMetaData = new ResultSetMetaData(orderedResult, new DatasetDirectoryRecord[nPartitions]);
            djr.put(rsId, resultSetMetaData);
        }

        DatasetDirectoryRecord[] records = resultSetMetaData.getRecords();
        if (records[partition] == null) {
            records[partition] = new DatasetDirectoryRecord();
        }
        records[partition].setNetworkAddress(networkAddress);
        records[partition].setEmpty(emptyResult);
        records[partition].start();

        Pair<JobId, ResultSetId> key = Pair.of(jobId, rsId);
        Pair<DatasetDirectoryRecord[], IResultCallback<DatasetDirectoryRecord[]>> value = waiters.get(key);
        if (value != null) {
            try {
                DatasetDirectoryRecord[] updatedRecords = updatedRecords(jobId, rsId, value.getLeft());
                if (updatedRecords != null) {
                    waiters.remove(key);
                    value.getRight().setValue(updatedRecords);
                }
            } catch (Exception e) {
                value.getRight().setException(e);
            }
        }
        notifyAll();
    }

    @Override
    public synchronized void reportResultPartitionWriteCompletion(JobId jobId, ResultSetId rsId, int partition) {
        int successCount = 0;

        DatasetJobRecord djr = (DatasetJobRecord) jobResultLocations.get(jobId);
        ResultSetMetaData resultSetMetaData = djr.get(rsId);
        DatasetDirectoryRecord[] records = resultSetMetaData.getRecords();
        records[partition].writeEOS();

        for (DatasetDirectoryRecord record : records) {
            if ((record != null) && (record.getStatus() == DatasetDirectoryRecord.Status.SUCCESS)) {
                successCount++;
            }
        }
        if (successCount == records.length) {
            djr.success();
        }
        notifyAll();
    }

    @Override
    public synchronized void reportResultPartitionFailure(JobId jobId, ResultSetId rsId, int partition) {
        DatasetJobRecord djr = (DatasetJobRecord) jobResultLocations.get(jobId);
        if (djr != null) {
            djr.fail();
        }
        notifyAll();
    }

    @Override
    public synchronized void reportJobFailure(JobId jobId, List<Exception> exceptions) {
        DatasetJobRecord djr = (DatasetJobRecord) jobResultLocations.get(jobId);
        if (djr != null) {
            djr.fail(exceptions);
        }
        notifyAll();
    }

    @Override
    public synchronized Status getResultStatus(JobId jobId, ResultSetId rsId) throws HyracksDataException {
        DatasetJobRecord djr;
        while ((djr = (DatasetJobRecord) jobResultLocations.get(jobId)) == null) {
            try {
                wait();
            } catch (InterruptedException e) {
                throw new HyracksDataException(e);
            }
        }

        return djr.getStatus();
    }

    @Override
    public Map<JobId, IDatasetStateRecord> getStateMap() {
        return jobResultLocations;
    }

    @Override
    public void deinitState(JobId jobId) {
        jobResultLocations.remove(jobResultLocations.get(jobId));
    }

    @Override
    public synchronized void getResultPartitionLocations(JobId jobId, ResultSetId rsId,
            DatasetDirectoryRecord[] knownRecords, IResultCallback<DatasetDirectoryRecord[]> callback)
            throws HyracksDataException {
        DatasetDirectoryRecord[] updatedRecords = updatedRecords(jobId, rsId, knownRecords);
        if (updatedRecords == null) {
            waiters.put(Pair.of(jobId, rsId), Pair.of(knownRecords, callback));
        } else {
            callback.setValue(updatedRecords);
        }
    }

    /**
     * Compares the records already known by the client for the given job's result set id with the records that the
     * dataset directory service knows and if there are any newly discovered records returns a whole array with the
     * new records filled in.
     * 
     * @param jobId
     *            - Id of the job for which the directory records should be retrieved.
     * @param rsId
     *            - Id of the result set for which the directory records should be retrieved.
     * @param knownRecords
     *            - An array of directory records that the client is already aware of.
     * @return
     *         Returns the updated records if new record were discovered, null otherwise
     * @throws HyracksDataException
     *             TODO(madhusudancs): Think about caching (and still be stateless) instead of this ugly O(n) iterations for
     *             every check. This already looks very expensive.
     */
    private DatasetDirectoryRecord[] updatedRecords(JobId jobId, ResultSetId rsId, DatasetDirectoryRecord[] knownRecords)
            throws HyracksDataException {
        DatasetJobRecord djr = (DatasetJobRecord) jobResultLocations.get(jobId);

        if (djr == null) {
            throw new HyracksDataException("Requested JobId " + jobId + " doesn't exist");
        }

        if (djr.getStatus() == Status.FAILED) {
            List<Exception> caughtExceptions = djr.getExceptions();
            if (caughtExceptions == null) {
                throw new HyracksDataException("Job failed.");
            } else {
                throw new HyracksDataException(caughtExceptions.get(caughtExceptions.size() - 1));
            }
        }

        ResultSetMetaData resultSetMetaData = djr.get(rsId);
        if (resultSetMetaData == null || resultSetMetaData.getRecords() == null) {
            return null;
        }

        DatasetDirectoryRecord[] records = resultSetMetaData.getRecords();

        return Arrays.equals(records, knownRecords) ? null : records;
    }
}