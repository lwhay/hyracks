/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.group.global.costmodels;

public class DatasetStats {

    private long recordCount, groupCount;

    private int recordSize;

    public DatasetStats() {
        recordCount = 0;
        groupCount = 0;
        recordSize = 0;
    }

    public DatasetStats(long recordCount, long groupCount, int recordSize) {
        this.recordCount = recordCount;
        this.groupCount = groupCount;
        this.recordSize = recordSize;
    }

    public DatasetStats(DatasetStats statToCopy) {
        this.recordCount = statToCopy.recordCount;
        this.groupCount = statToCopy.groupCount;
        this.recordSize = statToCopy.recordSize;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }

    public void updateRecordCount(long recordCountDelta) {
        this.recordCount += recordCountDelta;
    }

    public long getGroupCount() {
        return groupCount;
    }

    public void setGroupCount(long groupCount) {
        this.groupCount = groupCount;
    }

    public int getRecordSize() {
        return recordSize;
    }

    public void setRecordSize(int recordSize) {
        this.recordSize = recordSize;
    }

    public void updateGroupCount(long groupCountDelta) {
        this.groupCount += groupCountDelta;
    }

    public void resetWithDatasetStats(DatasetStats ds) {
        this.recordCount = ds.recordCount;
        this.groupCount = ds.groupCount;
        this.recordSize = ds.getRecordSize();
    }
}
