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
package edu.uci.ics.hyracks.dataflow.std.group.global;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.HistogramUtils;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper;

public abstract class AbstractHistogramPushBasedGrouper implements IPushBasedGrouper {

    private int[] histogram;
    private boolean enableHistogram = false;
    protected final String debugID;

    public AbstractHistogramPushBasedGrouper(boolean enableHistorgram) {
        this.histogram = new int[HistogramUtils.HISTOGRAM_SLOTS];
        this.enableHistogram = enableHistorgram;
        this.debugID = this.getClass().getSimpleName() + "." + String.valueOf(Thread.currentThread().getId());
    }

    protected void insertIntoHistogram(IFrameTupleAccessor accessor, int tupleIndex, int[] keyFields)
            throws HyracksDataException {
        if (enableHistogram) {
            this.histogram[HistogramUtils.getHistogramBucketID(accessor, tupleIndex, keyFields)]++;
        }
    }

    protected void insertIntoHistogram(int hashValue) {
        if (enableHistogram) {
            this.histogram[hashValue % this.histogram.length]++;
        }
    }

    public int[] getDataDistHistogram() throws HyracksDataException {
        return this.histogram;
    }

    protected void resetHistogram() {
        for (int i = 0; i < histogram.length; i++) {
            histogram[i] = 0;
        }
    }

}
