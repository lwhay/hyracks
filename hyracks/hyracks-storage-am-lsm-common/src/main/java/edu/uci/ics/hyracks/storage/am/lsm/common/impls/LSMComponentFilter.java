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
package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMComponentFilter;

public class LSMComponentFilter implements ILSMComponentFilter {

    private final IBinaryComparatorFactory[] filterCmpFactories;
    private final ITreeIndexTupleWriter tupleWriter;

    private ITupleReference minTuple;
    private ITupleReference maxTuple;

    private byte[] minTupleBytes;
    private ByteBuffer minTupleBuf;

    private byte[] maxTupleBytes;
    private ByteBuffer maxTupleBuf;

    public LSMComponentFilter(ITreeIndexTupleWriter tupleWriter, IBinaryComparatorFactory[] filterCmpFactories) {
        this.filterCmpFactories = filterCmpFactories;
        this.tupleWriter = tupleWriter;
        this.minTuple = tupleWriter.createTupleReference();
        this.maxTuple = tupleWriter.createTupleReference();
    }

    @Override
    public IBinaryComparatorFactory[] getFilterCmpFactories() {
        return filterCmpFactories;
    }

    @Override
    public void reset() {
        minTupleBytes = null;
        maxTupleBytes = null;
    }

    @Override
    public void update(ITupleReference tuple, MultiComparator cmp) {
        if (minTupleBytes == null) {
            int numBytes = tupleWriter.bytesRequired(tuple);
            minTupleBytes = new byte[numBytes];
            tupleWriter.writeTuple(tuple, minTupleBytes, 0);
            minTupleBuf = ByteBuffer.wrap(minTupleBytes);

            maxTupleBytes = new byte[numBytes];
            tupleWriter.writeTuple(tuple, maxTupleBytes, 0);
            maxTupleBuf = ByteBuffer.wrap(maxTupleBytes);
            ((ITreeIndexTupleReference) minTuple).resetByTupleOffset(minTupleBuf, 0);
            ((ITreeIndexTupleReference) maxTuple).resetByTupleOffset(maxTupleBuf, 0);
        } else {
            int c = cmp.compare(tuple, minTuple);
            if (c < 0) {
                int numBytes = tupleWriter.bytesRequired(tuple);
                if (minTupleBytes.length < numBytes) {
                    minTupleBytes = new byte[numBytes];
                    tupleWriter.writeTuple(tuple, minTupleBytes, 0);
                    minTupleBuf = ByteBuffer.wrap(minTupleBytes);
                } else {
                    tupleWriter.writeTuple(tuple, minTupleBytes, 0);
                }
                ((ITreeIndexTupleReference) minTuple).resetByTupleOffset(minTupleBuf, 0);
            }
            c = cmp.compare(tuple, maxTuple);
            if (c > 0) {
                int numBytes = tupleWriter.bytesRequired(tuple);
                if (maxTupleBytes.length < numBytes) {
                    maxTupleBytes = new byte[numBytes];
                    tupleWriter.writeTuple(tuple, maxTupleBytes, 0);
                    maxTupleBuf = ByteBuffer.wrap(maxTupleBytes);
                } else {
                    tupleWriter.writeTuple(tuple, maxTupleBytes, 0);
                }
                ((ITreeIndexTupleReference) maxTuple).resetByTupleOffset(maxTupleBuf, 0);
            }
        }
    }

    @Override
    public ITupleReference getMinTuple() {
        return minTuple;
    }

    @Override
    public ITupleReference getMaxTuple() {
        return maxTuple;
    }

}
