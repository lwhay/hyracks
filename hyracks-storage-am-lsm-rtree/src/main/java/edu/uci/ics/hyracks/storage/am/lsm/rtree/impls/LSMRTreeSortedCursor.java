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

package edu.uci.ics.hyracks.storage.am.lsm.rtree.impls;

import edu.uci.ics.hyracks.api.dataflow.value.ILinearizeComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ILinearizeComparatorFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;

public class LSMRTreeSortedCursor extends LSMRTreeAbstractCursor implements ITreeIndexCursor {

    private ILinearizeComparator linearizeCmp;
    private ITupleReference nextTuple;
    private boolean[] depletedRtreeCursors;
	
    public LSMRTreeSortedCursor(ILinearizeComparatorFactory linearizer) {
    	super();
		this.linearizeCmp = linearizer.createBinaryComparator();
		this.depletedRtreeCursors = new boolean[numberOfTrees];
	}

	@Override
    public void reset() throws HyracksDataException {
		depletedRtreeCursors = new boolean[numberOfTrees];
        foundNext = false;
        for(int i = 0; i < numberOfTrees; i++) {
        	rtreeCursors[i].reset();
        	try {
				diskRTreeAccessors[i].search(rtreeCursors[i], rtreeSearchPredicate);
			} catch (IndexException e) {
				throw new HyracksDataException(e);
			}
        	if(rtreeCursors[i].hasNext()) {
        		rtreeCursors[i].next();
        	} else {
        		depletedRtreeCursors[i] = true;
        	}
        }
    }

	@Override
    public boolean hasNext() throws HyracksDataException {
        if (foundNext) {
            return true;
        }
        
        while(!foundNext) {
        	int foundIn = -1;
	        for(int i = 0; i < numberOfTrees; i++) {
	        	if(depletedRtreeCursors[i]) continue;
	        	
	        	if(nextTuple == null) {
	        		nextTuple = rtreeCursors[i].getTuple();
	        		foundIn = i;
	        		continue;
	        	}

	        	if(linearizeCmp.compare(
	        			nextTuple.getFieldData(0),
	        			nextTuple.getFieldStart(0),
	        			nextTuple.getFieldLength(0) * linearizeCmp.getDimensions(),
	        			rtreeCursors[i].getTuple().getFieldData(0),
	        			rtreeCursors[i].getTuple().getFieldStart(0),
	        			rtreeCursors[i].getTuple().getFieldLength(0) * linearizeCmp.getDimensions())
        			<= 0) {
	        		nextTuple = rtreeCursors[i].getTuple();
	        		foundIn = i;
	        	}
	        }
	        if(foundIn == -1) return false;
	        if(rtreeCursors[foundIn].hasNext()) {
	        	rtreeCursors[foundIn].next();
	        } else {
	        	depletedRtreeCursors[foundIn] = true;
	        }
	        
	        for (int i = 0; i <= foundIn; i++) {
                try {
                    btreeCursors[i].reset();
                    btreeRangePredicate.setHighKey(nextTuple, true);
                    btreeRangePredicate.setLowKey(nextTuple, true);
                    diskBTreeAccessors[i].search(btreeCursors[i], btreeRangePredicate);
                } catch (IndexException e) {
                    throw new HyracksDataException(e);
                }
                try {
                    if (btreeCursors[i].hasNext()) {
                        break;
                    }
                } finally {
                    btreeCursors[i].close();
                }
            }
	        foundNext = true;
	        break;
        }
        
        return true;
    }

	@Override
    public void next() throws HyracksDataException {
    	frameTuple = nextTuple;
    	nextTuple = null;
        foundNext = false;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
    	super.open(initialState, searchPred);
        
        reset();
    }
}