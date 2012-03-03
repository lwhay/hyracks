/*
 * Copyright 2009-2012 by The Regents of the University of California
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

package edu.uci.ics.hyracks.storage.am.lsm.invertedindex.impls;

import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexOpContext;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.IndexOp;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndex;

public class LSMInvertedIndexOpContext implements IIndexOpContext {
    
    private IIndexAccessor memoryInvertedIndexAccessor;

    public LSMInvertedIndexOpContext(IInvertedIndex memoryInvertedIndex) {
        memoryInvertedIndexAccessor = memoryInvertedIndex.createAccessor();
    }
    
    @Override
    public void reset() {
        // TODO Auto-generated method stub

    }

    @Override
    public void reset(IndexOp newOp) {
        // TODO Auto-generated method stub

    }
    
    public IIndexAccessor getAccessor() {
        return memoryInvertedIndexAccessor;
    }

}
