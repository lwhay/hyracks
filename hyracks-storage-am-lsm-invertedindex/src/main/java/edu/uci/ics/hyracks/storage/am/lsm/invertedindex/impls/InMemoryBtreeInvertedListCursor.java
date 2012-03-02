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

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListCursor;

public class InMemoryBtreeInvertedListCursor implements IInvertedListCursor {
    private final ITreeIndexCursor cursor;

    public InMemoryBtreeInvertedListCursor(ITreeIndexCursor cursor) {
        this.cursor = cursor;
    }

    @Override
    public int compareTo(IInvertedListCursor cursor) {
        // Refer to getNumElements() for the explanation of the magic number 1.
        return 1 - cursor.getNumElements();
    }

    @Override
    public void reset(int startPageId, int endPageId, int startOff, int numElements) {
        // Do nothing
    }

    @Override
    public void pinPagesSync() throws HyracksDataException {
        // Do nothing
    }

    @Override
    public void pinPagesAsync() throws HyracksDataException {
        // Do nothing
    }

    @Override
    public void unpinPages() throws HyracksDataException {
        // Do nothing
    }

    @Override
    public boolean hasNext() throws HyracksDataException {
        return cursor.hasNext();
    }

    @Override
    public void next() throws HyracksDataException {
        cursor.next();
    }

    @Override
    public ITupleReference getTuple() {
        ITupleReference tuple = cursor.getTuple();
        // TODO: Need to convert the tuple here!
        // Currently, the token would be returned along with the (docid, ...) fields.
        // Consider FixedSizeTupleReference?
        return cursor.getTuple();
    }

    @Override
    public int getNumElements() {
        // Currently there is no efficient way to obtain the number of elements 
        // since the tokens are sitting in BTree leaf pages. For now, return that 
        // we have only 1 element. The effect is that the TOccurrenceSearcher performance 
        // may be degraded during the merging phase since the lists will not be ordered 
        // properly (lists are ordered by count!).
        return 1;
    }

    @Override
    public int getStartPageId() {
        return 0;
    }

    @Override
    public int getEndPageId() {
        return 0;
    }

    @Override
    public int getStartOff() {
        return 0;
    }

    @Override
    public void positionCursor(int elementIx) {
    }

    @Override
    public boolean containsKey(ITupleReference searchTuple, MultiComparator invListCmp) {
        return false;
    }

    @Override
    public String printInvList(ISerializerDeserializer[] serdes) throws HyracksDataException {
        return null;
    }

    @Override
    public String printCurrentElement(ISerializerDeserializer[] serdes) throws HyracksDataException {
        return null;
    }

}
