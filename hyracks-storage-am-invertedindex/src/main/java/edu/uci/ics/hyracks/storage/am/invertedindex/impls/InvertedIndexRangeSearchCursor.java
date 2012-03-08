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
package edu.uci.ics.hyracks.storage.am.invertedindex.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTree;
import edu.uci.ics.hyracks.storage.am.btree.impls.BTreeRangeSearchCursor;
import edu.uci.ics.hyracks.storage.am.btree.impls.RangePredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrame;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedListCursor;

public class InvertedIndexRangeSearchCursor implements IIndexCursor {

    private final BTree btree;
    private final ITreeIndexAccessor btreeAccessor;
    private final IInvertedIndex invIndex;
    private IInvertedListCursor invListCursor;

    // for btree cursor creation
    private RangePredicate btreePred;
    private final ITreeIndexFrame leafFrame;
    private final ITreeIndexCursor btreeCursor;
    private boolean flagEOF;

    private ITupleReference tokenTuple;
    private ITupleReference invListTuple;
    /*
     * compositeTuple : this tuple consists of token(the first field of
     * tokenTuple) and doc-id(the first field of invListTuple).
     */
    private CompositeTupleReference compositeTuple;

    public InvertedIndexRangeSearchCursor(IInvertedIndex invIndex) {
        this.btree = ((InvertedIndex) invIndex).getBTree();
        this.btreeAccessor = btree.createAccessor();
        this.invIndex = invIndex;

        // setup for btree cursor creation
        leafFrame = btree.getLeafFrameFactory().createFrame();
        btreeCursor = new BTreeRangeSearchCursor((IBTreeLeafFrame) leafFrame, false);
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException {
        flagEOF = false;
        this.btreePred = (RangePredicate) searchPred;
        // get token from btree
        try {
            btreeAccessor.search(btreeCursor, btreePred);
        } catch (IndexException e) {
            throw new HyracksDataException(e);
        }

        if (btreeCursor.hasNext()) {
            btreeCursor.next();
            tokenTuple = btreeCursor.getTuple();
            try {
                // create and open invertedListCursor
                invListCursor = invIndex.createInvertedListCursor();
                invIndex.openInvertedListCursor(invListCursor, (IFrameTupleReference) tokenTuple);
                invListCursor.pinPagesSync(); // unpinned on cursor changeover or close()
                // pinPage - required?
                //                invListCursor.pinPagesSync();
                //                if (invListCursor.hasNext()) {

                // invListCursor.next();
                // invListTuple = invListCursor.getTuple();
                //
                // // create a result tuple which consists of each first
                // field
                // // of the tokenTuple and the invListTuple
                // compositeTuple.reset(tokenTuple, 0, invListTuple, 0);
                //                }
                // This case seems erroneous and should never happen since if
                // there is a token, there must exist at least a docId.
                // else {
                // flagEOF = true;
                // }
                // unPinPage - required?
                //                invListCursor.unpinPages();
            } catch (IndexException e) {
                throw new HyracksDataException(e);
            }
        } else {
            flagEOF = true;
        }
    }

    @Override
    public boolean hasNext() throws HyracksDataException {
        if (flagEOF) {
            return false;
        }
        // check each cursor.
        if (!invListCursor.hasNext() && !btreeCursor.hasNext()) {
            flagEOF = true;
            return false;
        }

        return true;
    }

    @Override
    public void next() throws HyracksDataException {
        if (flagEOF) {
            return;
        }

        if (invListCursor.hasNext()) {
            invListCursor.next();
            // create a result tuple which consists of each first field
            // of the tokenTuple and the invListTuple
            invListTuple = invListCursor.getTuple();
            compositeTuple.reset(tokenTuple, 0, invListTuple, 0);
        } else {
            if (btreeCursor.hasNext()) {
                // read the next token from btreeCursor
                btreeCursor.next();
                tokenTuple = btreeCursor.getTuple();
                try {
                    invListCursor.unpinPages();
                    invIndex.openInvertedListCursor(invListCursor, (IFrameTupleReference) tokenTuple);
                    invListCursor.pinPagesSync(); // unpinned on cursor changeover or close()
                    invListCursor.hasNext(); // required?
                    invListCursor.next();
                    invListTuple = invListCursor.getTuple();
                    compositeTuple.reset(tokenTuple, 0, invListTuple, 0);
                } catch (IndexException e) {
                    throw new HyracksDataException(e);
                }
            } else {
                //no more token
                flagEOF = true;
            }
        }
    }

    @Override
    public void close() throws HyracksDataException {
        invListCursor.unpinPages();
        btreeCursor.close();
    }

    @Override
    public void reset() throws HyracksDataException {
        invListCursor.unpinPages();
        open(null, btreePred);
    }

    @Override
    public ITupleReference getTuple() throws HyracksDataException {
        return compositeTuple;
    }

}
