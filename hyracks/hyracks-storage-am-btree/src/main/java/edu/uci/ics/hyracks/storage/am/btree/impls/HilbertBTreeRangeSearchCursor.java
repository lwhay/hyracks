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

package edu.uci.ics.hyracks.storage.am.btree.impls;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.primitive.DoublePointable;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.storage.am.btree.api.IBTreeLeafFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ICursorInitialState;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.ILinearizerSearchHelper;
import edu.uci.ics.hyracks.storage.am.common.api.ILinearizerSearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ISearchPredicate;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;
import edu.uci.ics.hyracks.storage.common.buffercache.IBufferCache;
import edu.uci.ics.hyracks.storage.common.buffercache.ICachedPage;

public class HilbertBTreeRangeSearchCursor implements ITreeIndexCursor {

    private static final boolean DEBUG = false;

    private static final int DIMENSION = 2; //Only two dimensional data are supported.
    private static final int MAX_SEARCH_CANDIDATES = 2; //Max number of quadrants to be searched in a level to calculate the next match.  
    private static final int NUM_QUAD = 4; //Total number of quadrants
    private final IBTreeLeafFrame frame;
    private final boolean exclusiveLatchNodes;
    private MultiComparator hilbertCmp;
    private ICursorInitialState cursorInitialiState;
    private BTreeRangeSearchCursor cursor;
    private boolean hasNext;
    private HilbertRangeSearchContext currentSearchCtx;
    private HilbertRangeSearchContext backtrackSearchCtx;
    private HilbertRangeSearchContext tempSearchCtx;
    private boolean backtrackFlag;
    private int prevPageId;
    private boolean overlappedHKeys[] = new boolean[NUM_QUAD];
    private int overlappedCoordinates[] = new int[NUM_QUAD];
    private int nextSearchHKeys[] = new int[MAX_SEARCH_CANDIDATES];
    private int nextSearchCoordinates[] = new int[MAX_SEARCH_CANDIDATES];
    private int nextSearchCount;
    private double nextMatch[] = new double[DIMENSION];
    private ITupleReference tRefNextMatch;
    private ArrayTupleBuilder tBuilderNextMatch;
    private double searchedPoint[] = new double[DIMENSION];
    private double pageKey[] = new double[DIMENSION];
    private boolean needToCheckPageKey;
    private boolean firstOpen;
    private IIndexAccessor btreeAccessor;
    private double qBottomLeft[] = new double[DIMENSION];
    private double qTopRight[] = new double[DIMENSION];
    private ILinearizerSearchPredicate linearizerSearchPredicate;
    private ILinearizerSearchHelper linearizerSearchHelper;
    private double prevPoint[] = new double[DIMENSION];
    private ITupleReference tRefPrevPoint = new ArrayTupleReference();
    private ArrayTupleBuilder tBuilderPrevPoint = new ArrayTupleBuilder(1);

    private final HilbertState[] states = new HilbertState[] {
            new HilbertState(new int[] { 3, 0, 1, 0 }, new int[] { 0, 1, 3, 2 }),
            new HilbertState(new int[] { 1, 1, 0, 2 }, new int[] { 2, 1, 3, 0 }),
            new HilbertState(new int[] { 2, 3, 2, 1 }, new int[] { 2, 3, 1, 0 }),
            new HilbertState(new int[] { 0, 2, 3, 3 }, new int[] { 0, 3, 1, 2 }) };

    public HilbertBTreeRangeSearchCursor(IBTreeLeafFrame frame, boolean exclusiveLatchNodes) {
        this.frame = frame;
        this.exclusiveLatchNodes = exclusiveLatchNodes;
        this.currentSearchCtx = new HilbertRangeSearchContext();
        tBuilderNextMatch = new ArrayTupleBuilder(1);
        firstOpen = true;
    }

    @Override
    public void open(ICursorInitialState initialState, ISearchPredicate searchPred) throws HyracksDataException,
            IndexException {
        //TODO
        //make a better design to avoid this style of recursive call termination   
        if (!firstOpen) {
            this.cursorInitialiState = initialState;
            btreeAccessor = ((BTreeCursorInitialState) initialState).getAccessor();
            return;
        }

        this.cursorInitialiState = initialState;
        btreeAccessor = ((BTreeCursorInitialState) initialState).getAccessor();
        hilbertCmp = searchPred.getLowKeyComparator();
        tRefNextMatch = ((RangePredicate) searchPred).getLowKey();

        linearizerSearchPredicate = (ILinearizerSearchPredicate) searchPred;
        linearizerSearchHelper = linearizerSearchPredicate.getLinearizerSearchModifier();

        qBottomLeft[0] = linearizerSearchHelper.getQueryBottomLeftX();
        qBottomLeft[1] = linearizerSearchHelper.getQueryBottomLeftY();
        qTopRight[0] = linearizerSearchHelper.getQueryTopRightX();
        qTopRight[1] = linearizerSearchHelper.getQueryTopRightY();

        currentSearchCtx.init();
        currentSearchCtx.setQueryRegion(qBottomLeft[0], qBottomLeft[1], qTopRight[0], qTopRight[1]);

        cursor = calculateNextMatch(false);
        if (cursor != null) {
            prevPageId = cursor.getPageId();
        }
        hasNext = false;
        firstOpen = false;
    }

    @Override
    public void close() throws HyracksDataException {
        if (cursor != null) {
            cursor.close();
            cursor = null;
        }
    }

    @Override
    public boolean hasNext() throws HyracksDataException, IndexException {
        if (hasNext) {
            return true;
        }
        while (true) {
            if (cursor == null) {
                return false;
            }
            if (!cursor.hasNext()) {
                cursor.close();
                cursor = null;
                return false;
            }
            if (prevPageId == cursor.getPageId()) {
                cursor.next();
                if (isPointOnQueryRegion(cursor.getTuple())) {
                    hasNext = true;
                    return true;
                }

                //Consume all points which are located in the same coordinates.
                //This consumption is a critical operation in order to make the current pageKey set to the next distinct point.
                //If this consumption isn't executed, region query may terminate earlier without searching through all overlapped segments of the region correctly.   
                linearizerSearchHelper.convertPointField2TwoDoubles(cursor.getTuple().getFieldData(0), cursor
                        .getTuple().getFieldStart(0), prevPoint);
                linearizerSearchHelper.convertTwoDoubles2PointField(prevPoint, tBuilderPrevPoint);
                ((ArrayTupleReference) tRefPrevPoint).reset(tBuilderPrevPoint.getFieldEndOffsets(),
                        tBuilderPrevPoint.getByteArray());
                while (cursor.hasNext()) {
                    cursor.next();
                    if (hilbertCmp.compare(tRefPrevPoint, cursor.getTuple()) != 0) {
                        break;
                    }
                    linearizerSearchHelper.convertPointField2TwoDoubles(cursor.getTuple().getFieldData(0), cursor
                            .getTuple().getFieldStart(0), prevPoint);
                    linearizerSearchHelper.convertTwoDoubles2PointField(prevPoint, tBuilderPrevPoint);
                    ((ArrayTupleReference) tRefPrevPoint).reset(tBuilderPrevPoint.getFieldEndOffsets(),
                            tBuilderPrevPoint.getByteArray());
                }

                if (isPointOnQueryRegion(cursor.getTuple())) {
                    prevPageId = cursor.getPageId();
                    hasNext = true;
                    return true;
                }

                //Even though some points are not on the query region, check all points on the page.
                //Because it could be cheaper than triggering multiple underlying btree searches in the case,
                //where some points on the query region can be located in non-contiguous locations in the page. 

                //TODO compare the performance with or without reading all points in the page when a point is out of query region.
                /********************************************************************************************
                 * currentSearchCtx.init();
                 * currentSearchCtx.setQueryRegion(qBottomLeft[0], qBottomLeft[1], qTopRight[0], qTopRight[1]);
                 * setCurrentPageKey(tRefPrevPoint);
                 * cursor.close();
                 * cursor = null;
                 * cursor = calculateNextMatch(true);
                 * if (cursor != null) {
                 * prevPageId = cursor.getPageId();
                 * }
                 ********************************************************************************************/
            } else {
                cursor.next();
                if (isPointOnQueryRegion(cursor.getTuple())) {
                    //If the first point in the next leaf page is on the query region, 
                    //continue checking all points in the next page.
                    prevPageId = cursor.getPageId();
                    hasNext = true;
                    return true;
                }
                //The next leaf page is read and the first record of the next page is set as the current page key.
                //Then calculate the next match.
                currentSearchCtx.init();
                currentSearchCtx.setQueryRegion(qBottomLeft[0], qBottomLeft[1], qTopRight[0], qTopRight[1]);
                setCurrentPageKey(cursor.getTuple());
                cursor.close();
                cursor = null;
                cursor = calculateNextMatch(true);
                if (cursor != null) {
                    prevPageId = cursor.getPageId();
                }
            }
        }
    }

    @Override
    public ICachedPage getPage() {
        if (cursor != null) {
            return cursor.getPage();
        }
        return null;
    }

    @Override
    public void next() throws HyracksDataException {
        hasNext = false;
    }

    @Override
    public void reset() throws HyracksDataException {
        close();
    }

    @Override
    public void setBufferCache(IBufferCache bufferCache) {
        if (cursor != null) {
            cursor.setBufferCache(bufferCache);
        }
    }

    @Override
    public void setFileId(int fileId) {
        if (cursor != null) {
            cursor.setFileId(fileId);
        }
    }

    @Override
    public boolean exclusiveLatchNodes() {
        if (cursor != null) {
            return cursor.exclusiveLatchNodes();
        }
        return false;
    }

    @Override
    public void markCurrentTupleAsUpdated() throws HyracksDataException {
        if (cursor != null) {
            cursor.markCurrentTupleAsUpdated();
        }
    }

    @Override
    public ITupleReference getTuple() {
        if (cursor != null) {
            return cursor.getTuple();
        }
        return null;
    }

    public int getTupleOffset() {
        if (cursor != null) {
            return cursor.getTupleOffset();
        }
        return -1;
    }

    public int getPageId() {
        if (cursor != null) {
            return cursor.getPageId();
        }
        return -1;
    }

    private BTreeRangeSearchCursor calculateNextMatch(boolean search) throws HyracksDataException, IndexException {
        needToCheckPageKey = true;
        backtrackFlag = false;
        for (int i = 0; i < MAX_SEARCH_CANDIDATES; i++) {
            nextSearchHKeys[i] = -1;
            nextSearchCoordinates[i] = -1;
        }
        while (true) {
            nextSearchCount = 0;
            if (areCurrentQueryRegionAndCurrentSearchSpaceEqual()) {
                setNextMatch();
                cursor = new BTreeRangeSearchCursor(frame, exclusiveLatchNodes);
                if (search) {
                    btreeAccessor.search(this, linearizerSearchPredicate);
                }
                cursor.open(cursorInitialiState, linearizerSearchPredicate);
                return cursor;
            }
            //compute overlapping coordinates with current query region (BL, TL, BR and TR points)
            computeOverlappingCoordinates();
            if (nextSearchCount > 1) {
                createBacktrackContext();
            }
            if (nextSearchCount > 0) {
                updateCurrentSearchContext();
                continue;
            }
            if (backtrackFlag) {
                restoreBacktrackContext();
                continue;
            } else {
                return null;
            }
        }
    }

    private void setNextMatch() throws HyracksDataException {
        switch (currentSearchCtx.state) {
            case 0:
            case 3:
                nextMatch[0] = currentSearchCtx.centerPoint[0] - currentSearchCtx.stepSize * 2;
                nextMatch[1] = currentSearchCtx.centerPoint[1] - currentSearchCtx.stepSize * 2;
                break;

            case 1:
            case 2:
                nextMatch[0] = currentSearchCtx.centerPoint[0] + currentSearchCtx.stepSize * 2;
                nextMatch[1] = currentSearchCtx.centerPoint[1] + currentSearchCtx.stepSize * 2;
                break;

            default:
                throw new IllegalStateException("Illegal HilbertBTree search state: " + currentSearchCtx.state);
        }
        
        if (!currentSearchCtx.inclusive[0]) {
            nextMatch[0] = Math.nextAfter(nextMatch[0], currentSearchCtx.centerPoint[0]);
        }
        if (!currentSearchCtx.inclusive[1]) {
            nextMatch[1] = Math.nextAfter(nextMatch[1], currentSearchCtx.centerPoint[1]);
        }
        
        linearizerSearchHelper.convertTwoDoubles2PointField(nextMatch, tBuilderNextMatch);
        ((ArrayTupleReference) tRefNextMatch).reset(tBuilderNextMatch.getFieldEndOffsets(),
                tBuilderNextMatch.getByteArray());
    }

    private void restoreBacktrackContext() {
        //swap two contexts
        tempSearchCtx = currentSearchCtx;
        currentSearchCtx = backtrackSearchCtx;
        backtrackSearchCtx = tempSearchCtx;
        needToCheckPageKey = false;
        backtrackFlag = false;
    }

    private void updateCurrentSearchContext() {

        //check whether the current step size is less than machine epsilon
        if (currentSearchCtx.stepSize <= DoublePointable.getEpsilon()) {
            throw new IllegalStateException("Illegal HilbertBTree step size reached");
        }
        updateSearchCtx(currentSearchCtx, currentSearchCtx, nextSearchCoordinates[0]);

        if (DEBUG) {
            System.out.println("" + currentSearchCtx.qBottomLeft[0] + "\t" + currentSearchCtx.qBottomLeft[1] + "\t"
                    + currentSearchCtx.qTopRight[0] + "\t" + currentSearchCtx.qTopRight[1] + "\t"
                    + currentSearchCtx.centerPoint[0] + "\t" + currentSearchCtx.centerPoint[1] + "\t"
                    + currentSearchCtx.stepSize + "\t" + currentSearchCtx.state);
        }
    }

    private void createBacktrackContext() {
        //check whether the current step size is less than machine epsilon
        if (currentSearchCtx.stepSize <= DoublePointable.getEpsilon()) {
           throw new IllegalStateException("Illegal HilbertBTree step size reached");
        }

        if (backtrackSearchCtx == null) {
            backtrackSearchCtx = new HilbertRangeSearchContext();
        } else {
            backtrackSearchCtx.init();
        }
        backtrackSearchCtx.inclusive[0] = currentSearchCtx.inclusive[0];
        backtrackSearchCtx.inclusive[1] = currentSearchCtx.inclusive[1];
        backtrackSearchCtx.setQueryRegion(currentSearchCtx.qBottomLeft[0], currentSearchCtx.qBottomLeft[1],
                currentSearchCtx.qTopRight[0], currentSearchCtx.qTopRight[1]);
        
        updateSearchCtx(currentSearchCtx, backtrackSearchCtx, nextSearchCoordinates[1]);
        backtrackFlag = true;
    }
    
    private void updateSearchCtx (final HilbertRangeSearchContext currentCtx, HilbertRangeSearchContext nextCtx, int nextCoordinate) {
        switch (nextCoordinate) {
            case 0:
                if (currentCtx.qTopRight[0] > currentCtx.centerPoint[0]) {
                    nextCtx.qTopRight[0] = currentCtx.centerPoint[0];
                    nextCtx.qBottomRight[0] = currentCtx.centerPoint[0];
                }
                if (currentCtx.qTopRight[1] > currentCtx.centerPoint[1]) {
                    nextCtx.qTopRight[1] = currentCtx.centerPoint[1];
                    nextCtx.qTopLeft[1] = currentCtx.centerPoint[1];
                }
                nextCtx.centerPoint[0] = currentCtx.centerPoint[0] - currentCtx.stepSize;
                nextCtx.centerPoint[1] = currentCtx.centerPoint[1] - currentCtx.stepSize;
                nextCtx.inclusive[0] = false;
                nextCtx.inclusive[1] = false;
                break;

            case 1:
                if (currentCtx.qTopRight[0] > currentCtx.centerPoint[0]) {
                    nextCtx.qTopRight[0] = currentCtx.centerPoint[0];
                    nextCtx.qBottomRight[0] = currentCtx.centerPoint[0];
                }
                if (currentCtx.qBottomLeft[1] < currentCtx.centerPoint[1]) {
                    nextCtx.qBottomLeft[1] = currentCtx.centerPoint[1];
                    nextCtx.qBottomRight[1] = currentCtx.centerPoint[1];
                }
                nextCtx.centerPoint[0] = currentCtx.centerPoint[0] - currentCtx.stepSize;
                nextCtx.centerPoint[1] = currentCtx.centerPoint[1] + currentCtx.stepSize;
                nextCtx.inclusive[0] = false;
                break;

            case 2:
                if (currentCtx.qBottomLeft[0] < currentCtx.centerPoint[0]) {
                    nextCtx.qBottomLeft[0] = currentCtx.centerPoint[0];
                    nextCtx.qTopLeft[0] = currentCtx.centerPoint[0];
                }
                if (currentCtx.qTopRight[1] > currentCtx.centerPoint[1]) {
                    nextCtx.qTopRight[1] = currentCtx.centerPoint[1];
                    nextCtx.qTopLeft[1] = currentCtx.centerPoint[1];
                }
                nextCtx.centerPoint[0] = currentCtx.centerPoint[0] + currentCtx.stepSize;
                nextCtx.centerPoint[1] = currentCtx.centerPoint[1] - currentCtx.stepSize;
                nextCtx.inclusive[1] = false;
                break;

            case 3:
                if (currentCtx.qBottomLeft[0] < currentCtx.centerPoint[0]) {
                    nextCtx.qBottomLeft[0] = currentCtx.centerPoint[0];
                    nextCtx.qTopLeft[0] = currentCtx.centerPoint[0];
                }
                if (currentCtx.qBottomLeft[1] < currentCtx.centerPoint[1]) {
                    nextCtx.qBottomLeft[1] = currentCtx.centerPoint[1];
                    nextCtx.qBottomRight[1] = currentCtx.centerPoint[1];
                }
                nextCtx.centerPoint[0] = currentCtx.centerPoint[0] + currentCtx.stepSize;
                nextCtx.centerPoint[1] = currentCtx.centerPoint[1] + currentCtx.stepSize;
                break;

            default:
                throw new IllegalStateException("Illegal HilbertBtree coordinate: " + nextCoordinate);
        }
        nextCtx.state = states[currentCtx.state].nextState[nextCoordinate];
        nextCtx.stepSize = currentCtx.stepSize / 2;
    }

    private void computeOverlappingCoordinates() {
        int i;
        int coordinates[] = new int[NUM_QUAD];
        int pageKeyCoordinates = 0;
        int pageKeyHKey = 0;
        
        for (i = 0; i < NUM_QUAD; i++) {
            overlappedHKeys[i] = false;
        }

        // coordinates
        //                |
        //                |
        //       01       |       11
        //                |
        //                |
        // -------------- c -------s-------
        //                |
        //                |
        //       00       |       10
        //                |
        //                |
        // c: center point
        // s: step size

        //determine overlapped coordinates with the query region
        for (i = DIMENSION - 1; i >= 0; i--) {
            if (currentSearchCtx.qBottomLeft[i] >= currentSearchCtx.centerPoint[i])
                coordinates[0] ^= (1 << (DIMENSION - i - 1));
            if (currentSearchCtx.qTopLeft[i] >= currentSearchCtx.centerPoint[i])
                coordinates[1] ^= (1 << (DIMENSION - i - 1));
            if (currentSearchCtx.qBottomRight[i] >= currentSearchCtx.centerPoint[i])
                coordinates[2] ^= (1 << (DIMENSION - i - 1));
            if (currentSearchCtx.qTopRight[i] >= currentSearchCtx.centerPoint[i])
                coordinates[3] ^= (1 << (DIMENSION - i - 1));
            if (needToCheckPageKey) {
                if (currentSearchCtx.pageKey[i] >= currentSearchCtx.centerPoint[i])
                    pageKeyCoordinates ^= (1 << (DIMENSION - i - 1));
            }
        }

        for (i = 0; i < NUM_QUAD; i++) {
            overlappedHKeys[states[currentSearchCtx.state].key[coordinates[i]]] = true;
            overlappedCoordinates[states[currentSearchCtx.state].key[coordinates[i]]] = coordinates[i];
        }
        
        if (needToCheckPageKey) {
            pageKeyHKey = states[currentSearchCtx.state].key[pageKeyCoordinates];
        }

        //determine candidate quadrants to be searched considering the current page key 
        for (i = 0, nextSearchCount = 0; i < NUM_QUAD; i++) {
            if (overlappedHKeys[i]) {
                //If the first overlapped coordinates's HKey (represented in variable i) is greater than current page key's HKey,
                //the page key should not be considered after this step.
                if (needToCheckPageKey && pageKeyHKey < i && nextSearchCount == 0) {
                    needToCheckPageKey = false;
                }
                //If the overlapped coordinates's HKey is less than pageKey's HKey,
                //the overlapped coordinates are not included in the next search candidates.
                if (needToCheckPageKey && pageKeyHKey > i) {
                    continue;
                }
                nextSearchHKeys[nextSearchCount] = i;
                nextSearchCoordinates[nextSearchCount++] = overlappedCoordinates[i];
                if (nextSearchCount == MAX_SEARCH_CANDIDATES) {
                    break;
                }
            }
        }
        
        if (DEBUG) {
            System.out.println("center[ " + currentSearchCtx.centerPoint[0] + ", " + currentSearchCtx.centerPoint[1]
                    + " ]: " + nextSearchCount + " [ (key, quad): (" + nextSearchHKeys[0] + ", "
                    + nextSearchCoordinates[0] + "), (" + nextSearchHKeys[1] + ", " + nextSearchCoordinates[1]
                    + ") ], stepsize: " + currentSearchCtx.stepSize + "pageKey: " + currentSearchCtx.pageKey[0] + ", "
                    + currentSearchCtx.pageKey[1] + ", region: " + currentSearchCtx.qBottomLeft[0] + ", "
                    + currentSearchCtx.qBottomLeft[1] + ", " + currentSearchCtx.qTopRight[0] + ", "
                    + currentSearchCtx.qTopRight[1]);
        }
    }

    private boolean areCurrentQueryRegionAndCurrentSearchSpaceEqual() {
        if (Math.abs((currentSearchCtx.centerPoint[0] + currentSearchCtx.stepSize) - currentSearchCtx.centerPoint[0]) <= DoublePointable
                .getEpsilon() * 2
                || Math.abs((currentSearchCtx.centerPoint[1] + currentSearchCtx.stepSize)
                        - currentSearchCtx.centerPoint[1]) <= DoublePointable.getEpsilon() * 2
                || (Math.abs((currentSearchCtx.centerPoint[0] - currentSearchCtx.stepSize * 2)
                        - currentSearchCtx.qBottomLeft[0]) <= DoublePointable.getEpsilon()
                        && Math.abs((currentSearchCtx.centerPoint[1] - currentSearchCtx.stepSize * 2)
                                - currentSearchCtx.qBottomLeft[1]) <= DoublePointable.getEpsilon()
                        && Math.abs((currentSearchCtx.centerPoint[0] + currentSearchCtx.stepSize * 2)
                                - currentSearchCtx.qTopRight[0]) <= DoublePointable.getEpsilon() && Math
                        .abs((currentSearchCtx.centerPoint[1] + currentSearchCtx.stepSize * 2)
                                - currentSearchCtx.qTopRight[1]) <= DoublePointable.getEpsilon())) {
            return true;
        }
        return false;
    }

    private boolean isPointOnQueryRegion(ITupleReference tuple) throws HyracksDataException {
        // check whether qBottomLeftX <= x <=qTopRightX && similar for y.
        linearizerSearchHelper.convertPointField2TwoDoubles(tuple.getFieldData(0), tuple.getFieldStart(0),
                searchedPoint);
        
        if (DEBUG) {
            if (qBottomLeft[0] <= searchedPoint[0] && qTopRight[0] >= searchedPoint[0]
                    && qBottomLeft[1] <= searchedPoint[1] && qTopRight[1] >= searchedPoint[1]) {
                System.out.println("yes: \t" + searchedPoint[0] + ", " + searchedPoint[1]);
            } else {
                System.out.println("no : \t" + searchedPoint[0] + ", " + searchedPoint[1]);
            }
        }
        
        return qBottomLeft[0] <= searchedPoint[0] && qTopRight[0] >= searchedPoint[0]
                && qBottomLeft[1] <= searchedPoint[1] && qTopRight[1] >= searchedPoint[1];
    }

    private void setCurrentPageKey(ITupleReference tuple) throws HyracksDataException {
        linearizerSearchHelper.convertPointField2TwoDoubles(tuple.getFieldData(0), tuple.getFieldStart(0), pageKey);
        currentSearchCtx.setPageKey(pageKey[0], pageKey[1]);
    }

    private class HilbertRangeSearchContext {
        public double qBottomLeft[] = new double[2]; //queryRegionBottomLeft
        public double qTopRight[] = new double[2]; //queryRegionTopRight
        public double qBottomRight[] = new double[2];//queryRegionBottomRight
        public double qTopLeft[] = new double[2]; //queryRegionTopLeft

        public double pageKey[] = new double[2];
        public double centerPoint[] = new double[2];
        public double stepSize;
        public int state;
        public boolean inclusive[] = new boolean[2]; //value-on-border-line inclusiveness

        public void init() {
            setPageKey(-Double.MAX_VALUE, -Double.MAX_VALUE);
            centerPoint[0] = 0.0;
            centerPoint[1] = 0.0;
            stepSize = Double.MAX_VALUE / 2;
            state = 0;
            inclusive[0] = true;
            inclusive[1] = true;
        }

        public void setQueryRegion(double qBottomLeftX, double qBottomLeftY, double qTopRightX, double qTopRightY) {
            this.qBottomLeft[0] = qBottomLeftX;
            this.qBottomLeft[1] = qBottomLeftY;
            this.qTopRight[0] = qTopRightX;
            this.qTopRight[1] = qTopRightY;
            this.qBottomRight[0] = qTopRightX;
            this.qBottomRight[1] = qBottomLeftY;
            this.qTopLeft[0] = qBottomLeftX;
            this.qTopLeft[1] = qTopRightY;
        }

        public void setPageKey(double x, double y) {
            pageKey[0] = x;
            pageKey[1] = y;
        }
    }

    private class HilbertState {
        public final int[] nextState;
        public final int[] key;

        public HilbertState(int[] nextState, int[] key) {
            this.nextState = nextState;
            this.key = key;
        }
    }
}