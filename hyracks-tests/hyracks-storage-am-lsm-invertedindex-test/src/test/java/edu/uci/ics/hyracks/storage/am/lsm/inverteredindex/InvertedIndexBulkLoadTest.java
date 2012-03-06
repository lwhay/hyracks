package edu.uci.ics.hyracks.storage.am.lsm.inverteredindex;

import edu.uci.ics.hyracks.storage.am.lsm.invertedindex.utils.InvertedIndexTestUtils;

public class InvertedIndexBulkLoadTest extends AbstractInvertedIndexBulkloadTest {

    @Override
    protected void setTokenizer() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void setInvertedIndex() {
        invertedIndex = InvertedIndexTestUtils.createTestInvertedIndex();
    }

    @Override
    protected void setLogger() {
        // TODO Auto-generated method stub

    }

    @Override
    protected void setRandom() {
        // TODO Auto-generated method stub

    }

}
