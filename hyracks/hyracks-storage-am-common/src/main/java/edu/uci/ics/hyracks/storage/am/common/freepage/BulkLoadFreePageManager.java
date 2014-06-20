package edu.uci.ics.hyracks.storage.am.common.freepage;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.storage.am.common.api.IFreePageManager;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrame;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexMetaDataFrameFactory;

public class BulkLoadFreePageManager implements IFreePageManager {

    @Override
    public void open(int fileId) {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() {
        // TODO Auto-generated method stub

    }

    @Override
    public int getFreePage(ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void addFreePage(ITreeIndexMetaDataFrame metaFrame, int freePage) throws HyracksDataException {
        throw new HyracksDataException("Unsupported Operation for this type of Page Manager");
    }

    @Override
    public int getMaxPage(ITreeIndexMetaDataFrame metaFrame) throws HyracksDataException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void init(ITreeIndexMetaDataFrame metaFrame, int currentMaxPage) throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    public ITreeIndexMetaDataFrameFactory getMetaDataFrameFactory() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte getMetaPageLevelIndicator() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public byte getFreePageLevelIndicator() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean isMetaPage(ITreeIndexMetaDataFrame metaFrame) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isFreePage(ITreeIndexMetaDataFrame metaFrame) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public int getFirstMetadataPage() {
        // TODO Auto-generated method stub
        return 0;
    }

}
