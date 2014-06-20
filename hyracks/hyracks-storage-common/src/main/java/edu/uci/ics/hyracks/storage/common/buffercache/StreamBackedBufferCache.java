package edu.uci.ics.hyracks.storage.common.buffercache;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.IFilePath;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IStreamIOManager;


public class StreamBackedBufferCache implements IBufferCache {

    IStreamIOManager ioManager;
    @Override
    public void createFile(IFilePath fileRef) throws HyracksDataException {

    }

    @Override
    public void openFile(int fileId) throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    public void closeFile(int fileId) throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    public void deleteFile(int fileId, boolean flushDirtyPages) throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    public ICachedPage tryPin(long dpid) throws HyracksDataException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public ICachedPage pin(long dpid, boolean newPage) throws HyracksDataException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void unpin(ICachedPage page) throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    public void flushDirtyPage(ICachedPage page) throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    public void force(int fileId, boolean metadata) throws HyracksDataException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getPageSize() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public int getNumPages() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void close() throws HyracksDataException {
        // TODO Auto-generated method stub

    }

}
