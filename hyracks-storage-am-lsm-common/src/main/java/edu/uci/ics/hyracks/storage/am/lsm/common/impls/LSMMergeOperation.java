package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.AbstractTreeIndex;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;

public class LSMMergeOperation implements ILSMIOOperation {

    private final ILSMIndex index;
    private final List<Object> mergingComponents;
    private final ITreeIndexCursor cursor;
    private final FileReference mergeTarget;
    private final ILSMIOOperationCallback callback;

    public LSMMergeOperation(ILSMIndex index, List<Object> mergingComponents, ITreeIndexCursor cursor,
            FileReference mergeTarget, ILSMIOOperationCallback callback) {
        this.index = index;
        this.mergingComponents = mergingComponents;
        this.cursor = cursor;
        this.mergeTarget = mergeTarget;
        this.callback = callback;
    }

    @Override
    public List<IODeviceHandle> getReadDevices() {
        List<IODeviceHandle> devs = new ArrayList<IODeviceHandle>();
        for (Object o : mergingComponents) {
            AbstractTreeIndex idx = (AbstractTreeIndex) o;
            devs.add(idx.getFileReference().getDevideHandle());
        }
        return devs;
    }

    @Override
    public List<IODeviceHandle> getWriteDevices() {
        return Collections.singletonList(mergeTarget.getDevideHandle());
    }

    @Override
    public void perform() throws HyracksDataException, IndexException {
        ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        accessor.merge(this);
    }

    @Override
    public ILSMIOOperationCallback getCallback() {
        return callback;
    }

    public FileReference getMergeTarget() {
        return mergeTarget;
    }

    public ITreeIndexCursor getCursor() {
        return cursor;
    }

    public List<Object> getMergingComponents() {
        return mergingComponents;
    }
}
