package edu.uci.ics.hyracks.storage.am.lsm.common.impls;

import java.util.Collections;
import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.common.impls.NoOpOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperation;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIOOperationCallback;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndex;
import edu.uci.ics.hyracks.storage.am.lsm.common.api.ILSMIndexAccessor;

public class LSMFlushOperation implements ILSMIOOperation {

    private final ILSMIndex index;
    private final FileReference flushTarget;
    private final ILSMIOOperationCallback callback;

    public LSMFlushOperation(ILSMIndex index, FileReference flushTarget, ILSMIOOperationCallback callback) {
        this.index = index;
        this.flushTarget = flushTarget;
        this.callback = callback;
    }

    @Override
    public List<IODeviceHandle> getReadDevices() {
        return Collections.emptyList();
    }

    @Override
    public List<IODeviceHandle> getWriteDevices() {
        return Collections.singletonList(flushTarget.getDevideHandle());
    }

    @Override
    public void perform() throws HyracksDataException, IndexException {
        ILSMIndexAccessor accessor = (ILSMIndexAccessor) index.createAccessor(NoOpOperationCallback.INSTANCE,
                NoOpOperationCallback.INSTANCE);
        accessor.flush(this);
    }

    @Override
    public ILSMIOOperationCallback getCallback() {
        return callback;
    }

    public FileReference getFlushTarget() {
        return flushTarget;
    }

}
