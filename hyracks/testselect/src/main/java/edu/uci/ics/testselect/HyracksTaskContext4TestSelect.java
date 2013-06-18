package edu.uci.ics.testselect;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;

import edu.uci.ics.hyracks.api.context.IHyracksJobletContext;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.TaskAttemptId;
import edu.uci.ics.hyracks.api.dataflow.state.IStateObject;
import edu.uci.ics.hyracks.api.dataset.IDatasetPartitionManager;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.io.IIOManager;
import edu.uci.ics.hyracks.api.io.IODeviceHandle;
import edu.uci.ics.hyracks.api.job.profiling.counters.ICounterContext;
import edu.uci.ics.hyracks.api.resources.IDeallocatable;
import edu.uci.ics.hyracks.control.nc.io.IOManager;

public class HyracksTaskContext4TestSelect implements IHyracksTaskContext {

	static final int frameSize = 32768;
	private IOManager ioManager;
	public HyracksTaskContext4TestSelect() {
	       List<IODeviceHandle> devices = new ArrayList<IODeviceHandle>();
	        devices.add(new IODeviceHandle(new File(System.getProperty("java.io.tmpdir")), "."));
	        try {
				ioManager = new IOManager(devices, Executors.newCachedThreadPool());
			} catch (HyracksException e) {e.printStackTrace();}
	}
	@Override
	public int getFrameSize() {
		return frameSize;
	}

	@Override
	public IIOManager getIOManager() {
		return ioManager;
	}

	@Override
	public ByteBuffer allocateFrame() {
		return ByteBuffer.allocate(frameSize);
	}

	@Override
	public FileReference createUnmanagedWorkspaceFile(String prefix)
			throws HyracksDataException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FileReference createManagedWorkspaceFile(String prefix)
			throws HyracksDataException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void registerDeallocatable(final IDeallocatable deallocatable) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                deallocatable.deallocate();
            }
        });
	}

	@Override
	public void setStateObject(IStateObject taskState) {
		// TODO Auto-generated method stub

	}

	@Override
	public IStateObject getStateObject(Object id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IHyracksJobletContext getJobletContext() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TaskAttemptId getTaskAttemptId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ICounterContext getCounterContext() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IDatasetPartitionManager getDatasetPartitionManager() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void sendApplicationMessageToCC(byte[] message,
			DeploymentId deploymendId, String nodeId) throws Exception {
		// TODO Auto-generated method stub

	}

}
