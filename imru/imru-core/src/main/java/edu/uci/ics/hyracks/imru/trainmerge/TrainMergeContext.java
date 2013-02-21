package edu.uci.ics.hyracks.imru.trainmerge;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Hashtable;
import java.util.LinkedList;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.data.MergedFrames;
import edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUConnection;

public class TrainMergeContext extends IMRUContext {
    IFrameWriter writer;
    /**
     * current data partition
     */
    private int curTrainPartition;
    /**
     * unique integer to identify the partition among all paritions in all operators.
     */
    private int srcParitionUUID;
    private int curNodeId;
    IMRUConnection imruConnection;
    String jobId;

    public TrainMergeContext(IHyracksTaskContext ctx, String operatorName,
            IFrameWriter writer, int curTrainPartition, int srcParitionUUID,
            int curNodeId, IMRUConnection imruConnection, String jobId) {
        super(ctx, operatorName);
        this.writer = writer;
        this.curTrainPartition = curTrainPartition;
        this.curNodeId = curNodeId;
        this.imruConnection = imruConnection;
        this.jobId = jobId;
    }

    public int getCurPartition() {
        return curTrainPartition;
    }

    public void setJobStatus(String status) {
        try {
            imruConnection.setStatus(jobId, status);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean send(Serializable model, int partition) throws IOException {
        IFrameWriter writer = this.writer;
        if (writer == null) {
            if (getRuntimeContext().writers.size() == 0)
                return false;
            writer = getRuntimeContext().writers.get(0);
        }
        byte[] bs = JavaSerializationUtils.serialize(model);
        ByteBuffer frame = ctx.allocateFrame();
        int frameSize = ctx.getFrameSize();
        MergedFrames.serializeToFrames(frame, frameSize, writer, bs,
                curTrainPartition, partition, curNodeId);
        return true;
    }
}
