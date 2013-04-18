package edu.uci.ics.hyracks.imru.dataflow;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputSinkOperatorNodePushable;
import edu.uci.ics.hyracks.imru.api.IIMRUJob2;
import edu.uci.ics.hyracks.imru.api.TupleReader;
import edu.uci.ics.hyracks.imru.file.ConfigurationFactory;
import edu.uci.ics.hyracks.imru.file.IMRUFileSplit;
import edu.uci.ics.hyracks.imru.util.Rt;

public class HDFSOD extends IMRUOperatorDescriptor<Serializable> {
    private static final Logger LOG = Logger
            .getLogger(MapOperatorDescriptor.class.getName());

    private static final long serialVersionUID = 1L;

    protected final ConfigurationFactory confFactory;
    protected final IMRUFileSplit[] inputSplits;
    
    HDFSOD(JobSpecification spec,IIMRUJob2<Serializable> imruSpec, IMRUFileSplit[] inputSplits,
            ConfigurationFactory confFactory) {
        super(spec, 1, 0, "parse", imruSpec);
        this.inputSplits = inputSplits;
        this.confFactory = confFactory;
    }
        
    @Override
    public IOperatorNodePushable createPushRuntime(
            final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider,
            final int partition, int nPartitions)
            throws HyracksDataException {
        return new AbstractUnaryInputSinkOperatorNodePushable() {
            @Override
            public void open() throws HyracksDataException {
            }

            @Override
            public void nextFrame(ByteBuffer buffer)
                    throws HyracksDataException {
                try {
                    TupleReader reader = new TupleReader(buffer,
                            ctx.getFrameSize(), 2);
                    while (reader.nextTuple()) {
                        reader.seekToField(0);
                        int len = reader.getFieldLength(0);
                        byte[] bs = new byte[len];
                        reader.readFully(bs);
                        String word = new String(bs);
                        Rt.p(word);
                    }
                    reader.close();
                } catch (IOException ex) {
                    throw new HyracksDataException(ex);
                }
            }

            @Override
            public void fail() throws HyracksDataException {
            }

            @Override
            public void close() throws HyracksDataException {
            }
        };
    }
};