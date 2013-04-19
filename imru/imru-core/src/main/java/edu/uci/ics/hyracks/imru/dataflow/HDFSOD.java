/*
 * Copyright 2009-2010 by The Regents of the University of California
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

/**
 * 
 * @author Rui Wang
 */
public class HDFSOD extends IMRUOperatorDescriptor<Serializable> {
    private static final Logger LOG = Logger
            .getLogger(MapOperatorDescriptor.class.getName());

    private static final long serialVersionUID = 1L;

    protected final ConfigurationFactory confFactory;
    protected final IMRUFileSplit[] inputSplits;

    public HDFSOD(JobSpecification spec, IIMRUJob2<Serializable> imruSpec,
            IMRUFileSplit[] inputSplits, ConfigurationFactory confFactory) {
        super(spec, 1, 0, "parse", imruSpec);
        this.inputSplits = inputSplits;
        this.confFactory = confFactory;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(
            final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition,
            int nPartitions) throws HyracksDataException {
        return new AbstractUnaryInputSinkOperatorNodePushable() {
            @Override
            public void open() throws HyracksDataException {
            }

            @Override
            public void nextFrame(ByteBuffer buffer)
                    throws HyracksDataException {
                try {
                    TupleReader reader = new TupleReader(buffer,
                            ctx.getFrameSize(), 1);
                    while (reader.nextTuple()) {
//                        reader.dump();
                        int len = reader.getFieldLength(0);
                        reader.seekToField(0);
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