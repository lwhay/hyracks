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
package edu.uci.ics.hyracks.dataflow.std.benchmarking;

import java.nio.ByteBuffer;
import java.util.Random;

import edu.uci.ics.hyracks.api.context.IHyracksStageletContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorEnvironment;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class DataGeneratorOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final IGenDistributionDescriptor[] genDistributionDescriptors;

    private final long dataSize;

    private final boolean keyBased;

    @SuppressWarnings("rawtypes")
    private final ITypeGenerator[] dataGenerators;

    @SuppressWarnings("rawtypes")
    public DataGeneratorOperatorDescriptor(JobSpecification spec, ITypeGenerator[] dataGenerators,
            IGenDistributionDescriptor[] genDistributionDescriptors, long dataSize, boolean keyBased) {
        super(spec, 0, 1);
        this.genDistributionDescriptors = genDistributionDescriptors;
        this.dataSize = dataSize;
        this.dataGenerators = dataGenerators;
        this.keyBased = keyBased;

        final ISerializerDeserializer[] dataSeDers = new ISerializerDeserializer[dataGenerators.length];
        for (int i = 0; i < dataSeDers.length; i++) {
            dataSeDers[i] = dataGenerators[i].getSeDerInstance();
        }

        recordDescriptors[0] = new RecordDescriptor(dataSeDers);
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.api.dataflow.IActivityNode#createPushRuntime(edu.uci.ics.hyracks.api.context.IHyracksStageletContext, edu.uci.ics.hyracks.api.job.IOperatorEnvironment, edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider, int, int)
     */
    public IOperatorNodePushable createPushRuntime(IHyracksStageletContext ctx, IOperatorEnvironment env,
            IRecordDescriptorProvider recordDescProvider, int partition, int nPartitions) throws HyracksDataException {

        final ByteBuffer outFrame = ctx.allocateFrame();

        final FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());

        final ArrayTupleBuilder tb = new ArrayTupleBuilder(recordDescriptors[0].getFields().length);

        final Random rand = new Random();

        return new AbstractUnaryOutputSourceOperatorNodePushable() {

            @SuppressWarnings("unchecked")
            @Override
            public void initialize() throws HyracksDataException {
                writer.open();
                try {
                    appender.reset(outFrame, true);
                    for (int i = 0; i < dataSize; i++) {
                        tb.reset();
                        for (int j = 0; j < recordDescriptors[0].getFields().length; j++) {
                            if (keyBased)
                                tb.addField(recordDescriptors[0].getFields()[j], dataGenerators[j]
                                        .generate(genDistributionDescriptors[j].drawKey(rand.nextDouble())));
                            else
                                tb.addField(recordDescriptors[0].getFields()[j], dataGenerators[j].generate());
                        }

                        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                            FrameUtils.flushFrame(outFrame, writer);
                            appender.reset(outFrame, true);
                            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                throw new IllegalStateException();
                            }
                        }
                    }
                    FrameUtils.flushFrame(outFrame, writer);
                } finally {
                    writer.close();
                }
            }

        };
    }

}
