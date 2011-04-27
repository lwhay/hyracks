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

import java.net.InetAddress;
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
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryOutputSourceOperatorNodePushable;

public class DataGeneratorOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final IGenDistributionDescriptor[] genDistributionDescriptors;

    private final long dataSize;

    private final boolean keyBased;

    private final int randSeed;

    @SuppressWarnings("rawtypes")
    private final ITypeGenerator[] dataGenerators;

    private final boolean repeatable;

    /**
     * Randomly Generate data based on the given data type. The generator initialized by this
     * constructor will create different data sets for different runs.
     * 
     * @param spec
     * @param dataSeDers
     * @param genDistributionDescriptors
     * @param dataSize
     * @param keyBased
     */
    @SuppressWarnings("rawtypes")
    public DataGeneratorOperatorDescriptor(JobSpecification spec, ISerializerDeserializer[] dataSeDers,
            IGenDistributionDescriptor[] genDistributionDescriptors, long dataSize, boolean keyBased) {
        super(spec, 0, 1);
        this.genDistributionDescriptors = genDistributionDescriptors;
        this.dataSize = dataSize;
        this.randSeed = (int) (System.currentTimeMillis());
        Random rand = new Random(randSeed);
        int[] randSeeds = new int[dataSeDers.length];
        for (int i = 0; i < dataSeDers.length; i++) {
            randSeeds[i] = rand.nextInt();
        }
        this.dataGenerators = getDataGenerator(dataSeDers, randSeeds);
        this.keyBased = keyBased;

        recordDescriptors[0] = new RecordDescriptor(dataSeDers);

        this.repeatable = false;
    }

    /**
     * Randomly generate data based on the given data type and also the random generator seed. Different runs
     * with the same random key will create the same data set.
     * 
     * @param spec
     * @param dataSeDers
     * @param randSeeds
     * @param genDistributionDescriptors
     * @param dataSize
     * @param keyBased
     * @param randSeed
     */
    @SuppressWarnings("rawtypes")
    public DataGeneratorOperatorDescriptor(JobSpecification spec, ISerializerDeserializer[] dataSeDers,
            int[] randSeeds, IGenDistributionDescriptor[] genDistributionDescriptors, long dataSize, boolean keyBased,
            int randSeed, boolean repeatable) {
        super(spec, 0, 1);
        this.genDistributionDescriptors = genDistributionDescriptors;
        this.dataSize = dataSize;
        this.keyBased = keyBased;

        this.randSeed = randSeed;

        this.dataGenerators = getDataGenerator(dataSeDers, randSeeds);

        recordDescriptors[0] = new RecordDescriptor(dataSeDers);

        this.repeatable = repeatable;
    }

    /**
     * Randomly generate data using the given data generators, and different runs of this generator will
     * generate different data sets.
     * This constructor enables more control on the generators.
     * 
     * @param spec
     * @param dataGenerators
     * @param genDistributionDescriptors
     * @param dataSize
     * @param keyBased
     */
    @SuppressWarnings("rawtypes")
    public DataGeneratorOperatorDescriptor(JobSpecification spec, ITypeGenerator[] dataGenerators,
            IGenDistributionDescriptor[] genDistributionDescriptors, long dataSize, boolean keyBased) {
        super(spec, 0, 1);
        this.genDistributionDescriptors = genDistributionDescriptors;
        this.dataSize = dataSize;
        this.randSeed = (int) (System.currentTimeMillis());

        this.dataGenerators = dataGenerators;
        this.keyBased = keyBased;

        this.repeatable = false;

        ISerializerDeserializer[] dataSeDers = new ISerializerDeserializer[dataGenerators.length];
        for (int i = 0; i < dataGenerators.length; i++) {
            dataSeDers[i] = dataGenerators[i].getSeDerInstance();
        }

        recordDescriptors[0] = new RecordDescriptor(dataSeDers);
    }

    /**
     * Randomly generate data using the given data generators and random seed. Different runs using the
     * same random seed will produce the same data sets.
     * This constructor enables more control on the generators.
     * 
     * @param spec
     * @param dataGenerators
     * @param genDistributionDescriptors
     * @param dataSize
     * @param keyBased
     * @param randSeed
     */
    @SuppressWarnings("rawtypes")
    public DataGeneratorOperatorDescriptor(JobSpecification spec, ITypeGenerator[] dataGenerators,
            IGenDistributionDescriptor[] genDistributionDescriptors, long dataSize, boolean keyBased, int randSeed,
            boolean repeatable) {
        super(spec, 0, 1);
        this.genDistributionDescriptors = genDistributionDescriptors;
        this.dataSize = dataSize;
        this.keyBased = keyBased;

        this.randSeed = randSeed;

        this.dataGenerators = dataGenerators;

        this.repeatable = repeatable;

        ISerializerDeserializer[] dataSeDers = new ISerializerDeserializer[dataGenerators.length];
        for (int i = 0; i < dataGenerators.length; i++) {
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
        try {
            if (repeatable) {
                // Use the host address as the prefix of the random seed, so
                // that multiple runs of generator will produce the same
                // dataset.
                rand.setSeed(randSeed + InetAddress.getLocalHost().hashCode());
            } else {
                rand.setSeed(randSeed + ctx.hashCode());
            }
        } catch (Exception e) {
            throw new HyracksDataException("Failed to get local host information for data generator.");
        }

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

    @SuppressWarnings("rawtypes")
    private static ITypeGenerator[] getDataGenerator(ISerializerDeserializer[] seDers, int[] randSeeds) {
        ITypeGenerator[] rtn = new ITypeGenerator[seDers.length];
        for (int i = 0; i < seDers.length; i++) {
            if (seDers[i] instanceof IntegerSerializerDeserializer) {
                rtn[i] = new IntegerGenerator(randSeeds[i]);
            } else if (seDers[i] instanceof UTF8StringSerializerDeserializer) {
                rtn[i] = new UTF8StringGenerator(randSeeds[i]);
            } else {
                rtn[i] = new IntegerGenerator(randSeeds[i]);
            }
        }
        return rtn;
    }

}
