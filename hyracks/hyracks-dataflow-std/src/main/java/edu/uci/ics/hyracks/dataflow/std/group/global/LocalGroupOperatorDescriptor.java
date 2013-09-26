/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.dataflow.std.group.global;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.IOperatorNodePushable;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IRecordDescriptorProvider;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.IOperatorDescriptorRegistry;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractUnaryInputUnaryOutputOperatorNodePushable;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class LocalGroupOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

    private static final long serialVersionUID = 1L;

    private final int framesLimit, levelSeed, tableSize;

    private final int[] keyFields, decorFields;

    private final IAggregatorDescriptorFactory aggregatorFactory, partialMergerFactory, finalMergerFactory;

    private final IBinaryComparatorFactory[] comparatorFactories;

    private final INormalizedKeyComputerFactory firstNormalizerFactory;

    private final IBinaryHashFunctionFamily[] hashFamilies;

    private final GroupAlgorithms algorithm;

    private final long inputRecordCount, outputGroupCount;
    private final int groupStateSizeInBytes;
    private final double fudgeFactor;

    public enum GroupAlgorithms {
        SORT_GROUP,
        SORT_GROUP_MERGE_GROUP,
        HASH_GROUP,
        HASH_GROUP_SORT_MERGE_GROUP,
        SIMPLE_HYBRID_HASH,
        RECURSIVE_HYBRID_HASH,
        PRECLUSTER
    }

    public LocalGroupOperatorDescriptor(IOperatorDescriptorRegistry spec, int[] keyFields, int[] decorFields,
            int framesLimit, int tableSize, long inputRecordCount, long outputGroupCount, int groupStateSizeInBytes,
            double fudgeFactor, IBinaryComparatorFactory[] comparatorFactories,
            IBinaryHashFunctionFamily[] hashFamilies, INormalizedKeyComputerFactory firstNormalizerFactory,
            IAggregatorDescriptorFactory aggregatorFactory, IAggregatorDescriptorFactory partialMergerFactory,
            IAggregatorDescriptorFactory finalMergerFactory, RecordDescriptor outRecDesc, GroupAlgorithms algorithm,
            int levelSeed) throws HyracksDataException {
        super(spec, 1, 1);
        this.framesLimit = framesLimit;
        this.tableSize = tableSize;
        if (framesLimit <= 3) {
            throw new HyracksDataException("Not enough memory assigned for " + this.displayName
                    + ": at least 3 frames are necessary but just " + framesLimit + " available.");
        }
        this.keyFields = keyFields;
        this.decorFields = decorFields;
        this.levelSeed = levelSeed;
        this.aggregatorFactory = aggregatorFactory;
        this.partialMergerFactory = partialMergerFactory;
        this.finalMergerFactory = finalMergerFactory;
        this.comparatorFactories = comparatorFactories;
        this.firstNormalizerFactory = firstNormalizerFactory;
        this.hashFamilies = hashFamilies;
        recordDescriptors[0] = outRecDesc;
        this.algorithm = algorithm;

        this.inputRecordCount = inputRecordCount;
        this.outputGroupCount = outputGroupCount;
        this.groupStateSizeInBytes = groupStateSizeInBytes;
        this.fudgeFactor = fudgeFactor;
    }

    @Override
    public IOperatorNodePushable createPushRuntime(final IHyracksTaskContext ctx,
            IRecordDescriptorProvider recordDescProvider, final int partition, int nPartitions)
            throws HyracksDataException {

        final IBinaryComparator[] comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < comparators.length; i++) {
            comparators[i] = comparatorFactories[i].createBinaryComparator();
        }

        final RecordDescriptor inRecDesc = recordDescProvider.getInputRecordDescriptor(getActivityId(), 0);

        final RecordDescriptor outRecDesc = recordDescriptors[0];

        final IBinaryHashFunctionFactory[] hashFunctionFactories = new IBinaryHashFunctionFactory[this.hashFamilies.length];
        for (int i = 0; i < hashFunctionFactories.length; i++) {
            hashFunctionFactories[i] = HashFunctionFamilyFactoryAdapter.getFunctionFactoryFromFunctionFamily(
                    this.hashFamilies[i], levelSeed);
        }

        return new AbstractUnaryInputUnaryOutputOperatorNodePushable() {

            private IFrameWriter grouper = null;

            private long inputFrameCount = 0;

            @Override
            public void open() throws HyracksDataException {
                switch (algorithm) {
                    case SORT_GROUP:
                        grouper = new SortGrouper(ctx, keyFields, decorFields, framesLimit, aggregatorFactory,
                                finalMergerFactory, inRecDesc, outRecDesc, firstNormalizerFactory, comparatorFactories,
                                writer, false);
                        break;
                    case HASH_GROUP:
                        grouper = new HashGrouper(ctx, keyFields, decorFields, framesLimit, aggregatorFactory,
                                finalMergerFactory, inRecDesc, outRecDesc, false, writer, false, tableSize,
                                comparatorFactories, hashFunctionFactories, firstNormalizerFactory, false);
                        break;
                    case HASH_GROUP_SORT_MERGE_GROUP:
                        grouper = new HashGroupSortMergeGrouper(ctx, keyFields, decorFields, framesLimit, tableSize,
                                firstNormalizerFactory, comparatorFactories, hashFunctionFactories, aggregatorFactory,
                                partialMergerFactory, finalMergerFactory, inRecDesc, outRecDesc, writer);
                        break;
                    case SIMPLE_HYBRID_HASH:
                        grouper = new HybridHashGrouper(ctx, keyFields, decorFields, framesLimit, aggregatorFactory,
                                finalMergerFactory, inRecDesc, outRecDesc, false, writer, false, tableSize,
                                comparatorFactories, hashFunctionFactories, 1, true);
                        break;
                    case RECURSIVE_HYBRID_HASH:
                        grouper = new RecursiveHybridHashGrouper(ctx, keyFields, decorFields, framesLimit, tableSize,
                                inputRecordCount, outputGroupCount, groupStateSizeInBytes, fudgeFactor,
                                firstNormalizerFactory, comparatorFactories, hashFamilies, aggregatorFactory,
                                partialMergerFactory, finalMergerFactory, inRecDesc, outRecDesc, 0, writer);
                        break;
                    case PRECLUSTER:
                        grouper = new PreCluster(ctx, keyFields, decorFields, framesLimit, aggregatorFactory,
                                finalMergerFactory, inRecDesc, outRecDesc, comparatorFactories, writer);
                        break;
                    case SORT_GROUP_MERGE_GROUP:
                    default:
                        grouper = new SortGroupMergeGrouper(ctx, keyFields, decorFields, framesLimit,
                                firstNormalizerFactory, comparatorFactories, aggregatorFactory, partialMergerFactory,
                                finalMergerFactory, inRecDesc, outRecDesc, writer);
                        break;

                }
                writer.open();
                grouper.open();
            }

            @Override
            public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                inputFrameCount++;
                grouper.nextFrame(buffer);
            }

            @Override
            public void fail() throws HyracksDataException {
                // TODO Auto-generated method stub

            }

            @Override
            public void close() throws HyracksDataException {
                switch (algorithm) {
                    case SORT_GROUP:
                    case HASH_GROUP:
                    case SIMPLE_HYBRID_HASH:
                    case PRECLUSTER:
                        ((AbstractHistogramPushBasedGrouper) grouper).wrapup();
                        break;
                    default:
                        break;
                }
                grouper.close();
                writer.close();
                ctx.getCounterContext()
                        .getCounter(
                                LocalGroupOperatorDescriptor.class.getName() + "." + partition + ".inputFrameCount",
                                true).update(inputFrameCount);
            }

        };
    }
}
