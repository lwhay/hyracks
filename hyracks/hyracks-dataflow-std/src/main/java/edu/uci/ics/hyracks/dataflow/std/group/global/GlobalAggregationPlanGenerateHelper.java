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

import java.io.File;
import java.io.IOException;
import java.util.BitSet;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.HashtableLocalityMap;
import edu.uci.ics.hyracks.dataflow.std.connectors.LocalityAwareMToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.HashFunctionFamilyFactoryAdapter;
import edu.uci.ics.hyracks.dataflow.std.group.global.LocalGroupOperatorDescriptor;

public class GlobalAggregationPlanGenerateHelper {

    public static JobSpecification createHyracksJobSpec(int framesLimit, int[] keyFields, int[] decorFields,
            long inputCount, long outputCount, int groupStateSize, double fudgeFactor, int tableSize, String[] nodes,
            String[] inputNodes, FileSplit[] inputSplits, RecordDescriptor inRecDesc, RecordDescriptor outRecDesc,
            ITupleParserFactory parserFactory, IAggregatorDescriptorFactory aggregateFactory,
            IAggregatorDescriptorFactory partialMergeFactory, IAggregatorDescriptorFactory finalMergeFactory,
            IBinaryComparatorFactory[] comparatorFactories, IBinaryHashFunctionFamily[] hashFamilies,
            INormalizedKeyComputerFactory firstNormalizerFactory,
            LocalGroupOperatorDescriptor.GroupAlgorithms localGrouperAlgo,
            LocalGroupOperatorDescriptor.GroupAlgorithms[] globalGrouperAlgos, String[] localPartition,
            String[][] globalPartitions, BitSet[] partitionMaps) throws IOException, HyracksDataException {

        JobSpecification spec = new JobSpecification();

        StringBuilder outputLabel = new StringBuilder();

        IFileSplitProvider inputSplitProvider = new ConstantFileSplitProvider(inputSplits);
        FileScanOperatorDescriptor inputScanner = new FileScanOperatorDescriptor(spec, inputSplitProvider,
                parserFactory, inRecDesc);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, inputScanner, inputNodes);

        LocalGroupOperatorDescriptor localGrouper = new LocalGroupOperatorDescriptor(spec, keyFields, decorFields,
                framesLimit, tableSize, inputCount, outputCount, groupStateSize, fudgeFactor, comparatorFactories,
                hashFamilies, firstNormalizerFactory, aggregateFactory, partialMergeFactory, finalMergeFactory,
                outRecDesc, localGrouperAlgo, 0);

        IConnectorDescriptor localConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(localConn, inputScanner, 0, localGrouper, 0);

        outputLabel.append('_').append(localGrouperAlgo.name());

        LocalGroupOperatorDescriptor prevGrouper = localGrouper;

        int[] storedKeyFields = new int[keyFields.length];
        int[] storedDecorFields = new int[decorFields.length];
        for (int i = 0; i < storedKeyFields.length; i++) {
            storedKeyFields[i] = i;
        }
        for (int i = storedKeyFields.length; i < storedKeyFields.length + storedDecorFields.length; i++) {
            storedDecorFields[i] = i;
        }

        for (int i = 0; i < globalGrouperAlgos.length; i++) {
            LocalGroupOperatorDescriptor grouper = new LocalGroupOperatorDescriptor(spec, storedKeyFields,
                    storedDecorFields, framesLimit, tableSize, inputCount, outputCount, groupStateSize, fudgeFactor,
                    comparatorFactories, hashFamilies, firstNormalizerFactory, partialMergeFactory,
                    partialMergeFactory, finalMergeFactory, outRecDesc, globalGrouperAlgos[i], i + 1);

            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, grouper, globalPartitions[i]);

            IConnectorDescriptor conn = new LocalityAwareMToNPartitioningConnectorDescriptor(spec,
                    new FieldHashPartitionComputerFactory(keyFields,
                            new IBinaryHashFunctionFactory[] { HashFunctionFamilyFactoryAdapter
                                    .getFunctionFactoryFromFunctionFamily(MurmurHash3BinaryHashFunctionFamily.INSTANCE,
                                            i) }), new HashtableLocalityMap(partitionMaps[i]));

            spec.connect(conn, prevGrouper, 0, grouper, 0);

            outputLabel.append('_').append(globalGrouperAlgos[i].name());

            prevGrouper = grouper;
        }

        AbstractSingleActivityOperatorDescriptor printer = getPrinter(spec,
                globalPartitions[globalPartitions.length - 1], "global" + outputLabel.toString());

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer,
                globalPartitions[globalPartitions.length - 1]);

        IConnectorDescriptor printConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(printConn, prevGrouper, 0, printer, 0);

        spec.addRoot(printer);

        return spec;
    }

    private static AbstractSingleActivityOperatorDescriptor getPrinter(JobSpecification spec, String[] outputNodeIDs,
            String prefix) throws IOException {

        FileSplit[] outputSplits = new FileSplit[outputNodeIDs.length];
        for (int i = 0; i < outputNodeIDs.length; i++) {
            outputSplits[i] = new FileSplit(outputNodeIDs[i], new FileReference(new File("/Volumes/Home/hyracks_tmp/"
                    + prefix + "_" + outputNodeIDs[i] + ".log")));
        }

        ResultSetId rsId = new ResultSetId(1);
        AbstractSingleActivityOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec,
                new ConstantFileSplitProvider(outputSplits), "|");
        spec.addResultSetId(rsId);

        return printer;
    }

}
