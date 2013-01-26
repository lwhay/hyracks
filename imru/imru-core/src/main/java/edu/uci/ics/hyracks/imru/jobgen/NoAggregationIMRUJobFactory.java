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

package edu.uci.ics.hyracks.imru.jobgen;

import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.mapreduce.InputSplit;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNReplicatingConnectorDescriptor;
import edu.uci.ics.hyracks.imru.api.IIMRUJobSpecification;
import edu.uci.ics.hyracks.imru.dataflow.MapOperatorDescriptor;
import edu.uci.ics.hyracks.imru.dataflow.UpdateOperatorDescriptor;
import edu.uci.ics.hyracks.imru.file.HDFSInputSplitProvider;
import edu.uci.ics.hyracks.imru.hadoop.config.ConfigurationFactory;
import edu.uci.ics.hyracks.imru.jobgen.clusterconfig.ClusterConfig;

/**
 * Generates JobSpecifications for iterations of iterative map reduce update,
 * without any type of aggregation tree.
 */
public class NoAggregationIMRUJobFactory extends AbstractIMRUJobFactory {

    /**
     * Construct a new NoAggregationIMRUJobFactory.
     * 
     * @param inputPaths
     *            A comma-separated list of paths specifying the input files.
     * @param confFactory
     *            A Hadoop configuration used for HDFS settings.
     */
    public NoAggregationIMRUJobFactory(String inputPaths, ConfigurationFactory confFactory) {
        super(inputPaths, confFactory);
    }

    @SuppressWarnings( { "rawtypes", "unchecked" })
    @Override
    public JobSpecification generateJob(IIMRUJobSpecification model, UUID id, int roundNum, String envInPath,
            String envOutPath) throws HyracksException {
        JobSpecification spec = new JobSpecification();
        // Create operators
        // File reading and writing
        HDFSInputSplitProvider inputSplitProvider = confFactory == null ? null : new HDFSInputSplitProvider(inputPaths,
                confFactory.createConfiguration());
        List<InputSplit> inputSplits = inputSplitProvider == null ? null : inputSplitProvider.getInputSplits();

        // IMRU Computation
        // We will have one Map operator per input file.
        IOperatorDescriptor mapOperator = new MapOperatorDescriptor(spec, model, envInPath, confFactory, roundNum,
                "map");
        // For repeatability of the partition assignments, seed the source of
        // randomness using the job id.
        Random random = new Random(id.getLeastSignificantBits());
        ClusterConfig.setLocationConstraint(spec, mapOperator, inputSplits, inputPaths, random);

        // Environment updating
        IOperatorDescriptor updateOperator = new UpdateOperatorDescriptor(spec, model, envInPath, confFactory,
                envOutPath);
        PartitionConstraintHelper.addPartitionCountConstraint(spec, updateOperator, 1);

        // Connect things together
        IConnectorDescriptor mapReduceConn = new MToNReplicatingConnectorDescriptor(spec);

        spec.connect(mapReduceConn, mapOperator, 0, updateOperator, 0);

        spec.addRoot(updateOperator);
        return spec;
    }

}
