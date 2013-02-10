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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNReplicatingConnectorDescriptor;
import edu.uci.ics.hyracks.imru.api.IIMRUJobSpecification;
import edu.uci.ics.hyracks.imru.dataflow.IMRUOperatorDescriptor;
import edu.uci.ics.hyracks.imru.dataflow.MapOperatorDescriptor;
import edu.uci.ics.hyracks.imru.dataflow.UpdateOperatorDescriptor;
import edu.uci.ics.hyracks.imru.file.HDFSInputSplitProvider;
import edu.uci.ics.hyracks.imru.file.IMRUFileSplit;
import edu.uci.ics.hyracks.imru.hadoop.config.ConfigurationFactory;
import edu.uci.ics.hyracks.imru.jobgen.clusterconfig.ClusterConfig;

/**
 * Generates JobSpecifications for iterations of iterative map reduce
 * update, using an n-ary aggregation tree. The reducers are
 * assigned to random NC's by the Hyracks scheduler.
 * 
 * @author Josh Rosen
 */
public class NAryAggregationIMRUJobFactory extends AbstractIMRUJobFactory {

    private final int fanIn;

    /**
     * Construct a new GenericAggregationIMRUJobFactory.
     * 
     * @param inputPaths
     *            A comma-separated list of paths specifying the input
     *            files.
     * @param confFactory
     *            A Hadoop configuration used for HDFS settings.
     * @param fanIn
     *            The number of incoming connections to each
     *            reducer (excluding the level farthest from
     *            the root).
     */
    public NAryAggregationIMRUJobFactory(String inputPaths, ConfigurationFactory confFactory, int fanIn) {
        super(inputPaths, confFactory);
        this.fanIn = fanIn;
    }

    @SuppressWarnings( { "rawtypes", "unchecked" })
    @Override
    public JobSpecification generateJob(IIMRUJobSpecification model, UUID id, int roundNum, String modelInPath,
            String modelOutPath) throws HyracksException {

        JobSpecification spec = new JobSpecification();
        // Create operators
        // File reading and writing
        Configuration conf = confFactory.createConfiguration();
        HDFSInputSplitProvider inputSplitProvider = new HDFSInputSplitProvider(inputPathCommaSeparated, conf);
        List<IMRUFileSplit> inputSplits = inputSplitProvider.getInputSplits();

        // IMRU Computation
        // We will have one Map operator per input file.
        IMRUOperatorDescriptor mapOperator = new MapOperatorDescriptor(spec, model, modelInPath, confFactory, roundNum,
                "map");
        // For repeatability of the partition assignments, seed the source of
        // randomness using the job id.
        Random random = new Random(id.getLeastSignificantBits());
        final String[] mapOperatorLocations = ClusterConfig.setLocationConstraint(spec, mapOperator, inputSplits,
                random);

        // Environment updating
        IMRUOperatorDescriptor updateOperator = new UpdateOperatorDescriptor(spec, model, modelInPath, confFactory,
                modelOutPath,false);
        PartitionConstraintHelper.addPartitionCountConstraint(spec, updateOperator, 1);

        // Reduce aggregation tree.
        IConnectorDescriptor reduceUpdateConn = new MToNReplicatingConnectorDescriptor(spec);
        ReduceAggregationTreeFactory.buildAggregationTree(spec, mapOperator, 0, inputSplits == null ? inputPaths.length
                : inputSplits.size(), updateOperator, 0, reduceUpdateConn, fanIn, true, mapOperatorLocations, model);

        spec.addRoot(updateOperator);
        return spec;
    }

}
