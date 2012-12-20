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
import edu.uci.ics.hyracks.dataflow.std.connectors.LocalityAwareMToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNReplicatingConnectorDescriptor;
import edu.uci.ics.hyracks.imru.api.IIMRUJobSpecification;
import edu.uci.ics.hyracks.imru.dataflow.MapOperatorDescriptor;
import edu.uci.ics.hyracks.imru.dataflow.ReduceOperatorDescriptor;
import edu.uci.ics.hyracks.imru.dataflow.UpdateOperatorDescriptor;
import edu.uci.ics.hyracks.imru.file.HDFSInputSplitProvider;
import edu.uci.ics.hyracks.imru.hadoop.config.ConfigurationFactory;
import edu.uci.ics.hyracks.imru.jobgen.clusterconfig.ClusterConfig;

/**
 * Generates JobSpecifications for iterations of iterative map reduce
 * update, using a generic aggregation tree. In this aggregation tree,
 * one level of reducers sits between the Map and Update operators.
 * Map outputs are divided among the Reduce operators, which are
 * assigned to random NCs by the Hyracks scheduler.
 *
 * @author Josh Rosen
 */
public class GenericAggregationIMRUJobFactory extends AbstractIMRUJobFactory {

    private final int reducerCount;

    /**
     * Construct a new GenericAggregationIMRUJobFactory.
     *
     * @param inputPaths
     *            A comma-separated list of paths specifying the input
     *            files.
     * @param confFactory
     *            A Hadoop configuration used for HDFS settings.
     * @param reducerCount
     *            The number of reducers to use.
     */
    public GenericAggregationIMRUJobFactory(String inputPaths, ConfigurationFactory confFactory, int reducerCount) {
        super(inputPaths, confFactory);
        this.reducerCount = reducerCount;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public JobSpecification generateJob(IIMRUJobSpecification imruSpec, UUID id, int roundNum, String modelInPath,
            String modelOutPath) throws HyracksException {

        JobSpecification spec = new JobSpecification();
        // Create operators
        // File reading and writing
        HDFSInputSplitProvider inputSplitProvider = new HDFSInputSplitProvider(inputPaths,
                confFactory.createConfiguration());
        List<InputSplit> inputSplits = inputSplitProvider.getInputSplits();

        // IMRU computation
        // We will have one Map operator per input file.
        IOperatorDescriptor mapOperator = new MapOperatorDescriptor(spec, imruSpec, modelInPath, confFactory, roundNum);
        // For repeatability of the partition assignments, seed the source of
        // randomness using the job id.
        Random random = new Random(id.getLeastSignificantBits());
        final String[] mapOperatorLocations = ClusterConfig.setLocationConstraint(spec, mapOperator,
                inputSplits, random);

        // Update operator
        IOperatorDescriptor updateOperator = new UpdateOperatorDescriptor(spec, imruSpec, modelInPath, confFactory,
                modelOutPath);
        PartitionConstraintHelper.addPartitionCountConstraint(spec, updateOperator, 1);

        // One level of reducers (ala Hadoop)
        IOperatorDescriptor reduceOperator = new ReduceOperatorDescriptor(spec, imruSpec);
        PartitionConstraintHelper.addPartitionCountConstraint(spec, reduceOperator, reducerCount);

        // Set up the local combiners (machine-local reducers)
        IConnectorDescriptor mapReducerConn = new LocalityAwareMToNPartitioningConnectorDescriptor(spec,
                OneToOneTuplePartitionComputerFactory.INSTANCE, new RangeLocalityMap(mapOperatorLocations.length));
        LocalReducerFactory.addLocalReducers(spec, mapOperator, 0, mapOperatorLocations, reduceOperator, 0,
                mapReducerConn, imruSpec);

        // Connect things together
        IConnectorDescriptor reduceUpdateConn = new MToNReplicatingConnectorDescriptor(spec);
        spec.connect(reduceUpdateConn, reduceOperator, 0, updateOperator, 0);

        spec.addRoot(updateOperator);
        return spec;
    }
}
