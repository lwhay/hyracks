package edu.uci.ics.hyracks.imru.jobgen;

import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.mapreduce.InputSplit;

import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.imru.api.IIMRUJobSpecification;
import edu.uci.ics.hyracks.imru.base.IJobFactory;
import edu.uci.ics.hyracks.imru.dataflow.DataLoadOperatorDescriptor;
import edu.uci.ics.hyracks.imru.file.HDFSInputSplitProvider;
import edu.uci.ics.hyracks.imru.hadoop.config.ConfigurationFactory;
import edu.uci.ics.hyracks.imru.jobgen.clusterconfig.ClusterConfig;

/**
 * Abstract base class for IMRU Job factories. The data-loading phase
 * is the same for all of the IMRU plans, so it is implemented here.
 */
public abstract class AbstractIMRUJobFactory implements IJobFactory {

    final String inputPaths;
    final ConfigurationFactory confFactory;

    /**
     * Construct a new AbstractIMRUJobFactory.
     *
     * @param inputPaths
     *            The paths to load data from in HDFS.
     * @param confFactory
     *            The HDFS configuration to use.
     */
    public AbstractIMRUJobFactory(String inputPaths, ConfigurationFactory confFactory) {
        this.inputPaths = inputPaths;
        this.confFactory = confFactory;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public JobSpecification generateDataLoadJob(IIMRUJobSpecification model, UUID id) throws HyracksException {
        JobSpecification spec = new JobSpecification();

        HDFSInputSplitProvider inputSplitProvider = new HDFSInputSplitProvider(inputPaths,
                confFactory.createConfiguration());
        List<InputSplit> inputSplits = inputSplitProvider.getInputSplits();

        IOperatorDescriptor dataLoad = new DataLoadOperatorDescriptor(spec, model, inputSplitProvider, confFactory);
        // For repeatability of the partition assignments, seed the
        // source of
        // randomness using the job id.
        Random random = new Random(id.getLeastSignificantBits());
        ClusterConfig.setLocationConstraint(spec, dataLoad, inputSplits, random);
        spec.addRoot(dataLoad);

        return spec;
    }

}