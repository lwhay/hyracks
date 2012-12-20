package edu.uci.ics.hyracks.imru.base;

import java.util.UUID;

import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.imru.api.IIMRUJobSpecification;

/**
 * Interface for factories for creating dataflows for IMRU data loading and iterations.
 */
public interface IJobFactory {

    /**
     * Construct a JobSpecification for a single iteration of IMRU.
     *
     * @param model
     *            The IIMRUJobSpecification
     * @param id
     *            A UUID identifying the IMRU job that this iteration belongs to.
     * @param roundNum
     *            The round number.
     * @param modelInPath
     *            The HDFS path to read the current model from.
     * @param modelOutPath
     *            The HDFS path to write the updated model to.
     * @return A JobSpecification for an iteration of IMRU.
     * @throws HyracksException
     */
    @SuppressWarnings("rawtypes")
    public JobSpecification generateJob(IIMRUJobSpecification model, UUID id, int roundNum, String modelInPath,
            String modelOutPath) throws HyracksException;

    /**
     * Construct a JobSpecificiation for caching IMRU input records.
     *
     * @param model
     *            The IIMRUJobSpecification
     * @param id
     *            A UUID identifying the IMRU job that this iteration belongs to.
     * @return A JobSpecification for caching the IMRU training examples.
     * @throws HyracksException
     */
    @SuppressWarnings("rawtypes")
    public JobSpecification generateDataLoadJob(IIMRUJobSpecification model, UUID id) throws HyracksException;

}
