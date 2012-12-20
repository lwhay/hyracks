package edu.uci.ics.hyracks.imru.jobgen;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

/**
 * A TuplePartitionComputerFactory that always sends tuples to partition 0.
 *
 * For use with LocalityAwareMToNPartitioningConnectorDescriptor.
 */
public class OneToOneTuplePartitionComputerFactory implements ITuplePartitionComputerFactory {
    private static final long serialVersionUID = 1L;
    public static final OneToOneTuplePartitionComputerFactory INSTANCE = new OneToOneTuplePartitionComputerFactory();

    @Override
    public ITuplePartitionComputer createPartitioner() {
        return new ITuplePartitionComputer() {

            @Override
            public int partition(IFrameTupleAccessor accessor, int tIndex, int nParts) throws HyracksDataException {
                assert nParts == 1;
                return 0;
            }
        };
    }
}
