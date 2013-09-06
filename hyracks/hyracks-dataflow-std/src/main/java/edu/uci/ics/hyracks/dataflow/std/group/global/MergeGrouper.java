package edu.uci.ics.hyracks.dataflow.std.group.global;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;

public class MergeGrouper {

    private final IHyracksTaskContext ctx;
    
    
    private final int[] keyFields;
    
    private final IBinaryComparator[] comparators;
    
    private final int framesLimit;
    
    private final IAggregatorDescriptor merger;
    private AggregateState mergeState;
    
    public MergeGrouper(IHyracksTaskContext ctx, int[] keyFields, int framesLimit,
            IBinaryComparator[] comparators, IAggregatorDescriptor merger,
            RecordDescriptor inRecDesc, RecordDescriptor outRecDesc) throws HyracksDataException {
        this.ctx = ctx;
        this.keyFields = keyFields;
        this.framesLimit = framesLimit;
        this.comparators = comparators;
        this.merger = merger;
    }
    
}
