package edu.uci.ics.hyracks.api.comm;

import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IPartitionManager {
    public IFrameWriter createPartitionWriter(IHyracksContext ctx, PartitionId partitionId) throws HyracksDataException;
}