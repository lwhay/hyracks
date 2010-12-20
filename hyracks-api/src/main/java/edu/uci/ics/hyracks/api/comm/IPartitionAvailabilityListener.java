package edu.uci.ics.hyracks.api.comm;

public interface IPartitionAvailabilityListener {
    public void notifyPartitionAvailability(PartitionId partitionId, NetworkAddress netAddress);

    public void noMorePartitions();
}