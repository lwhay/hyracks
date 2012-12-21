package edu.uci.ics.hyracks.imru.jobgen;

import edu.uci.ics.hyracks.dataflow.std.connectors.ILocalityMap;

class RangeLocalityMap implements ILocalityMap {

    private static final long serialVersionUID = 1L;
    private final int nProducerPartitions;

    public RangeLocalityMap(int nProducerPartitions) {
        this.nProducerPartitions = nProducerPartitions;
    }

    @Override
    public int[] getConsumers(int senderID, int nConsumerPartitions) {
        return new int[] { (int) Math.floor(((1.0 * senderID) / nProducerPartitions) * nConsumerPartitions) };
    }

    @Override
    public boolean isConnected(int senderID, int receiverID, int nConsumerPartitions) {
        return receiverID == getConsumers(senderID, nConsumerPartitions)[0];
    }

    @Override
    public int getConsumerPartitionCount(int nConsumerPartitions) {
        return nConsumerPartitions;
    }
}