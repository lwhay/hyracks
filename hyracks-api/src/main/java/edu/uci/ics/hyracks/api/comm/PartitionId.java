package edu.uci.ics.hyracks.api.comm;

import java.io.Serializable;
import java.util.UUID;

import edu.uci.ics.hyracks.api.dataflow.ConnectorDescriptorId;

public final class PartitionId implements Serializable {
    private static final long serialVersionUID = 1L;

    private final UUID jobId;
    private final int attempt;
    private final ConnectorDescriptorId cdId;
    private final int producerPartitionIndex;
    private final int consumerPartitionIndex;

    public PartitionId(UUID jobId, int attempt, ConnectorDescriptorId cdId, int producerPartitionIndex,
            int consumerPartitionIndex) {
        this.jobId = jobId;
        this.attempt = attempt;
        this.cdId = cdId;
        this.producerPartitionIndex = producerPartitionIndex;
        this.consumerPartitionIndex = consumerPartitionIndex;
    }

    public UUID getJobId() {
        return jobId;
    }

    public int getAttempt() {
        return attempt;
    }

    public ConnectorDescriptorId getOperatorDescriptorId() {
        return cdId;
    }

    public int getProducerPartitionIndex() {
        return producerPartitionIndex;
    }

    public int getConsumerPartitionIndex() {
        return consumerPartitionIndex;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + attempt;
        result = prime * result + ((cdId == null) ? 0 : cdId.hashCode());
        result = prime * result + consumerPartitionIndex;
        result = prime * result + ((jobId == null) ? 0 : jobId.hashCode());
        result = prime * result + producerPartitionIndex;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PartitionId other = (PartitionId) obj;
        if (attempt != other.attempt)
            return false;
        if (cdId == null) {
            if (other.cdId != null)
                return false;
        } else if (!cdId.equals(other.cdId))
            return false;
        if (consumerPartitionIndex != other.consumerPartitionIndex)
            return false;
        if (jobId == null) {
            if (other.jobId != null)
                return false;
        } else if (!jobId.equals(other.jobId))
            return false;
        if (producerPartitionIndex != other.producerPartitionIndex)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return jobId + "_" + attempt + "_" + cdId.getId() + "_" + producerPartitionIndex + "_" + consumerPartitionIndex;
    }
}