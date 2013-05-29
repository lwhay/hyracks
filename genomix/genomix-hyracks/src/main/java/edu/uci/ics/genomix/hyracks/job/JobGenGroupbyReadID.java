package edu.uci.ics.genomix.hyracks.job;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import edu.uci.ics.genomix.data.Marshal;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.hyracks.api.client.NodeControllerInfo;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriter;
import edu.uci.ics.hyracks.hdfs.api.ITupleWriterFactory;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSReadOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.dataflow.HDFSWriteOperatorDescriptor;
import edu.uci.ics.hyracks.hdfs.scheduler.Scheduler;

public class JobGenGroupbyReadID extends JobGenBrujinGraph {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public JobGenGroupbyReadID(GenomixJobConf job, Scheduler scheduler, Map<String, NodeControllerInfo> ncMap,
            int numPartitionPerMachine) throws HyracksDataException {
        super(job, scheduler, ncMap, numPartitionPerMachine);
        // TODO Auto-generated constructor stub
    }

    @Override
    public JobSpecification generateJob() throws HyracksException {

        JobSpecification jobSpec = new JobSpecification();
        logDebug("ReadKmer Operator");
        HDFSReadOperatorDescriptor readOperator = createHDFSReader(jobSpec);

        logDebug("Group by Kmer");
        AbstractOperatorDescriptor lastOperator = generateGroupbyKmerJob(jobSpec, readOperator);

        //logDebug("Write kmer to result");
        //generateRootByWriteKmerGroupbyResult(jobSpec, lastOperator);

        logDebug("Map Kmer to Read Operator");
        lastOperator = generateMapperFromKmerToRead(jobSpec, lastOperator);

        logDebug("Group by Read Operator");
        lastOperator = generateGroupbyReadJob(jobSpec, lastOperator);

        logDebug("Write node to result");
        lastOperator = generateRootByWriteReadIDAggregationResult(jobSpec, lastOperator);
        jobSpec.addRoot(lastOperator);
        return jobSpec;
    }

    public AbstractOperatorDescriptor generateRootByWriteReadIDAggregationResult(JobSpecification jobSpec,
            AbstractOperatorDescriptor readCrossAggregator) throws HyracksException {
        HDFSWriteOperatorDescriptor writeKmerOperator = new HDFSWriteOperatorDescriptor(jobSpec,
                hadoopJobConfFactory.getConf(), new ITupleWriterFactory() {

                    /**
                     * 
                     */
                    private static final long serialVersionUID = 1L;

                    @Override
                    public ITupleWriter getTupleWriter(IHyracksTaskContext ctx) throws HyracksDataException {
                        // TODO Auto-generated method stub
                        return new ITupleWriter() {

                            private KmerBytesWritable kmer = new KmerBytesWritable(kmerSize);
                            private PositionListWritable plist = new PositionListWritable();

                            @Override
                            public void open(DataOutput output) throws HyracksDataException {
                                // TODO Auto-generated method stub

                            }

                            @Override
                            public void write(DataOutput output, ITupleReference tuple) throws HyracksDataException {
                                int readId = Marshal.getInt(tuple.getFieldData(0), tuple.getFieldStart(0));
                                try {
                                    output.write((Integer.toString(readId) + "\t").getBytes());
                                    for (int i = 1; i < tuple.getFieldCount(); i++) {
                                        int fieldOffset = tuple.getFieldStart(i);
                                        while (fieldOffset < tuple.getFieldStart(i) + tuple.getFieldLength(i)) {
                                            byte[] buffer = tuple.getFieldData(i);
                                            // read poslist
                                            int posCount = PositionListWritable.getCountByDataLength(Marshal.getInt(
                                                    buffer, fieldOffset));
                                            fieldOffset += 4;
                                            plist.setNewReference(posCount, buffer, fieldOffset);
                                            fieldOffset += plist.getLength();

                                            int kmerbytes = Marshal.getInt(buffer, fieldOffset);
                                            if (kmer.getLength() != kmerbytes) {
                                                throw new IllegalArgumentException("kmerlength is invalid");
                                            }
                                            fieldOffset += 4;
                                            kmer.setNewReference(buffer, fieldOffset);
                                            fieldOffset += kmer.getLength();

                                            output.write(Integer.toString(i - 1).getBytes());
                                            output.writeByte('\t');
                                            output.write(plist.toString().getBytes());
                                            output.writeByte('\t');
                                            output.write(kmer.toString().getBytes());
                                            output.writeByte('\t');
                                        }
                                    }
                                    output.writeByte('\n');
                                } catch (IOException e) {
                                    throw new HyracksDataException(e);
                                }
                            }

                            @Override
                            public void close(DataOutput output) throws HyracksDataException {
                                // TODO Auto-generated method stub

                            }

                        };
                    }

                });
        connectOperators(jobSpec, readCrossAggregator, ncNodeNames, writeKmerOperator, ncNodeNames,
                new OneToOneConnectorDescriptor(jobSpec));

        return writeKmerOperator;
    }

}
