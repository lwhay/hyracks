package edu.uci.ics.genomix.pregelix.operator.bridgeremove;

import java.util.Iterator;
import org.apache.hadoop.io.NullWritable;

import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;
import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.GraphCleanOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;

/*
 * vertexId: BytesWritable
 * vertexValue: ByteWritable
 * edgeValue: NullWritable
 * message: MessageWritable
 * 
 * DNA:
 * A: 00
 * C: 01
 * G: 10
 * T: 11
 * 
 * succeed node
 *  A 00000001 1
 *  G 00000010 2
 *  C 00000100 4
 *  T 00001000 8
 * precursor node
 *  A 00010000 16
 *  G 00100000 32
 *  C 01000000 64
 *  T 10000000 128
 *  
 * For example, ONE LINE in input file: 00,01,10    0001,0010,
 * That means that vertexId is ACG, its succeed node is A and its precursor node is C.
 * The succeed node and precursor node will be stored in vertexValue and we don't use edgeValue.
 * The details about message are in edu.uci.ics.pregelix.example.io.MessageWritable. 
 */
/**
 * Naive Algorithm for path merge graph
 */
public class BridgeAddVertex extends
        Vertex<VKmerBytesWritable, VertexValueWritable, NullWritable, MessageWritable> {
    public static final String KMER_SIZE = "BridgeRemoveVertex.kmerSize"; // TODO consolidate config options
    public static final String LENGTH = "BridgeRemoveVertex.length";
    public static int kmerSize = -1;
    private int length = -1;
    
    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1) {
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
            KmerBytesWritable.setGlobalKmerLength(kmerSize);
        }
        if (length == -1)
            length = getContext().getConfiguration().getInt(LENGTH, kmerSize + 5); // TODO fail on parse
    }

    @SuppressWarnings("unchecked")
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if(getSuperstep() == 1){
            if(getVertexId().toString().equals("ATA")){
            	VKmerBytesWritable vertexId = new VKmerBytesWritable(kmerSize);
                vertexId.setByRead(kmerSize, "GTA".getBytes(), 0);
                getVertexValue().getFRList().append(vertexId);
                
                //add bridge vertex
                @SuppressWarnings("rawtypes")
                Vertex vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());
                vertex.getMsgList().clear();
                vertex.getEdges().clear();
                VertexValueWritable vertexValue = new VertexValueWritable(kmerSize);
                /**
                 * set the src vertex id
                 */
                vertex.setVertexId(vertexId);
                /**
                 * set the vertex value
                 */
                VKmerListWritable kmerFRList = new VKmerListWritable();
                kmerFRList.append(getVertexId());
                vertexValue.setFRList(kmerFRList);
                VKmerBytesWritable otherVertexId = new VKmerBytesWritable(kmerSize);
                otherVertexId.setByRead(kmerSize, "ACG".getBytes(), 0);
                VKmerListWritable kmerRFList = new VKmerListWritable();
                kmerRFList.append(otherVertexId);
                vertexValue.setRFList(kmerRFList);
                vertexValue.setActualKmer(vertexId);
                vertex.setVertexValue(vertexValue);
                
                addVertex(vertexId, vertex);
            } 
            else if(getVertexId().toString().equals("ACG")){
                VKmerBytesWritable brdgeVertexId = new VKmerBytesWritable(kmerSize);
                brdgeVertexId.setByRead(kmerSize, "GTA".getBytes(), 0);
                getVertexValue().getRFList().append(brdgeVertexId);
            }
        }
        voteToHalt();
    }

    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(BridgeAddVertex.class.getSimpleName());
        job.setVertexClass(BridgeAddVertex.class);
        /**
         * BinaryInput and BinaryOutput
         */
        job.setVertexInputFormatClass(GraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(GraphCleanOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(KmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        Client.run(args, job);
    }
}