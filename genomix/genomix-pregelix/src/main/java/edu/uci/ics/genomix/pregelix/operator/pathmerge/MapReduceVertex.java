package edu.uci.ics.genomix.pregelix.operator.pathmerge;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;

import edu.uci.ics.genomix.pregelix.client.Client;
import edu.uci.ics.genomix.pregelix.format.GraphCleanInputFormat;
import edu.uci.ics.genomix.pregelix.format.P2PathMergeOutputFormat;
import edu.uci.ics.genomix.pregelix.io.MessageWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable;
import edu.uci.ics.genomix.pregelix.io.VertexValueWritable.State;
import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.type.VKmerListWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.pregelix.api.graph.Vertex;
import edu.uci.ics.pregelix.api.job.PregelixJob;
import edu.uci.ics.pregelix.api.util.BspUtils;

public class MapReduceVertex extends
    BasicGraphCleanVertex {
    
    public static boolean fakeVertexExist = false;
    protected static VKmerBytesWritable fakeVertex = null;
    
    protected VKmerBytesWritable reverseKmer;
    protected VKmerListWritable kmerList = null;
    protected Map<VKmerBytesWritable, VKmerListWritable> kmerMapper = new HashMap<VKmerBytesWritable, VKmerListWritable>();

    /**
     * initiate kmerSize, maxIteration
     */
    public void initVertex() {
        if (kmerSize == -1)
            kmerSize = getContext().getConfiguration().getInt(KMER_SIZE, 5);
        if (maxIteration < 0)
            maxIteration = getContext().getConfiguration().getInt(ITERATIONS, 1000000);
        if(incomingMsg == null)
            incomingMsg = new MessageWritable(kmerSize);
        if(outgoingMsg == null)
            outgoingMsg = new MessageWritable(kmerSize);
        else
            outgoingMsg.reset(kmerSize);
        if(reverseKmer == null)
            reverseKmer = new VKmerBytesWritable();
        if(kmerList == null)
            kmerList = new VKmerListWritable();
        else
            kmerList.reset();
        if(fakeVertex == null){
            fakeVertex = new VKmerBytesWritable();
            String random = generaterRandomString(kmerSize + 1);
            fakeVertex.setByRead(kmerSize + 1, random.getBytes(), 0); 
        }
        if(destVertexId == null)
            destVertexId = new VKmerBytesWritable(kmerSize);
    }
    
    /**
     * Generate random string from [ACGT]
     */
    public String generaterRandomString(int n){
        char[] chars = "ACGT".toCharArray();
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int i = 0; i < n; i++) {
            char c = chars[random.nextInt(chars.length)];
            sb.append(c);
        }
        return sb.toString();
    }
    
    /**
     * add fake vertex
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void addFakeVertex(){
        if(!fakeVertexExist){
            //add a fake vertex
            Vertex vertex = (Vertex) BspUtils.createVertex(getContext().getConfiguration());
            vertex.getMsgList().clear();
            vertex.getEdges().clear();
            VertexValueWritable vertexValue = new VertexValueWritable(kmerSize + 1);
            vertexValue.setState(State.IS_FAKE);
            vertexValue.setFakeVertex(true);
            
            vertex.setVertexId(fakeVertex);
            vertex.setVertexValue(vertexValue);
            
            addVertex(fakeVertex, vertex);
            fakeVertexExist = true;
        }
    }
    
    public void sendMsgToFakeVertex(){
        if(!getVertexValue().isFakeVertex()){
            outgoingMsg.setSourceVertexId(getVertexId());
            outgoingMsg.setActualKmer(getVertexValue().getActualKmer());
            sendMsg(fakeVertex, outgoingMsg);
            voteToHalt();
        }
    }
    
    public void mapKeyByActualKmer(Iterator<MessageWritable> msgIterator){
        while(msgIterator.hasNext()){
            incomingMsg = msgIterator.next();
            String kmerString = incomingMsg.getActualKmer().toString();
            tmpKmer.reset(kmerString.length());
            tmpKmer.setByRead(kmerString.length(), kmerString.getBytes(), 0);
            reverseKmer.setByReadReverse(kmerString.length(), kmerString.getBytes(), 0);

            if(reverseKmer.compareTo(tmpKmer) < 0)
                tmpKmer.setAsCopy(reverseKmer);
            if(!kmerMapper.containsKey(tmpKmer)){
                kmerList.reset();
                kmerList.append(incomingMsg.getSourceVertexId());
                kmerMapper.put(tmpKmer, kmerList);
            } else{
                kmerList.setCopy(kmerMapper.get(tmpKmer));
                kmerList.append(incomingMsg.getSourceVertexId());
                kmerMapper.put(tmpKmer, kmerList);
            }
        }
    }
    
    public void reduceKeyByActualKmer(){
        for(VKmerBytesWritable key : kmerMapper.keySet()){
            kmerList = kmerMapper.get(key);
            for(int i = 1; i < kmerList.getCountOfPosition(); i++){
                //send kill message
                outgoingMsg.setFlag(MessageFlag.KILL);
                destVertexId.setAsCopy(kmerList.getPosition(i));
                sendMsg(destVertexId, outgoingMsg);
            }
        }
    }
    
    public void finalVertexResponseToFakeVertex(Iterator<MessageWritable> msgIterator){
        while(msgIterator.hasNext()){
            incomingMsg = msgIterator.next();
            inFlag = incomingMsg.getFlag();
            if(inFlag == MessageFlag.KILL){
                broadcaseKillself();
            }
        }
    }
    
    @Override
    public void compute(Iterator<MessageWritable> msgIterator) {
        initVertex();
        if(getSuperstep() == 1){
            addFakeVertex();
        }
        else if(getSuperstep() == 2){
            /** NON-FAKE and Final vertice send msg to FAKE vertex **/
            sendMsgToFakeVertex();
        } else if(getSuperstep() == 3){
            kmerMapper.clear();
            /** Mapper **/
            mapKeyByActualKmer(msgIterator);
            /** Reducer **/
            reduceKeyByActualKmer();
        } else if(getSuperstep() == 4){
            /** only for test single MapReduce job**/
            if(!msgIterator.hasNext() && getVertexValue().getState() == State.IS_FAKE){
                fakeVertexExist = false;
                deleteVertex(fakeVertex);
            }
            finalVertexResponseToFakeVertex(msgIterator);
        } else if(getSuperstep() == 5){
            while (msgIterator.hasNext()) {
                incomingMsg = msgIterator.next();
                if(isKillMsg())
                    responseToDeadVertex();
            }
        }
    }
    
    public static void main(String[] args) throws Exception {
        PregelixJob job = new PregelixJob(MapReduceVertex.class.getSimpleName());
        job.setVertexClass(MapReduceVertex.class);
        /**
         * BinaryInput and BinaryOutput
         */
        job.setVertexInputFormatClass(GraphCleanInputFormat.class);
        job.setVertexOutputFormatClass(P2PathMergeOutputFormat.class);
        job.setDynamicVertexValueSize(true);
        job.setOutputKeyClass(VKmerBytesWritable.class);
        job.setOutputValueClass(VertexValueWritable.class);
        Client.run(args, job);
    }
}