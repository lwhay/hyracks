package edu.uci.ics.genomix.pregelix.io;

import java.io.*;

import org.apache.hadoop.io.WritableComparable;

import edu.uci.ics.genomix.pregelix.type.MessageFlag;
import edu.uci.ics.genomix.pregelix.type.State;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.PositionListWritable;
import edu.uci.ics.genomix.type.PositionWritable;

public class VertexValueWritable implements WritableComparable<VertexValueWritable> {

    private AdjacencyListWritable incomingList;
    private AdjacencyListWritable outgoingList;
    private byte state;
    private KmerBytesWritable kmer;
    private PositionWritable mergeDest;

    public VertexValueWritable() {
        incomingList = new AdjacencyListWritable();
        outgoingList = new AdjacencyListWritable();
        state = State.NON_VERTEX;
        kmer = new KmerBytesWritable(0);
        mergeDest = new PositionWritable();
    }

    public VertexValueWritable(PositionListWritable forwardForwardList, PositionListWritable forwardReverseList,
            PositionListWritable reverseForwardList, PositionListWritable reverseReverseList,
            byte state, KmerBytesWritable kmer) {
        set(forwardForwardList, forwardReverseList, 
                reverseForwardList, reverseReverseList,
                state, kmer);
    }
    
    public void set(PositionListWritable forwardForwardList, PositionListWritable forwardReverseList,
            PositionListWritable reverseForwardList, PositionListWritable reverseReverseList, 
            byte state, KmerBytesWritable kmer) {
        this.incomingList.setForwardList(reverseForwardList);
        this.incomingList.setReverseList(reverseReverseList);
        this.outgoingList.setForwardList(forwardForwardList);
        this.outgoingList.setReverseList(forwardReverseList);
        this.state = state;
        this.kmer.set(kmer);
    }
    
    public void set(VertexValueWritable value) {
        set(value.getFFList(),value.getFRList(),value.getRFList(),value.getRRList(),value.getState(),
                value.getKmer());
    }
    
    public PositionListWritable getFFList() {
        return outgoingList.getForwardList();
    }

    public PositionListWritable getFRList() {
        return outgoingList.getReverseList();
    }

    public PositionListWritable getRFList() {
        return incomingList.getForwardList();
    }

    public PositionListWritable getRRList() {
        return incomingList.getReverseList();
    }
    
    public void setFFList(PositionListWritable forwardForwardList){
        outgoingList.setForwardList(forwardForwardList);
    }
    
    public void setFRList(PositionListWritable forwardReverseList){
        outgoingList.setReverseList(forwardReverseList);
    }
    
    public void setRFList(PositionListWritable reverseForwardList){
        incomingList.setForwardList(reverseForwardList);
    }

    public void setRRList(PositionListWritable reverseReverseList){
        incomingList.setReverseList(reverseReverseList);
    }
    
    public AdjacencyListWritable getIncomingList() {
        return incomingList;
    }

    public void setIncomingList(AdjacencyListWritable incomingList) {
        this.incomingList.set(incomingList);
    }

    public AdjacencyListWritable getOutgoingList() {
        return outgoingList;
    }

    public void setOutgoingList(AdjacencyListWritable outgoingList) {
        this.outgoingList.set(outgoingList);
    }

    public byte getState() {
        return state;
    }

    public void setState(byte state) {
        this.state = state;
    }

    public int getLengthOfKmer() {
        return kmer.getKmerLength();
    }

    public KmerBytesWritable getKmer() {
        return kmer;
    }

    public void setKmer(KmerBytesWritable kmer) {
        this.kmer.set(kmer);
    }
    
    public PositionWritable getMergeDest() {
        return mergeDest;
    }

    public void setMergeDest(PositionWritable mergeDest) {
        this.mergeDest = mergeDest;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        incomingList.readFields(in);
        outgoingList.readFields(in);
        state = in.readByte();
        kmer.readFields(in);
        mergeDest.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        incomingList.write(out);
        outgoingList.write(out);
        out.writeByte(state);
        kmer.write(out);
        mergeDest.write(out);
    }

    @Override
    public int compareTo(VertexValueWritable o) {
        return 0;
    }

    @Override
    public String toString() {
        StringBuilder sbuilder = new StringBuilder();
        sbuilder.append('(');
        sbuilder.append(outgoingList.getForwardList().toString()).append('\t');
        sbuilder.append(outgoingList.getReverseList().toString()).append('\t');
        sbuilder.append(incomingList.getForwardList().toString()).append('\t');
        sbuilder.append(incomingList.getReverseList().toString()).append('\t');
        sbuilder.append(kmer.toString()).append(')');
        return sbuilder.toString();
    }
    
    public int inDegree(){
        return incomingList.getForwardList().getCountOfPosition() + incomingList.getReverseList().getCountOfPosition();
    }
    
    public int outDegree(){
        return outgoingList.getForwardList().getCountOfPosition() + outgoingList.getReverseList().getCountOfPosition();
    }
    
    /*
     * Process any changes to value.  This is for edge updates
     */
    public void processUpdates(byte neighborToDeleteDir, PositionWritable nodeToDelete,
            byte neighborToMergeDir, PositionWritable nodeToAdd){
//        TODO
//        this.getListFromDir(neighborToDeleteDir).remove(nodeToDelete);
//        this.getListFromDir(neighborToMergeDir).append(nodeToDelete);
        
        switch (neighborToDeleteDir & MessageFlag.DIR_MASK) {
            case MessageFlag.DIR_FF:
                this.getFFList().remove(nodeToDelete);
                break;
            case MessageFlag.DIR_FR:
                this.getFRList().remove(nodeToDelete);
                break;
            case MessageFlag.DIR_RF:
                this.getRFList().remove(nodeToDelete);
                break;
            case MessageFlag.DIR_RR:
                this.getRRList().remove(nodeToDelete);
                break;
        }
        switch (neighborToMergeDir & MessageFlag.DIR_MASK) {
            case MessageFlag.DIR_FF:
                this.getFFList().append(nodeToAdd);
                break;
            case MessageFlag.DIR_FR:
                this.getFRList().append(nodeToAdd);
                break;
            case MessageFlag.DIR_RF:
                this.getRFList().append(nodeToAdd);
                break;
            case MessageFlag.DIR_RR:
                this.getRRList().append(nodeToAdd);
                break;
        }
    }
    
    /*
     * Process any changes to value.  This is for merging
     */
    public void processMerges(byte neighborToDeleteDir, PositionWritable nodeToDelete,
            byte neighborToMergeDir, PositionWritable nodeToAdd, 
            int kmerSize, KmerBytesWritable kmer){
        switch (neighborToDeleteDir & MessageFlag.DIR_MASK) {
            case MessageFlag.DIR_FF:
                this.getFFList().remove(nodeToDelete); //set(null);
                break;
            case MessageFlag.DIR_FR:
                this.getFRList().remove(nodeToDelete);
                break;
            case MessageFlag.DIR_RF:
                this.getRFList().remove(nodeToDelete);
                break;
            case MessageFlag.DIR_RR:
                this.getRRList().remove(nodeToDelete);
                break;
        }
        // TODO: remove switch below and replace with general direction merge
//        this.getKmer().mergeWithDirKmer(neighborToMergeDir);
        
        switch (neighborToMergeDir & MessageFlag.DIR_MASK) {
            case MessageFlag.DIR_FF:
                this.getKmer().mergeWithFFKmer(kmerSize, kmer);
                this.getFFList().append(nodeToAdd);
                break;
            case MessageFlag.DIR_FR:
                this.getKmer().mergeWithFRKmer(kmerSize, kmer);
                this.getFRList().append(nodeToAdd);
                break;
            case MessageFlag.DIR_RF:
                this.getKmer().mergeWithRFKmer(kmerSize, kmer);
                this.getRFList().append(nodeToAdd);
                break;
            case MessageFlag.DIR_RR:
                this.getKmer().mergeWithRRKmer(kmerSize, kmer);
                this.getRRList().append(nodeToAdd);
                break;
        }
    }
    

}
