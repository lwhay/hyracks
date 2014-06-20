package edu.uci.ics.hyracks.api.io;

public class StreamDescriptor {
    private int streamId;
    private IStreamIOManager.FileStreamReadWriteMode mode;
    
    public StreamDescriptor(int streamId, IStreamIOManager.FileStreamReadWriteMode mode){
       this.mode = mode;
       this.streamId = streamId;
    }
    
    public IStreamIOManager.FileStreamReadWriteMode getMode(){ return mode;}
    public int getStreamId(){ return streamId;} 
}
