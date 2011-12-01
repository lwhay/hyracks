package edu.uci.ics.hyracks.storage.am.btree.datagen;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * Quick & dirty data generator for performance testing. 
 *
 */
public class DataGenThread extends Thread {
    public final BlockingQueue<ITupleReference> tupleQueue;
    private final int maxNumTuples;
    private final int maxOutstanding;
    private final Random rnd = new Random(50);
    private int numTuples;
    
    // maxOutstanding pre-created objects for populating the queue.
    private TupleGenerator[] tupleGens;
    private int ringPos;
    
    public DataGenThread(int maxNumTuples, int maxOutstanding, ISerializerDeserializer[] fieldSerdes, int payloadSize, boolean sorted) {
        this.maxNumTuples = maxNumTuples;
        this.maxOutstanding = maxOutstanding;
        tupleQueue = new LinkedBlockingQueue<ITupleReference>(maxOutstanding);
        numTuples = 0;
        tupleGens = new TupleGenerator[maxOutstanding];
        for (int i = 0; i < maxOutstanding; i++) {
            tupleGens[i] = new TupleGenerator(fieldSerdes, payloadSize, rnd, sorted);
        }
        ringPos = 0;
    }
    
    @Override
    public void run() {
        while(numTuples < maxNumTuples) {
            try {
                tupleQueue.put(tupleGens[ringPos].next());
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            numTuples++;
            ringPos++;
            if (ringPos >= maxOutstanding) {
                ringPos = 0;
            }
        }
    }
}
