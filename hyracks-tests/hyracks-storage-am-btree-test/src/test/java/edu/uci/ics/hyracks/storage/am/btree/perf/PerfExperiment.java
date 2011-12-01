package edu.uci.ics.hyracks.storage.am.btree.perf;

import java.util.Enumeration;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTrait;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.SerdeUtils;
import edu.uci.ics.hyracks.storage.am.btree.datagen.DataGenThread;
import edu.uci.ics.hyracks.storage.am.common.ophelpers.MultiComparator;

public class PerfExperiment {
    public static void main(String[] args) throws Exception {
        // Disable logging so we can better see the output times.
        Enumeration<String> loggers = LogManager.getLogManager().getLoggerNames();
        while(loggers.hasMoreElements()) {
            String loggerName = loggers.nextElement();
            Logger logger = LogManager.getLogManager().getLogger(loggerName);
            logger.setLevel(Level.OFF);
        }
        
        int numTuples = 1000000;
        
        ISerializerDeserializer[] fieldSerdes = new ISerializerDeserializer[] { IntegerSerializerDeserializer.INSTANCE };
        ITypeTrait[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes, 30);
        
        // Add 1 byte for the null flags.
        // TODO: hide this in some method.
        int tupleSize = 4 + 30 + 1;
        
        IBinaryComparator[] cmps = SerdeUtils.serdesToComparators(fieldSerdes, fieldSerdes.length);
        MultiComparator cmp = new MultiComparator(cmps);
        
        int repeats = 3;
        long[] times = new long[repeats];
        //ConcurrentSkipListRunner runner = new ConcurrentSkipListRunner(numTuples, tupleSize, typeTraits, cmp);
        BTreeRunner runner = new BTreeRunner(numTuples, 8192, 200000, typeTraits, cmp);
        //BTreeBulkLoadRunner runner = new BTreeBulkLoadRunner(numTuples, 8192, 100000, typeTraits, cmp, 1.0f);
        runner.init();
        for (int i = 0; i < repeats; i++) {
            DataGenThread dataGen = new DataGenThread(numTuples, 10000, fieldSerdes, 30, false);
            dataGen.start();            
            times[i] = runner.runExperiment(dataGen);
            System.out.println("TIME " + i + ": " + times[i] + "ms");
        }
        runner.deinit();
        long avgTime = 0;
        for (int i = 0; i < repeats; i++) {
            avgTime += times[i];
        }
        avgTime /= repeats;
        System.out.println("AVG TIME: " + avgTime + "ms");
    }
}
