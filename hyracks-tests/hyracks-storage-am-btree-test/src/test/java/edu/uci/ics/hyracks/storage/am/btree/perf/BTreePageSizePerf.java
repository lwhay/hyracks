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

public class BTreePageSizePerf {
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
        
        IBinaryComparator[] cmps = SerdeUtils.serdesToComparators(fieldSerdes, fieldSerdes.length);
        MultiComparator cmp = new MultiComparator(cmps);
        
        //runExperiment(numTuples, 128, 800000, fieldSerdes, cmp, typeTraits);
        //runExperiment(numTuples, 256, 400000, fieldSerdes, cmp, typeTraits);
        //runExperiment(numTuples, 512, 200000, fieldSerdes, cmp, typeTraits);
        //runExperiment(numTuples, 1024, 100000, fieldSerdes, cmp, typeTraits);
        //runExperiment(numTuples, 2048, 50000, fieldSerdes, cmp, typeTraits);
        //runExperiment(numTuples, 4096, 25000, fieldSerdes, cmp, typeTraits);
        runExperiment(numTuples, 8192, 12500, fieldSerdes, cmp, typeTraits);
        //runExperiment(numTuples, 16384, 6250, fieldSerdes, cmp, typeTraits);
        //runExperiment(numTuples, 32768, 3125, fieldSerdes, cmp, typeTraits);
        //runExperiment(numTuples, 65536, 1564, fieldSerdes, cmp, typeTraits);
        //runExperiment(numTuples, 131072, 782, fieldSerdes, cmp, typeTraits);
        //runExperiment(numTuples, 262144, 391, fieldSerdes, cmp, typeTraits);
    }
    
    private static void runExperiment(int numTuples, int pageSize, int numPages, ISerializerDeserializer[] fieldSerdes, MultiComparator cmp, ITypeTrait[] typeTraits) throws Exception {
        System.out.println("PAGE SIZE: " + pageSize);
        System.out.println("NUM PAGES: " + numPages);
        System.out.println("MEMORY: " + (pageSize * numPages));
        int repeats = 5;
        long[] times = new long[repeats];
        BTreeRunner runner = new BTreeRunner(numTuples, pageSize, numPages, typeTraits, cmp);
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
        System.out.println("-------------------------------");
    }
}
