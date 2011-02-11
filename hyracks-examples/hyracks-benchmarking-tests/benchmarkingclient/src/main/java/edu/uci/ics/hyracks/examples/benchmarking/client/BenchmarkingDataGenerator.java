/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.examples.benchmarking.client;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.Random;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.examples.benchmarking.helpers.FloatGenerator;
import edu.uci.ics.hyracks.examples.benchmarking.helpers.IDataGenerator;
import edu.uci.ics.hyracks.examples.benchmarking.helpers.IntegerGenerator;
import edu.uci.ics.hyracks.examples.benchmarking.helpers.UTF8StringGenerator;

/**
 * An example data generator for benchmarking data.
 * 
 * This class shows an example of using primitive generators.
 * 
 */
public class BenchmarkingDataGenerator {

    public final RecordDescriptor recordDescriptor;
    
    @SuppressWarnings("rawtypes")
    public final IDataGenerator[] generators;
    
    public final int[] keyFields;
    
    public BenchmarkingDataGenerator(RecordDescriptor recordDescriptor, int[] keyFields){
        this.recordDescriptor = recordDescriptor;
        this.keyFields = keyFields;
        generators = new IDataGenerator[recordDescriptor.getFields().length];
        for(int i = 0; i < recordDescriptor.getFields().length; i++){
            if(recordDescriptor.getFields()[i] instanceof UTF8StringSerializerDeserializer){
                generators[i] = UTF8StringGenerator.getSingleton();
            }else if(recordDescriptor.getFields()[i] instanceof IntegerSerializerDeserializer){
                generators[i] = IntegerGenerator.getSingleton();
            }else if(recordDescriptor.getFields()[i] instanceof FloatSerializerDeserializer){
                generators[i] = FloatGenerator.getSingleton();
            }else{
                generators[i] = UTF8StringGenerator.getSingleton();
            }
        }
    }
    
    /**
     * Generate test data using the given parameters.
     * 
     * @param uniqueKeyRatio    The ratio of the unique keys (upper-bound)
     * @param fileSplits        How many file splits to be produced
     * @param lineCount         How many lines of records to be generated
     * @return
     * @throws IOException
     */
    public FileSplit[] createTestDataset(double uniqueKeyRatio, String fileSplits, long lineCount) throws IOException {
        String[] splits = fileSplits.split(",");
        FileSplit[] fSplits = new FileSplit[splits.length];
        Random rand = new Random();
        // The range of keys
        long keyRange = (long)(uniqueKeyRatio * lineCount);
        // Number of lines included in a file split
        long fileLength = lineCount / splits.length;
        // Generate data with specified unique key ratio
        long lcnt = 0;
        BufferedWriter writer = new BufferedWriter(new FileWriter(splits[0]));
        while(lcnt < lineCount){
            if(lcnt > 0 && lcnt % fileLength == 0){
                writer.close();
                writer = new BufferedWriter(new FileWriter(splits[(int)(lcnt / fileLength)]));
            }
            writer.append(generateRecord((long)(rand.nextFloat() * keyRange)) + "\n");
            lcnt++;
        }
        // TODO create file splits handlers
        writer.close();
        return fSplits;
    }
    
    private String generateRecord(long key){
        StringBuilder sbder = new StringBuilder();
        for( int i = 0; i < generators.length; i++){
            @SuppressWarnings("rawtypes")
            IDataGenerator generator = generators[i];
            boolean isKey = false;
            for(int k : keyFields){
                if(k == i){
                    isKey = true;
                    break;
                }
            }
            if(isKey){
                if(generator instanceof UTF8StringGenerator){
                    sbder.append(((UTF8StringGenerator)generator).generate(key, 20));
                }else if(generator instanceof IntegerGenerator){
                    sbder.append(((IntegerGenerator)generator).generate(key, 60));
                }else{
                    generator.generate(key);
                }
            }else{
                if(generator instanceof UTF8StringGenerator){
                    sbder.append(((UTF8StringGenerator)generator).generate(80));
                }else{
                    sbder.append(generator.generate());
                }
            }
            if(i < recordDescriptor.getFields().length - 1){
                sbder.append("|");
            }
        }
        return sbder.toString();
    }
    
    /**
     * @param args
     * @throws IOException 
     */
    public static void main(String[] args) throws IOException {
        System.out.println("***** Tests for data generator");
        System.out.println("***** Date: " + Calendar.getInstance().getTime().toString());
        
        BenchmarkingDataGenerator testGenerator = new BenchmarkingDataGenerator(new RecordDescriptor(
                new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE,
                        IntegerSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                        UTF8StringSerializerDeserializer.INSTANCE }), new int[] { 0 });
        testGenerator.createTestDataset(0.1, "/Users/jarodwen/Desktop/testDataset.001,/Users/jarodwen/Desktop/testDataset.002", 50);
    }

}
