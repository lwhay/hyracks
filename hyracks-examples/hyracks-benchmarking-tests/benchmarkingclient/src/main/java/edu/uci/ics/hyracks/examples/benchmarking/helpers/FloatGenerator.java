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
package edu.uci.ics.hyracks.examples.benchmarking.helpers;

import java.util.Random;

/**
 * Float data generator.
 *
 */
public class FloatGenerator implements IDataGenerator<Float> {

    private final Random rand = new Random();
    
    private FloatGenerator(){};
    
    private static class SingletonHolder {
        public static long SEED = System.currentTimeMillis();
        public static final FloatGenerator INSTANCE = new FloatGenerator();
    }
    
    public static FloatGenerator getSingleton(){
        return SingletonHolder.INSTANCE;
    }
    
    public static FloatGenerator getSingleton(long seed){
        SingletonHolder.SEED = seed;
        return SingletonHolder.INSTANCE;
    }
    
    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.examples.benchmarking.helpers.IDataGenerator#generate()
     */
    @Override
    public Float generate() {
        rand.setSeed(SingletonHolder.SEED ++);
        rand.setSeed(rand.nextLong());
        return rand.nextFloat();
    }

    /* (non-Javadoc)
     * @see edu.uci.ics.hyracks.examples.benchmarking.helpers.IDataGenerator#generate(long)
     */
    @Override
    public Float generate(long key) {
        rand.setSeed(key);
        return rand.nextFloat();
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        FloatGenerator test = FloatGenerator.getSingleton();
        System.out.println("**** Generate random: " + test.generate());
        System.out.println("**** Generate fixed length 20: " + test.generate(20));
        System.out.println("**** Generate fixed key: " + test.generate(Long.valueOf("20110206")));
        System.out.println("**** Generate fixed key again: " + test.generate(Long.valueOf("20110206")));
        System.out.println("**** Generate fixed length 20 again: " + test.generate(20));
    }

}
