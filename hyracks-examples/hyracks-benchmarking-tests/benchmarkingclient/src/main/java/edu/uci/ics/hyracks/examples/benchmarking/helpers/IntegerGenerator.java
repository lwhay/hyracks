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
 * Integer generator
 *
 */
public class IntegerGenerator implements IDataGenerator<Integer> {

    private IntegerGenerator(){};
    
    private final Random rand = new Random();
    
    private static class SingletonHolder {
        public static long SEED = System.currentTimeMillis();
        public static final IntegerGenerator INSTANCE = new IntegerGenerator();
    }
    
    public static IntegerGenerator getSingleton(){
        return SingletonHolder.INSTANCE;
    }
    
    public static IntegerGenerator getSingleton(long seed){
        SingletonHolder.SEED = seed;
        return SingletonHolder.INSTANCE;
    }
    
    /**
     * @param args
     */
    public static void main(String[] args) {
        IntegerGenerator test = IntegerGenerator.getSingleton();
        System.out.println("**** Generate random: " + test.generate());
        System.out.println("**** Generate random: " + test.generate());
        System.out.println("**** Generate random: " + test.generate());
        System.out.println("**** Generate fixed length 20: " + test.generate(20));
        System.out.println("**** Generate fixed key: " + test.generate(Long.valueOf("20110206")));
        System.out.println("**** Generate fixed key with fixed length: " + test.generate(Long.valueOf("20110206"), 20));
        System.out.println("**** Generate fixed key again: " + test.generate(Long.valueOf("20110206")));
        System.out.println("**** Generate fixed key with fixed length again: " + test.generate(Long.valueOf("20110206"), 20));
        System.out.println("**** Generate fixed length 20 again: " + test.generate(20));
    }

    @Override
    public Integer generate() {
        rand.setSeed(SingletonHolder.SEED ++);
        return rand.nextInt(rand.nextInt(65535));
    }
    
    public Integer generate(int max){
        rand.setSeed(SingletonHolder.SEED ++);
        return rand.nextInt(max);
    }

    @Override
    public Integer generate(long key) {
        rand.setSeed(key);
        return rand.nextInt(rand.nextInt(65535));
    }
    
    public Integer generate(long key, int max){
        rand.setSeed(key);
        return rand.nextInt(max);
    }

}
