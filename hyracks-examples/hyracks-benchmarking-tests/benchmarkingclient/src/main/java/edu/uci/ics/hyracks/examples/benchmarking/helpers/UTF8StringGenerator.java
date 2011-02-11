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
 * Random string generator.
 *
 */
public class UTF8StringGenerator implements IDataGenerator<String>{
    
    private UTF8StringGenerator(){};
    
    // Random string source
    private static final String RANDOMSRC = "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890";
    
    private final Random rand = new Random();
    
    private static class SingletonHolder {
        public static long SEED = System.currentTimeMillis();
        public static final UTF8StringGenerator INSTANCE = new UTF8StringGenerator();
    }
    
    public static UTF8StringGenerator getSingleton(){
        return SingletonHolder.INSTANCE;
    }
    
    public static UTF8StringGenerator getSingleton(long seed){
        SingletonHolder.SEED = seed;
        return SingletonHolder.INSTANCE;
    }

    /**
     * @param args
     */
    public static void main(String[] args) {
        UTF8StringGenerator test = UTF8StringGenerator.getSingleton();
        System.out.println("**** Generate random: " + test.generate());
        System.out.println("**** Generate fixed length 20: " + test.generate(20));
        System.out.println("**** Generate fixed key: " + test.generate(Long.valueOf("20110206")));
        System.out.println("**** Generate fixed key with fixed length: " + test.generate(Long.valueOf("20110206"), 20));
        System.out.println("**** Generate fixed key again: " + test.generate(Long.valueOf("20110206")));
        System.out.println("**** Generate fixed key with fixed length again: " + test.generate(Long.valueOf("20110206"), 20));
        System.out.println("**** Generate fixed length 20 again: " + test.generate(20));
    }

    @Override
    public String generate() {
        rand.setSeed(SingletonHolder.SEED ++);
        char[] text = new char[rand.nextInt(100) + 1];
        for(int i = 0; i < text.length; i++){
            text[i] = RANDOMSRC.charAt(rand.nextInt(RANDOMSRC.length()));
        }
        return new String(text);
    }
    
    public String generate(int length){
        rand.setSeed(SingletonHolder.SEED ++);
        char[] text = new char[length];
        for(int i = 0; i < length; i++){
            text[i] = RANDOMSRC.charAt(rand.nextInt(RANDOMSRC.length()));
        }
        return new String(text);
    }

    @Override
    public String generate(long key) {
        rand.setSeed(SingletonHolder.SEED ++);
        char[] text = new char[rand.nextInt(100) + 1];
        rand.setSeed(key);
        for(int i = 0; i < text.length; i++){
            text[i] = RANDOMSRC.charAt(rand.nextInt(RANDOMSRC.length()));
        }
        return new String(text);
    }
    
    public String generate(long key, int length){
        char[] text = new char[length];
        rand.setSeed(key);
        for(int i = 0; i < length; i++){
            text[i] = RANDOMSRC.charAt(rand.nextInt(RANDOMSRC.length()));
        }
        return new String(text);
    }

}
