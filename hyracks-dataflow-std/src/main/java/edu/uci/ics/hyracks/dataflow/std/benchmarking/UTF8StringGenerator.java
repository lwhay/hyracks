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
package edu.uci.ics.hyracks.dataflow.std.benchmarking;

import java.util.Random;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;

/**
 * @author jarodwen
 *
 */
public class UTF8StringGenerator implements ITypeGenerator<String> {
    
    private static final long serialVersionUID = 1L;
    private final Random rand;
    private final int maxLength;
    private int randSeed;
    private final boolean isLengthFixed;
    
    private static final String randSrc = "QPWOEIRUTYALSKDJFHGZMXNCBVqwpeorituyasdfghjklzxcvbnm1234567890_";
    
    public UTF8StringGenerator(int randSeed){
        this.randSeed = randSeed;
        rand = new Random(randSeed);
        maxLength = 10;
        isLengthFixed = true;
    }
    
    public UTF8StringGenerator(int maxLength, boolean isLengthFixed, int randSeed){
        this.randSeed = randSeed;
        this.rand = new Random(randSeed);
        this.maxLength = maxLength;
        this.isLengthFixed = isLengthFixed;
    }
    
    public String generate(int key) {
        rand.setSeed(Integer.valueOf(key + randSeed * 31).hashCode());
        int len = maxLength - (isLengthFixed ? 0 : rand.nextInt(maxLength));
        char[] text = new char[len];
        for(int i = 0; i < len; i++) {
            text[i] = randSrc.charAt(rand.nextInt(randSrc.length()));
        }
        return new String(text);
    }
    
    public String generate() {
        int len = maxLength - (isLengthFixed ? 0 : rand.nextInt(maxLength));
        char[] text = new char[len];
        for(int i = 0; i < len; i++) {
            text[i] = randSrc.charAt(rand.nextInt(randSrc.length()));
        }
        return new String(text);
    }

    public void reset() {
        randSeed = rand.nextInt();
        rand.setSeed(randSeed);
    }

    public ISerializerDeserializer<String> getSeDerInstance() {
        return UTF8StringSerializerDeserializer.INSTANCE;
    }
    
    public static void main(String[] args) {
        ITypeGenerator<String> tester = new UTF8StringGenerator((int)(System.currentTimeMillis()));
        for(int i = 0; i < 500; i++) {
            System.out.println("Random string for key " + i + ": " + tester.generate(i));
        }
        System.out.println("Test string generator: ");
        System.out.println("Random string 0: " + tester.generate());
        System.out.println("Random string 1: " + tester.generate());
        System.out.println("Random string 2 for key 9746: " + tester.generate(9764));
        System.out.println("Random string 3: " + tester.generate());
        System.out.println("Random string 4 for key 4738: " + tester.generate(4738));
        System.out.println("Random string 5 for key 9746: " + tester.generate(9764));
        System.out.println("Random string 6 for key 1: " + tester.generate(1));
        System.out.println("Random string 7 for key 2: " + tester.generate(2));
        System.out.println("Reset generator now.");
        tester.reset();
        System.out.println("Random string 8 for key 9746: " + tester.generate(9764));
        System.out.println("Random string 9: " + tester.generate());
        System.out.println("Random string 10 for key 4738: " + tester.generate(4738));
        System.out.println("Random string 11 for key 9746: " + tester.generate(9764));
        System.out.println("Random string 12 for key 1: " + tester.generate(1));
        System.out.println("Random string 13 for key 2: " + tester.generate(2));
        for(int i = 0; i < 500; i++) {
            System.out.println("Random string for key " + i + ": " + tester.generate(i));
        }
    }
}
