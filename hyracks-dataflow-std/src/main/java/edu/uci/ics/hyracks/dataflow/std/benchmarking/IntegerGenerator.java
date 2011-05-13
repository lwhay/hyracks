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
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

/**
 * @author jarodwen
 *
 */
public class IntegerGenerator implements ITypeGenerator<Integer> {
    
    private static final long serialVersionUID = 1L;
    private final Random rand;
    private final int max;
    private int randSeed;
    
    public IntegerGenerator(int randSeed){
        this.randSeed = randSeed;
        rand = new Random(randSeed);
        max = Integer.MAX_VALUE;
    }
    
    public IntegerGenerator(int max, int randSeed){
        this.randSeed = randSeed;
        rand = new Random(randSeed);
        this.max = max;
    }
    
    public Integer generate(int key) {
        return (key + randSeed) % max;
    }
    
    public Integer generate() {
        return rand.nextInt(max);
    }

    public void reset() {
        randSeed = rand.nextInt();
        rand.setSeed(randSeed);
    }

    public ISerializerDeserializer<Integer> getSeDerInstance() {
        return IntegerSerializerDeserializer.INSTANCE;
    }
}
