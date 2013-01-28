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
package edu.uci.ics.hyracks.data.std;

import java.nio.ByteBuffer;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;

public class AbstractHashFunctionQualityTest {

    private static final int testers = 65536;
    private static final int tableSizes[] = new int[] { 65537, 131101 };
    private static final int byteArrayLength = 255;

    private static final Logger LOGGER = Logger.getLogger(AbstractHashFunctionQualityTest.class.getSimpleName());

    public void IntegerKeyFamilyTest(IBinaryHashFunctionFamily hfFamily) {

        ByteBuffer bbuf = ByteBuffer.allocate(4);

        LOGGER.info("====> Hash Quality Test (Integer Keys) for " + hfFamily.getClass().getSimpleName());

        for (int tableSize : tableSizes) {
            ByteBuffer statBuf = ByteBuffer.allocate(tableSize);

            for (int seed = 0; seed < 5; seed++) {

                for (int i = 0; i < statBuf.capacity(); i++) {
                    statBuf.put(i, (byte) 0);
                }

                IBinaryHashFunction hashFunction = hfFamily.createBinaryHashFunction(seed);

                int h;

                long time = System.nanoTime();

                for (int i = testers; i > 0; i--) {
                    bbuf.putInt(0, i);
                    h = hashFunction.hash(bbuf.array(), 0, 4) % tableSize;
                    if (h < 0) {
                        h *= -1;
                    }
                    statBuf.put(h, (byte) (statBuf.get(h) + 1));
                }

                time = System.nanoTime() - time;

                int slots = 0;
                for (int i = 0; i < tableSize; i++) {
                    if (statBuf.get(i) > 0) {
                        slots++;
                    }
                }

                double averageSlotLength = ((double) testers) / slots;

                if (averageSlotLength > 2) {
                    LOGGER.warning("Table Size " + tableSize + " with Seeds " + seed + ": total " + testers
                            + " keys uses " + slots + " hash slots (" + averageSlotLength
                            + "); the hash performance would be bad!");
                } else {
                    LOGGER.info("Table Size " + tableSize + " with Seeds " + seed + ": total " + testers
                            + " keys uses " + slots + " hash slots (" + averageSlotLength + ")");
                }
            }
        }
    }

    public void ByteArrayFamilyTest(IBinaryHashFunctionFamily hfFamily) {

        ByteBuffer bbuf = ByteBuffer.allocate(byteArrayLength);

        for (int i = 0; i < byteArrayLength; i++) {
            bbuf.put(i, (byte) i);
        }

        LOGGER.info("====> Hash Quality Test (Bytes Keys) for " + hfFamily.getClass().getSimpleName());

        for (int tableSize : tableSizes) {

            ByteBuffer statBuf = ByteBuffer.allocate(tableSize);

            for (int seed = 0; seed < 5; seed++) {

                for (int i = 0; i < statBuf.capacity(); i++) {
                    statBuf.put(i, (byte) 0);
                }

                IBinaryHashFunction hashFunction = hfFamily.createBinaryHashFunction(seed);

                int h;

                long time = System.nanoTime();

                int keysCount = 0;

                for (int i = 0; i < byteArrayLength; i++) {
                    for (int j = i; j < byteArrayLength - i; j++) {
                        keysCount++;
                        h = hashFunction.hash(bbuf.array(), i, j) % tableSize;
                        if (h < 0) {
                            h *= -1;
                        }
                        statBuf.put(h, (byte) (statBuf.get(h) + 1));

                    }

                }

                time = System.nanoTime() - time;

                int slots = 0;
                for (int i = 0; i < tableSize; i++) {
                    if (statBuf.get(i) > 0) {
                        slots++;
                    }
                }

                double averageSlotLength = ((double) keysCount) / slots;

                if (averageSlotLength > 2) {
                    LOGGER.warning("Table Size " + tableSize + " with Seeds " + seed + ": total " + keysCount
                            + " keys uses " + slots + " hash slots (" + averageSlotLength
                            + "); the hash performance would be bad!");
                } else {
                    LOGGER.info("Table Size " + tableSize + " with Seeds " + seed + ": total " + keysCount
                            + " keys uses " + slots + " hash slots (" + averageSlotLength + ")");
                }
            }
        }

    }
}
