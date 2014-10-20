/*
 * Copyright 2009-2013 by The Regents of the University of California
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package edu.uci.ics.hyracks.data.std.primitive;

import org.junit.Test;

import static org.junit.Assert.*;

public class ByteArrayPointableTest {


    @Test
    public void testCompareTo() throws Exception {
        byte [] bytes = new byte[] { 1, 2, 3, 4};
        ByteArrayPointable byteArrayPointable = new ByteArrayPointable();
        byteArrayPointable.set(bytes, 0, bytes.length);

        testEqual(byteArrayPointable, new byte[] { 1,2 ,3,4});

        testLessThan(byteArrayPointable, new byte[] {2});
        testLessThan(byteArrayPointable, new byte[] {1,2,3,5});
        testLessThan(byteArrayPointable, new byte[] {1,2,3,4,5});

        testGreaterThan(byteArrayPointable, new byte[] { });
        testGreaterThan(byteArrayPointable, new byte[] { 0});
        testGreaterThan(byteArrayPointable, new byte[] { 1,2,3});
    }


    void testEqual(ByteArrayPointable pointable, byte [] bytes){
        assertTrue(pointable.compareTo(bytes, 0, bytes.length) == 0);
    }

    void testLessThan(ByteArrayPointable pointable, byte[] bytes){
        assertTrue(pointable.compareTo(bytes, 0, bytes.length) < 0);
    }

    void testGreaterThan(ByteArrayPointable pointable, byte[] bytes){
        assertTrue(pointable.compareTo(bytes, 0, bytes.length) > 0);
    }
}