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

package edu.uci.ics.hyracks.dataflow.common.data.parsers;

import edu.uci.ics.hyracks.data.std.primitive.ByteArrayPointable;
import org.junit.Test;

import javax.xml.bind.DatatypeConverter;

import java.util.Arrays;

import static org.junit.Assert.*;

public class ByteArrayParserFactoryTest {

    @Test
    public void testExtractByteArrayFromValidHexString() throws Exception {
        String hexString = "XFFAB99213489";
        byte[] parsed = new byte[hexString.length() / 2 ];
        ByteArrayParserFactory.extractByteArrayFromValidHexString(hexString, 1, parsed, 0);
        assertTrue(Arrays.equals(parsed, DatatypeConverter.parseHexBinary(hexString.substring(1))));

        byte[] parsed2 = new byte[] {};
        ByteArrayParserFactory.extractByteArrayFromValidHexString("", 0, parsed, 0);
        assertTrue(Arrays.equals(parsed2, DatatypeConverter.parseHexBinary("")));


        hexString = hexString.toLowerCase();
        parsed = ByteArrayParserFactory.extractByteArrayFromValidHexString(hexString, 1, parsed, 0);
        assertTrue(Arrays.equals(parsed, DatatypeConverter.parseHexBinary(hexString.substring(1))));

        String mixString = "xFFab9921ccCd";
        parsed = new byte[mixString.length() / 2 ];
        ByteArrayParserFactory.extractByteArrayFromValidHexString(mixString, 1, parsed, 0 );
        assertTrue(Arrays.equals(parsed, DatatypeConverter.parseHexBinary(mixString.substring(1))));
    }
}