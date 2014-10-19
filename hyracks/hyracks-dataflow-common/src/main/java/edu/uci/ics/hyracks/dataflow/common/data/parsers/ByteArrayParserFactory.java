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

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

import java.io.DataOutput;
import java.io.IOException;

public class ByteArrayParserFactory implements IValueParserFactory {
    public static ByteArrayParserFactory INSTANCE = new ByteArrayParserFactory();

    private ByteArrayParserFactory() {
    }

    @Override public IValueParser createValueParser() {
        return new IValueParser() {
            @Override public void parse(char[] buffer, int start, int length, DataOutput out)
                    throws HyracksDataException {
                String str = String.valueOf(buffer, start, length);
                try {
                    out.write(extractByteArrayFromValidHexString(str));
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }

    public static int getValueFromValidHexChar(char c) {
        if (c >= '0' && c <= '9') {
            return c - '0';
        }
        if (c >= 'a' && c <= 'f') {
            return c - 'a';
        }
        return c - 'A';
    }

    public static final byte[] extractByteArrayFromValidHexString(String str) {
        int len = str.length();
        int start = 0;
        if (len >= 2) {
            if (str.charAt(0) == '0' && (str.charAt(1) == 'x' || str.charAt(1) == 'X') ) {
                start += 2;
                len -= 2;
            }
        }
        byte[] bytes = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            bytes[i / 2] = (byte) (getValueFromValidHexChar(str.charAt(start + i)) << 4 +
                    getValueFromValidHexChar(str.charAt(start + i + 1)));
        }
        return bytes;
    }

}
