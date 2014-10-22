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
import edu.uci.ics.hyracks.data.std.primitive.ByteArrayPointable;

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
                    int byteLength = str.length() / 2;
                    int strStart = 0;
                    if (str.charAt(0) == 'X' || str.charAt(0) == 'x') {
                        byteLength = (str.length() - 1) / 2;
                        strStart = 1;
                    }

                    byte[] bytes = new byte[byteLength + ByteArrayPointable.SIZE_OF_LENGTH];
                    out.write(extractByteArrayFromValidHexString(str, strStart, bytes,
                            ByteArrayPointable.SIZE_OF_LENGTH));
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
            return 10 + c - 'a';
        }
        return 10 + c - 'A';
    }

    public static byte[] extractByteArrayFromValidHexString(String str, int start, byte[] output, int offset) {
        int len = str.length() - start;

        for (int i = 0; i < len; i += 2) {
            output[offset + i / 2] = (byte) ((getValueFromValidHexChar(str.charAt(start + i)) << 4) +
                    getValueFromValidHexChar(str.charAt(start + i + 1)));
        }
        return output;
    }

}
