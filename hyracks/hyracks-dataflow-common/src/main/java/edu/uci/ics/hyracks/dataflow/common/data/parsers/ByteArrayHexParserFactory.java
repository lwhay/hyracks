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
import java.util.Arrays;

public class ByteArrayHexParserFactory implements IValueParserFactory {
    public static ByteArrayHexParserFactory INSTANCE = new ByteArrayHexParserFactory();

    private ByteArrayHexParserFactory() {
    }

    @Override public IValueParser createValueParser() {
        return new IValueParser() {
            private byte[] buffer = new byte[] { };

            @Override public void parse(char[] input, int start, int length, DataOutput out)
                    throws HyracksDataException {
                try {
                    if (!isValidHexBinaryString(input, start, length)) {
                        throw new HyracksDataException(
                                "Invalid hex string for binary type: " + new String(input, start, length));
                    }
                    buffer = extractPointableArrayFromHexString(input, start, length, buffer);
                    out.write(buffer, 0, ByteArrayPointable.getFullLength(buffer, 0));
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }

    public static boolean isValidHexBinaryString(char[] input, int start, int length) {
        for (int i = 0; i < length; ++i) {
            if (input[start + i] >= '0' && input[start + i] <= '9'
                    || input[start + i] >= 'a' && input[start + i] <= 'f'
                    || input[start + i] >= 'A' && input[start + i] <= 'F') {
                continue;
            }
            return false;
        }
        return length % 2 == 0;
    }

    public static byte[] extractPointableArrayFromHexString(char[] input, int start, int length,
            byte[] cacheNeedToReset) throws HyracksDataException {
        int byteLength = length / 2;
        cacheNeedToReset = ensureCapacity(byteLength + ByteArrayPointable.SIZE_OF_LENGTH, cacheNeedToReset);
        extractByteArrayFromValidHexString(input, start, length, cacheNeedToReset,
                ByteArrayPointable.SIZE_OF_LENGTH);
        if (byteLength >= ByteArrayPointable.MAX_LENGTH) {
            throw new HyracksDataException("The decoded byte array is too long.");
        }
        ByteArrayPointable.putLength(byteLength, cacheNeedToReset, 0);
        return cacheNeedToReset;
    }

    public static byte[] ensureCapacity(int capacity, byte[] original) {
        if (original == null) {
            return new byte[capacity];
        }
        if (original.length < capacity) {
            return Arrays.copyOf(original, capacity);
        }
        return original;
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

    public static void extractByteArrayFromValidHexString(char[] input, int start, int length, byte[] output,
            int offset) {

        for (int i = 0; i < length; i += 2) {
            output[offset + i / 2] = (byte) ((getValueFromValidHexChar(input[start + i]) << 4) +
                    getValueFromValidHexChar(input[start + i + 1]));
        }
    }
}
