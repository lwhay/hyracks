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
import edu.uci.ics.hyracks.dataflow.common.data.util.StringUtils;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public class ByteArrayParserFactory implements IValueParserFactory {
    public static ByteArrayParserFactory INSTANCE = new ByteArrayParserFactory();

    private ByteArrayParserFactory() {
    }

    @Override public IValueParser createValueParser() {
        return new IValueParser() {
            private byte[] cache = new byte[] { };

            @Override public void parse(char[] buffer, int start, int length, DataOutput out)
                    throws HyracksDataException {
                String str = String.valueOf(buffer, start, length);
                try {
                    cache = extractPointableArrayFromHexString(str, cache);
                    int validLength = ByteArrayPointable.getFullLength(cache, 0);
                    out.write(cache, 0, validLength);
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        };
    }

    public static boolean isValidBinaryLiteral(String unquoted) {
        int start = 0;
        if (unquoted.charAt(0) == 'X' || unquoted.charAt(0) == 'x') {
            start += 1;
        }
        for (int i = start; i < unquoted.length(); ++i) {
            if (unquoted.charAt(i) >= '0' && unquoted.charAt(i) <= '9'
                    || unquoted.charAt(i) >= 'a' && unquoted.charAt(i) <= 'f'
                    || unquoted.charAt(i) >= 'A' && unquoted.charAt(i) <= 'F') {
                continue;
            }
            return false;
        }
        return (unquoted.length() - start) % 2 == 0;
    }

    public static byte[] extractPointableArrayFromHexString(String str, byte[] cacheNeedToReset) {
        int byteLength = str.length() / 2;
        int strStart = 0;
        if (str.charAt(0) == 'X' || str.charAt(0) == 'x') {
            byteLength = (str.length() - 1) / 2;
            strStart = 1;
        }
        cacheNeedToReset = ensureCapacity(byteLength + ByteArrayPointable.SIZE_OF_LENGTH, cacheNeedToReset);
        StringUtils.extractByteArrayFromValidHexString(str, strStart, cacheNeedToReset,
                ByteArrayPointable.SIZE_OF_LENGTH);
        ByteArrayPointable.putLength(byteLength, cacheNeedToReset, 0);
        return cacheNeedToReset;
    }

    private static byte[] ensureCapacity(int capacity, byte[] original) {
        if (original.length < capacity) {
            return Arrays.copyOf(original, capacity);
        }
        return original;
    }

}
