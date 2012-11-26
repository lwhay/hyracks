/**
 * Copyright 2010-2011 The Regents of the University of California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on
 * an "AS IS"; BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under
 * the License.
 * 
 * Author: Alexander Behm <abehm (at) ics.uci.edu>
 */

package edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers;

import java.io.IOException;

import edu.uci.ics.hyracks.data.std.api.IMutableValueStorage;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.util.StringUtils;

public class UTF8WordToken extends AbstractUTF8Token {

    public UTF8WordToken(byte tokenTypeTag, byte countTypeTag) {
        super(tokenTypeTag, countTypeTag);
    }

    @Override
    public void serializeToken(IMutableValueStorage outVal) throws IOException {
        handleTokenTypeTag(outVal.getDataOutput());
        int tokenUTF8LenOff = outVal.getLength();
        int tokenUTF8Len = 0;
        // Write dummy UTF length which will be correctly set later.
        outVal.getDataOutput().writeShort(0);
        int pos = start;
        for (int i = 0; i < tokenLength; i++) {
            char c = Character.toLowerCase(UTF8StringPointable.charAt(data, pos));
            tokenUTF8Len += StringUtils.writeCharAsModifiedUTF8(c, outVal.getDataOutput());
            pos += UTF8StringPointable.charSize(data, pos);
        }
        // Set UTF length of token.
        outVal.getByteArray()[tokenUTF8LenOff] = (byte) ((tokenUTF8Len >>> 8) & 0xFF);
        outVal.getByteArray()[tokenUTF8LenOff + 1] = (byte) ((tokenUTF8Len >>> 0) & 0xFF);
    }
}
