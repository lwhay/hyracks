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

package edu.uci.ics.hyracks.imru.example.digitRecognition;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

//http://yann.lecun.com/exdb/mnist/
public class MNIST {
    public static byte[][][] readImages(DataInputStream stream)
            throws IOException {
        int magic = stream.readInt();
        int n = stream.readInt();
        int w = stream.readInt();
        int h = stream.readInt();
        byte[][][] bs = new byte[n][h][w];
        for (int i = 0; i < n; i++) {
            for (int j = 0; j < h; j++) {
                int len = stream.read(bs[i][j]);
                if (len != w)
                    throw new Error(n+" "+h+" "+w+" "+len+" "+w);
            }
        }
        stream.close();
        return bs;
    }

    public static byte[] readLabel(DataInputStream stream) throws IOException {
        int magic = stream.readInt();
        int n = stream.readInt();
        byte[] bs = new byte[n];
        int len = stream.read(bs);
        if (len != n)
            throw new Error();
        stream.close();
        return bs;
    }
}
