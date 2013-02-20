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

package edu.uci.ics.hyracks.imru.example.trainmerge.ann;

import java.awt.image.BufferedImage;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

import javax.imageio.ImageIO;

import edu.uci.ics.hyracks.imru.util.Rt;

//http://yann.lecun.com/exdb/mnist/
public class MNIST {
    public static void fill(byte[] bs, InputStream stream) throws IOException {
        int pos = 0;
        int len = bs.length;
        while (pos < len) {
            int l = stream.read(bs, pos, len - pos);
            if (l <= 0)
                throw new IOException("Can't fill " + pos + " " + len);
            pos += l;
            len -= l;
        }
    }

    public static byte[][][] readImages(DataInputStream stream, int start,
            int len) throws IOException {
        int magic = stream.readInt();
        int n = stream.readInt();
        int w = stream.readInt();
        int h = stream.readInt();
        if (start + len > n)
            len = n - start;
        byte[][][] bs = new byte[len][h][w];
        if (start > 0)
            stream.skip(w * h * start);
        for (int i = 0; i < len; i++) {
            for (int j = 0; j < h; j++) {
                fill(bs[i][j], stream);
            }
        }
        stream.close();
        return bs;
    }

    public static void write(byte[][] bs, File file) throws IOException {
        BufferedImage d = new BufferedImage(28, 28, BufferedImage.TYPE_INT_RGB);
        for (int y = 0; y < 28; y++) {
            for (int x = 0; x < 28; x++) {
                int gray = bs[y][x] & 0xFF;
                if (gray < 0 || gray > 255)
                    throw new Error();
                gray = 255 - gray;
                gray *= 0x010101;
                d.setRGB(x, y, gray);
            }
        }
        ImageIO.write(d, "png", file);
    }

    public static byte[] readLabel(DataInputStream stream, int start, int len)
            throws IOException {
        int magic = stream.readInt();
        int n = stream.readInt();
        if (start + len > n)
            len = n - start;
        byte[] bs = new byte[len];
        stream.skip(start);
        fill(bs, stream);
        stream.close();
        return bs;
    }
}
