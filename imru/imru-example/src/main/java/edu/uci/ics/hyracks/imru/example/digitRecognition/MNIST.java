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
