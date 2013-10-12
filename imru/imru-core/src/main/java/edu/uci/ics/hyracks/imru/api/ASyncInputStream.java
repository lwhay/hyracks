package edu.uci.ics.hyracks.imru.api;

import java.io.IOException;
import java.util.Iterator;

import edu.uci.ics.hyracks.imru.util.Rt;

public class ASyncInputStream extends java.io.InputStream {
    private ASyncIO<byte[]> io;
    Iterator<byte[]> input;
    byte[] bs;
    int pos = 0;

    public ASyncInputStream(ASyncIO<byte[]> io) {
        this.io = io;
        input = io.getInput();
    }

    @Override
    public int read() throws IOException {
        while (bs == null || pos >= bs.length) {
            if (!input.hasNext())
                return -1;
            bs = input.next();
            pos = 0;
        }
        //        Rt.p("read "+(int)bs[pos]);
        return bs[pos++] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int read = 0;
        int left = len;
        all: while (left > 0) {
            while (bs == null || pos >= bs.length) {
                if (!input.hasNext()) {
                    if (read == 0)
                        return -1;
                    else
                        break all;
                }
                bs = input.next();
                pos = 0;
            }
            int available = bs.length - pos;
            if (available >= left) {
                System.arraycopy(bs, pos, b, off, left);
                read += left;
                off += left;
                pos += left;
                left = 0;
            } else {
                System.arraycopy(bs, pos, b, off, available);
                read += available;
                off += available;
                pos += available;
                left -= available;
            }
        }
        //        Rt.p("read "+read);
        return read;
    }
}
