package edu.uci.ics.hyracks.control.nc.comm;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.resources.IDeallocatable;

public class Run implements IDeallocatable {
    private final File file;
    private final FileChannel channel;
    private long writeFP;
    private boolean eof;

    public Run(File file) throws HyracksDataException {
        this.file = file;
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            channel = raf.getChannel();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        writeFP = 0;
        eof = false;
    }

    public synchronized void write(ByteBuffer frame) throws HyracksDataException {
        try {
            int len = frame.capacity();
            while (len > 0) {
                int sz = channel.write(frame, writeFP);
                if (sz < 0) {
                    throw new HyracksDataException("Error writing to run");
                }
                len -= sz;
                writeFP += sz;
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        notifyAll();
    }

    public synchronized void close() {
        eof = true;
        try {
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        notifyAll();
    }

    @Override
    public void deallocate() {
        try {
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        file.delete();
    }

    public class Reader {
        private final FileChannel channel;
        private long readFP;

        public Reader() throws HyracksDataException {
            try {
                RandomAccessFile raf = new RandomAccessFile(file, "r");
                channel = raf.getChannel();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
            readFP = 0;
        }

        public boolean read(ByteBuffer buffer) throws HyracksDataException {
            synchronized (Run.this) {
                while (!eof && readFP >= writeFP) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                if (eof && readFP >= writeFP) {
                    return false;
                }
                try {
                    channel.position(readFP);
                    buffer.clear();
                    int len = buffer.capacity();
                    while (len > 0) {
                        int sz = channel.read(buffer, readFP);
                        if (sz < 0) {
                            throw new HyracksDataException("Error reading file");
                        }
                        len -= sz;
                        readFP += sz;
                    }
                    return true;
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }
        }

        public void close() throws HyracksDataException {
            try {
                channel.close();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }
    }
}