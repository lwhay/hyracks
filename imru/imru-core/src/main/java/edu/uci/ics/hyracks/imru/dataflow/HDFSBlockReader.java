package edu.uci.ics.hyracks.imru.dataflow;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.mapred.*;

public class HDFSBlockReader implements RecordReader<LongWritable, Text> {
    private byte[] buf;
    private long start;
    private long pos;
    private long end;
    private FSDataInputStream fileIn;
    private final Seekable filePosition;
    private Decompressor decompressor;

    public HDFSBlockReader(Configuration job, FileSplit split, int bufSize) throws IOException {
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();

        // open the file and seek to the start of the split
        final FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(file);
        fileIn.seek(start);
        filePosition = fileIn;
        this.pos = start;

        buf = new byte[bufSize];
    }

    public LongWritable createKey() {
        return new LongWritable();
    }

    public Text createValue() {
        return new Text();
    }

    private long getFilePosition() throws IOException {
        return pos;
    }

    /** Read a line. */
    public synchronized boolean next(LongWritable key, Text value) throws IOException {
        // We always read one extra line, which lies outside the upper
        // split limit i.e. (end - 1)
        while (getFilePosition() <= end) {
            key.set(pos);
            int len = fileIn.read(buf);
            if (len <= 0)
                return false;
            value.set(buf, 0, len);
            pos += len;
            return true;
        }
        return false;
    }

    /**
     * Get the progress within the split
     */
    public synchronized float getProgress() throws IOException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (getFilePosition() - start) / (float) (end - start));
        }
    }

    public synchronized long getPos() throws IOException {
        return pos;
    }

    public synchronized void close() throws IOException {
        if (fileIn != null)
            fileIn.close();
    }
}
