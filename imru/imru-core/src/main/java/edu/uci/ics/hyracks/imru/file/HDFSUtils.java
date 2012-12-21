package edu.uci.ics.hyracks.imru.file;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

/**
 * Utility functions for working with HDFS.
 *
 * @author Josh Rosen
 */
public class HDFSUtils {
    /**
     * Open a file in HDFS for reading, performing automatic
     * decompression as necessary.
     *
     * @param dfs
     *            The HDFS file system object.
     * @param conf
     *            The HDFS configuration.
     * @param path
     *            The path to the file.
     * @return An InputStream for reading the file.
     * @throws IOException
     */
    public static InputStream open(FileSystem dfs, Configuration conf, Path path) throws IOException {
        FSDataInputStream fin = dfs.open(path);
        CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(conf);
        final CompressionCodec codec = compressionCodecs.getCodec(path);
        if (codec != null) {
            return codec.createInputStream(fin);
        } else {
            return fin;
        }
    }
}
