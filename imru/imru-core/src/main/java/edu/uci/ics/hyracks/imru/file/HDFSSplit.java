package edu.uci.ics.hyracks.imru.file;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import edu.uci.ics.hyracks.imru.base.IConfigurationFactory;

/**
 * Separate hadoop classes to avoid uploading
 * hadoop.jar when HDFS is not used.
 * Doesn't work as expected.
 * 
 * @author wangrui
 */
public class HDFSSplit implements Serializable {
    String path;
    String[] locations;

    public HDFSSplit(InputSplit split) throws IOException, InterruptedException {
        this.path = split.toString();
        this.locations = split.getLocations();
    }

    public HDFSSplit(String path) throws IOException {
        this.path=path;
    }

    public String[] getLocations() throws IOException {
        return locations;
    }

    public static List<IMRUFileSplit> get(List<InputSplit> splits) throws IOException, InterruptedException {
        Vector<IMRUFileSplit> list = new Vector<IMRUFileSplit>(splits.size());
        for (InputSplit split : splits) {
            list.add(new IMRUFileSplit(new HDFSSplit(split)));
        }
        return list;
    }

    public InputStream getInputStream(IConfigurationFactory confFactory) throws IOException {
        Path path = new Path(this.path);
        Configuration conf = confFactory.createConfiguration();
        FileSystem dfs = FileSystem.get(conf);
        return HDFSUtils.open(dfs, conf, path);
    }
}
