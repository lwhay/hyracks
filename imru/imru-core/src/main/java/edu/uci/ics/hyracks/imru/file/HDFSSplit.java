package edu.uci.ics.hyracks.imru.file;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
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
public class HDFSSplit {
    FileSplit split;

    public HDFSSplit(InputSplit split) {
        this.split = (FileSplit) split;
    }

    public HDFSSplit(DataInput input) throws IOException {
        split.readFields(input);
    }

    public void write(DataOutput output) throws IOException {
        split.write(output);
    }

    public String[] getLocations() throws IOException {
        return split.getLocations();
    }

    public static List<IMRUFileSplit> get(List<InputSplit> splits) {
        Vector<IMRUFileSplit> list = new Vector<IMRUFileSplit>(splits.size());
        for (InputSplit split : splits) {
            list.add(new IMRUFileSplit(new HDFSSplit(split)));
        }
        return list;
    }

    public InputStream getInputStream(IConfigurationFactory confFactory) throws IOException {
        Path path = split.getPath();
        Configuration conf = confFactory.createConfiguration();
        FileSystem dfs = FileSystem.get(conf);
        return HDFSUtils.open(dfs, conf, path);
    }
}
