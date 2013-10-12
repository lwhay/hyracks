package edu.uci.ics.hyracks.imru.dataflow;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public class HDFSBlockFormat extends FileInputFormat<LongWritable, Text> implements JobConfigurable {
    public void configure(JobConf conf) {
    }

    protected boolean isSplitable(FileSystem fs, Path file) {
        return true;
    }

    public RecordReader<LongWritable, Text> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter)
            throws IOException {
        reporter.setStatus(genericSplit.toString());
        String delimiter = job.get("textinputformat.record.delimiter");
        return new HDFSBlockReader(job, (FileSplit) genericSplit, 65536);
    }
}