package edu.uci.ics.hyracks.examples.onlineaggregation.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import edu.uci.ics.hyracks.examples.onlineaggregation.sample.wordcount.StateCountMapper;
import edu.uci.ics.hyracks.examples.onlineaggregation.sample.wordcount.WordCountReducer;

public class StateCountJobGen {
	    public static void main(String[] args) throws Exception {
	        Configuration conf = new Configuration();
	        Job job = new Job(conf, "state count");
	        job.setMapperClass(StateCountMapper.class);
	        job.setCombinerClass(WordCountReducer.class);
	        job.setReducerClass(WordCountReducer.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(IntWritable.class);
	        FileInputFormat.addInputPath(job, new Path(args[0]));
	        FileOutputFormat.setOutputPath(job, new Path(args[1]));

	        job.getConfiguration().writeXml(System.out);
	    }
}
