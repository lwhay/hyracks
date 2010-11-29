package edu.uci.ics.hyracks.examples.onlineaggregation;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class OnlineFileSplit extends FileSplit {

	private long schedule_time;

	public OnlineFileSplit(Path file, long start, long length, String[] hosts,
			long schedule_time) {
		super(file, start, length, hosts);
		this.schedule_time = schedule_time;
	}

	public long blockid() {
		return getStart();
	}

	public long scheduleTime() {
		return this.schedule_time;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeLong(this.schedule_time);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		this.schedule_time = in.readLong();
	}

}
