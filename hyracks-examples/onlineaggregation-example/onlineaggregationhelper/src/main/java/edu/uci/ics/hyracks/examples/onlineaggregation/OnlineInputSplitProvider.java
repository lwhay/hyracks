package edu.uci.ics.hyracks.examples.onlineaggregation;

import java.util.Queue;

public class OnlineInputSplitProvider implements IInputSplitProvider<OnlineFileSplit> {
	
	private Queue<OnlineFileSplit> queue;
	
	public OnlineInputSplitProvider(Queue<OnlineFileSplit> queue) {
		this.queue = queue;
	}

	@Override
	public OnlineFileSplit next() {
		return this.queue.size() > 0 ? this.queue.remove() : null;
	}

}
