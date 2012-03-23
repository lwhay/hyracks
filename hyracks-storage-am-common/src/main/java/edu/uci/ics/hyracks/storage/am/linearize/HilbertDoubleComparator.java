package edu.uci.ics.hyracks.storage.am.linearize;

import java.nio.ByteBuffer;
import java.nio.DoubleBuffer;
import java.util.Stack;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;

public class HilbertDoubleComparator implements IBinaryComparator {
	private final int dim; // dimension
	private final HilbertState[] states;
	
	private double[] bounds;
	private double stepsize;
	private int state;
	private Stack<Integer> stateStack = new Stack<Integer>();
	private Stack<double[]> boundsStack = new Stack<double[]>();
	
	private int DEBUG_comps = 0;
	
	private class HilbertState {
		public final int[] nextState;
		public final int[] position;
		
		public HilbertState(int[] nextState, int[] order) {
			this.nextState = nextState;
			this.position = order;
		}
	}
	
	public HilbertDoubleComparator(int dimension) {
		if(dimension != 2) throw new IllegalArgumentException();
		dim = dimension;
		
		states = new HilbertState[] {
				new HilbertState(new int[] {3,0,1,0}, new int[]{0,1,3,2}),
				new HilbertState(new int[] {1,1,0,2}, new int[]{2,1,3,0}),
				new HilbertState(new int[] {2,3,2,1}, new int[]{2,3,1,0}),
				new HilbertState(new int[] {0,2,3,3}, new int[]{0,3,1,2})
		};
		
		resetStateMachine();
	}
	
	private void resetStateMachine() {
		state = 0;
		stateStack.clear();
		stateStack.push(state);
		stepsize = Double.MAX_VALUE / 2;
		bounds = new double[dim];
		boundsStack.clear();
		boundsStack.push(bounds);
	}
	
	public int compare(double[] ds, double[] ds2) {
		boolean equal = true;
		for(int i = 0; i < dim; i++) {
			// for some reason, ds.equals(ds2) does not work -.-
			// FIXME double comparison can be wrong
			if(ds[i] != ds2[i]) equal = false;
		}
		if(equal) return 0;
		
		// We keep the state of the state machine after a comparison. In most cases,
		// the needed zoom factor is close to the old one. In this step, we check if we have
		// to zoom out

		while(true) {
			if(stateStack.size() == 1) {
				resetStateMachine();
				break;
			}
			boolean zoomOut = false;
			for(int i = 0; i < dim; i++) {
				if(ds[i] <= bounds[i] - stepsize || ds[i] >= bounds[i] + stepsize ||
						ds2[i] <= bounds[i] - stepsize || ds2[i] >= bounds[i] + stepsize) {
					state = stateStack.pop();
					bounds = boundsStack.pop();
					stepsize *= 2;
					zoomOut = true;
					break;
				}
			}
			if(!zoomOut) break;
		}
		
		while(true) {
			// Find the quadrant in which A and B are
			int quadrantA = 0, quadrantB = 0;
			for(int i = dim-1; i >= 0; i--) {
				if(ds[i] >= bounds[i]) quadrantA ^= (1 << (dim - i - 1));
				if(ds2[i] >= bounds[i]) quadrantB ^= (1 << (dim - i - 1));
				
				if(ds[i] >= bounds[i]) {
					boundsStack.push(bounds);
					bounds[i] += stepsize;
				} else {
					boundsStack.push(bounds);
					bounds[i] -= stepsize;
				}
			}
			stepsize /= 2;
			
			if(quadrantA != quadrantB) {
				// find the position of A and B's quadrants
				int posA = states[state].position[quadrantA];
				int posB = states[state].position[quadrantB];
				
				if(posA < posB) return -1; else return 1;
			}
						
			stateStack.push(state);
			state = states[state].nextState[quadrantA];
		}
	}
	
	
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		DoubleBuffer aIB = ByteBuffer.wrap(b1, s1, l1 * 2).asDoubleBuffer();
		DoubleBuffer bIB = ByteBuffer.wrap(b2, s2, l2 * 2).asDoubleBuffer();
		double[] a = new double[aIB.remaining()];
		double[] b = new double[bIB.remaining()];
		for(int i = 0; i < aIB.remaining() && i < dim; i++) {
			a[i] = aIB.get();
			b[i] = bIB.get();
		}
		
		DEBUG_comps++;
		if(DEBUG_comps % 1000000 == 0) System.out.println(DEBUG_comps + " comparisons");
		
		return compare(a, b);
	}
}
