package edu.uci.ics.hyracks.api.dataflow.value;

public class NullLinearizeComparatorFactory implements ILinearizeComparatorFactory {

	private static final long serialVersionUID = 1L;
	private int dim;
	
	public NullLinearizeComparatorFactory(int dim) {
		this.dim = dim;
	}

	public static NullLinearizeComparatorFactory get(int dim) {
        return new NullLinearizeComparatorFactory(dim);
    }

	@Override
	public ILinearizeComparator createBinaryComparator() {
		return new ILinearizeComparator() {
			
			@Override
			public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
				return 0;
			}

			@Override
			public int getDimensions() {
				return dim;
			}
		};
	}

}
