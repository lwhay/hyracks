package edu.uci.ics.testselect;

import java.io.File;

import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IDataSink;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.FileSplitDomain;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.IPartitioningProperty;
import edu.uci.ics.hyracks.algebricks.core.algebra.properties.RandomPartitioningProperty;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

public class TestDataSink implements IDataSink {
	private String file;

	private FileSplit[] fileSplits;

	private IPartitioningProperty partProp;

	public TestDataSink(String file) {
		this.file = file;
		fileSplits = parseFileSplits(file);
		partProp = new RandomPartitioningProperty(new FileSplitDomain(
				fileSplits));
	}

	@Override
	public Object getId() {
		return file;
	}

	public FileSplit[] getFileSplits() {
		return fileSplits;
	}

	@Override
	public Object[] getSchemaTypes() {
		return null;
	}

	@Override
	public IPartitioningProperty getPartitioningProperty() {
		return partProp;
	}

	public static FileSplit[] parseFileSplits(String fileSplits) {
		String[] splits = fileSplits.split(",");
		FileSplit[] fSplits = new FileSplit[splits.length];
		for (int i = 0; i < splits.length; ++i) {
			String s = splits[i].trim();
			int idx = s.indexOf(':');
			if (idx < 0) {
				throw new IllegalArgumentException("File split " + s
						+ " not well formed");
			}
			fSplits[i] = new FileSplit(s.substring(0, idx), new FileReference(
					new File(s.substring(idx + 1))));
		}
		return fSplits;
	}
}
