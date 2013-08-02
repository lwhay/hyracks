package edu.uci.ics.genomix.hadoop.velvetgraphbuilding;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.oldtype.PositionListWritable;
import edu.uci.ics.genomix.oldtype.PositionWritable;
import edu.uci.ics.genomix.type.KmerBytesWritable;

@SuppressWarnings({ "deprecation", "unused" })
public class GraphInvertedIndexBuildingReducer extends MapReduceBase implements
        Reducer<KmerBytesWritable, PositionWritable, KmerBytesWritable, PositionListWritable> {
    PositionListWritable outputlist = new PositionListWritable();
    @Override
    public void reduce(KmerBytesWritable key, Iterator<PositionWritable> values,
            OutputCollector<KmerBytesWritable, PositionListWritable> output, Reporter reporter) throws IOException {
        outputlist.reset();
        while (values.hasNext()) {
            outputlist.append(values.next());
        }
        output.collect(key, outputlist);
    }
}