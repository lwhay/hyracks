/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.graphbuilding;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * This class implement reducer operator of mapreduce model
 */
@SuppressWarnings("deprecation")
public class GenomixReducer extends MapReduceBase implements
        Reducer<KmerBytesWritable, AdjacentWritable, KmerBytesWritable, AdjacentWritable> {
    AdjacentWritable valWriter = new AdjacentWritable();

    @Override
    public void reduce(KmerBytesWritable key, Iterator<AdjacentWritable> values,
            OutputCollector<KmerBytesWritable, AdjacentWritable> output, Reporter reporter) throws IOException {
        byte groupByAdjList = 0;
        int count = 0;
        byte bytCount = 0;
        while (values.hasNext()) {
            //Merge By the all adjacent Nodes;
            AdjacentWritable geneValue = values.next();
            groupByAdjList = (byte) (groupByAdjList | geneValue.getFirst());
            count = count + (int) geneValue.getSecond();
        }
        if (count >= 127)
            bytCount = (byte) 127;
        else
            bytCount = (byte) count;
        valWriter.set(groupByAdjList, bytCount);
        output.collect(key, valWriter);
    }
}
