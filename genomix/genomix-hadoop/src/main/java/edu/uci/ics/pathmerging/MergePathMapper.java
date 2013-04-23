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
package edu.uci.ics.pathmerging;

import java.io.IOException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import edu.uci.ics.genomix.type.GeneCode;
import edu.uci.ics.genomix.type.KmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritable;
import edu.uci.ics.genomix.type.VKmerBytesWritableFactory;

@SuppressWarnings("deprecation")
public class MergePathMapper extends MapReduceBase implements
        Mapper<KmerBytesWritable, MergePathValueWritable, KmerBytesWritable, MergePathValueWritable> {
    private int KMER_SIZE;
    private VKmerBytesWritableFactory outputKmerFactory;
    private MergePathValueWritable outputAdjList; 
    private VKmerBytesWritable tmpKmer;
    private VKmerBytesWritable outputKmer;

    public void configure(JobConf job) {
        KMER_SIZE = job.getInt("sizeKmer", 0);
        outputKmerFactory = new VKmerBytesWritableFactory(KMER_SIZE);
        outputAdjList = new MergePathValueWritable();
        tmpKmer = new VKmerBytesWritable(KMER_SIZE);
        outputKmer = new VKmerBytesWritable(KMER_SIZE);
    }

    @Override
    public void map(KmerBytesWritable key, MergePathValueWritable value,
            OutputCollector<KmerBytesWritable, MergePathValueWritable> output, Reporter reporter) throws IOException {

        byte precursor = (byte) 0xF0;
        byte succeed = (byte) 0x0F;
        byte adjBitMap = value.getAdjBitMap();
        byte bitFlag = value.getFlag();
        precursor = (byte) (precursor & adjBitMap);
        precursor = (byte) ((precursor & 0xff) >> 4);
        succeed = (byte) (succeed & adjBitMap);

        if (bitFlag == 1) {
            byte succeedCode = GeneCode.getGeneCodeFromBitMap(succeed);
            tmpKmer.set(outputKmerFactory.getLastKmerFromChain(KMER_SIZE, key));
            outputKmer.set(outputKmerFactory.shiftKmerWithNextCode(tmpKmer, succeedCode));

            KmerBytesWritable mergedKmer = outputKmerFactory.getFirstKmerFromChain(value.getKmerSize()
                    - (KMER_SIZE - 1), value.getKmer());
            outputAdjList.set(mergedKmer, adjBitMap, bitFlag);
            output.collect(outputKmer, outputAdjList);
        } else {
            outputAdjList.set(value);
            output.collect(key, outputAdjList);
        }
    }
}