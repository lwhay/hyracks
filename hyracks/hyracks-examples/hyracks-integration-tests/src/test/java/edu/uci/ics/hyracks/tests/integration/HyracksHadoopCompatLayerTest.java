/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.tests.integration;

import java.io.File;
import java.net.Inet4Address;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.Test;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.*;

import edu.uci.ics.hyracks.tests.integration.AbstractIntegrationTest;
import edu.uci.ics.hyracks.control.nc.work.StartTasksWork;
import edu.uci.ics.hyracks.hadoop.compat.driver.HyracksHadoopCompatLayer;
import edu.uci.ics.hyracks.hadoop.compat.driver.CompatibilityLayer;

public class HyracksHadoopCompatLayerTest extends AbstractIntegrationTest {
    private static final Logger LOGGER = Logger.getLogger(HyracksHadoopCompatLayerTest.class.getName()); 
    
    @Test
    public void runCompatibilityTest() throws Exception {
    	try {
    	    // simple sample examples (new api)
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/nwc.jar", "edu.uci.hadoop.newexamples.NWC", "data/in", "data/out"}); // mappper | combiner | reducer
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/nm1.jar", "edu.uci.hadoop.newexamples.NM1", "data/in", "data/out"}); // mappper | no combiner | no reducer | no setNumReduceTasks //->14 words in one file (part-r...) 
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/nm2.jar", "edu.uci.hadoop.newexamples.NM2", "data/in", "data/out"}); // mappper | no combiner | no reducer | setNumReduceTasks //-> 9 + 5 words in two files (part-m...)
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/nmc1.jar", "edu.uci.hadoop.newexamples.NMC1", "data/in", "data/out"}); // mappper | combiner | no reducer | no setNumReduceTasks
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/nmc2.jar", "edu.uci.hadoop.newexamples.NMC2", "data/in", "data/out"}); // mappper | combiner | no reducer | setNumReduceTasks
  
    	    // simple sample examples (old api)
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/owc.jar", "edu.uci.hadoop.newexamples.OWC", "in", "out"}); // mappper | combiner | reducer --> real old wordcount example
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/om1.jar", "edu.uci.hadoop.newexamples.OM1", "in", "out"}); // mappper | no combiner | no reducer | no setNumReduceTasks
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/om2.jar", "edu.uci.hadoop.newexamples.OM2", "in", "out"}); // mappper | no combiner | no reducer | setNumReduceTasks
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/omc1.jar", "edu.uci.hadoop.newexamples.OMC1", "in", "out"}); // mappper | combiner | no reducer | no setNumReduceTasks
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/omc2.jar", "edu.uci.hadoop.newexamples.OMC2", "in", "out"}); // mappper | combiner | no reducer | setNumReduceTasks
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/or.jar", "edu.uci.hadoop.newexamples.OR", "in", "out"}); // no mappper | no combiner | reducer 

            //with big input file
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-examples-public.jar", "wordcount", "tpch/in/lineitem/lineitem.tbl.1", "out"});

    	    //pigmix queries - new API
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixNewAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L1", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-new/", "4"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixNewAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L2", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-new/", "4"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixNewAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L3", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-new/", "4"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixNewAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L4", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-new/", "4"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixNewAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L5", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-new/", "4"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixNewAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L6", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-new/", "4"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixNewAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L7", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-new/", "4"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixNewAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L8", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-new/", "4"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixNewAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L9", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-new/", "4"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixNewAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L10", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-new/", "4"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixNewAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L11", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-new/", "4"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixNewAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L12", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-new/", "4"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixNewAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L13", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-new/", "4"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixNewAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L14", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-new/", "4"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixNewAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L15", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-new/", "4"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixNewAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L16", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-new/", "4"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixNewAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L17", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-new/", "4"});
    	    
    	    //pigmix queries - old API
//            org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixOldAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L1", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-old/", "4"});
//            org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixOldAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L2", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-old/", "4"});
//            org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixOldAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L3", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-old/", "4"});
//            org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixOldAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L4", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-old/", "4"});
//            org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixOldAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L5", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-old/", "4"});
//            org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixOldAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L6", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-old/", "4"});
//            org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixOldAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L7", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-old/", "4"});
//            org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixOldAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L8", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-old/", "4"});
//            org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixOldAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L9", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-old/", "4"});
//            org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixOldAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L10", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-old/", "4"});
//            org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixOldAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L11", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-old/", "4"});
//            org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixOldAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L12", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-old/", "4"});
//            org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixOldAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L13", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-old/", "4"});
//            org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixOldAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L14", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-old/", "4"});
//            org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixOldAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L15", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-old/", "4"});
//            org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixOldAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L16", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-old/", "4"});
//            org.apache.hadoop.util.RunJar.main(new String[] {"data/pigmixOldAPI/pigmix.jar", "org.apache.pig.test.pigmix.mapreduce.L17", "/user/pig/tests/data/pigmix2", "output-data2-hyracks-old/", "4"});
    	    
    	    // hadoop-examples in v2.2.0 (new api)
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-mapreduce-examples-2.2.0.jar", "wordcount", "hyracks/rantxtwriter/in", "hyracks/wordcount"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-mapreduce-examples-2.2.0.jar", "wordmean", "hyracks/rantxtwriter/in", "hyracks/wordmean"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-mapreduce-examples-2.2.0.jar", "wordmedian", "hyracks/rantxtwriter/in", "hyracks/wordmedian"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-mapreduce-examples-2.2.0.jar", "grep", "in", "grep", ".*Bye.*"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-mapreduce-examples-2.2.0.jar", "secondarysort",  "data/secondarySortInput", "data/secondarySortOutput"});
//    		org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-mapreduce-examples-2.2.0.jar", "teragen", "100", "tera/in"});		// 1. not to be sorted 3. exception should be occurred when output dir exist
//    		org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-mapreduce-examples-2.2.0.jar", "terasort","tera/in", "tera/out"}); 
//    		org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-mapreduce-examples-2.2.0.jar", "terasort", "-D", "mapreduce.terasort.simplepartitioner=true", "tera/in", "tera/out"});
//    		org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-mapreduce-examples-2.2.0.jar", "teravalidate","tera/out", "tera/val"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-mapreduce-examples-2.2.0.jar", "randomtextwriter", "-D", "mapreduce.randomtextwriter.bytespermap=3000000", "-D", "mapreduce.randomtextwriter.mapsperhost=5", "hyracks/rantxtwriter/in"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-mapreduce-examples-2.2.0.jar", "randomwriter", "-D", "test.randomwrite.bytes_per_map=400", "-D", "test.randomwriter.maps_per_host=2", "randomwrite/in"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-mapreduce-examples-2.2.0.jar", "pi", "16", "1000"});		
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-mapreduce-examples-2.2.0.jar", "sort", "-inFormat", "org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat", "-outFormat", "org.apache.hadoop.mapreduce.lib.output.TextOutputFormat", "-outKey", "org.apache.hadoop.io.Text", "-outValue", "org.apache.hadoop.io.Text", "in", "out"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-mapreduce-examples-2.2.0.jar", "bbp", "1", "100", "2", "hyracks/bbp"});    	    
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-mapreduce-examples-2.2.0.jar", "aggregatewordcount", "hyracks/rantxtwriter/in", "hyracks/aggreagewordcount"});
//    	    org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-mapreduce-examples-2.2.0.jar", "aggregatewordhist", "hyracks/rantxtwriter/in", "hyracks/aggregatewordhist"});
    	    
            //hadoop-examples old api
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-0.20.2-examples.jar", "wordcount", "in", "out"});             //-- new API
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/owc.jar", "edu.uci.hadoop.newexamples.OWC", "in", "ooo"}); // PASS - mappper | combiner | reducer --> real old wordcount example
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-0.20.2-examples.jar", "sort"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-0.20.2-examples.jar", "grep", "in", "grepout", ".*Bye.*"});   //-- new API
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-0.20.2-examples.jar", "teragen", "100", "oldtera2/in"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-0.20.2-examples.jar", "terasort","oldtera/in", "oldtera/out"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-examples-public.jar", "teravalidate","oldtera/out", "oldtera/val"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-0.20.2-examples.jar", "randomtextwriter", "randomtxt/in"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-0.20.2-examples.jar", "randomtextwriter", "-D", "test.randomtextwrite.bytes_per_map=300", "-D", "test.randomtextwrite.maps_per_host=3", "randomtxt/in"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-0.20.2-examples.jar", "randomwriter", "rand/in"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-0.20.2-examples.jar", "randomwriter", "-D", "test.randomwrite.bytes_per_map=400", "-D", "test.randomwriter.maps_per_host=2", "rand/in"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-0.20.2-examples.jar", "secondarysort", "snd/in", "snd/out"});
            
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-0.20.2-examples.jar", "sleep", "-m", "2", "-r", "1", "-mt", "1000", "-rt", "1000"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-0.20.2-examples.jar", "piestimator"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-0.20.2-examples.jar", "multifilewordcount"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-0.20.2-examples.jar", "join"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-0.20.2-examples.jar", "dbcountpageview"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-0.20.2-examples.jar", "aggregatewordhistogram"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-0.20.2-examples.jar", "aggregatewordcount"});
            
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-examples-1.2.1.jar", "wordcount", "in", "out"}); // wordcount sample is not using old api // PASS         
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-examples-1.2.1.jar", "sort"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-examples-1.2.1.jar", "grep", "in", "out", ".*Bye.*"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-examples-1.2.1.jar", "teragen", "100", "tera121/in"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-examples-1.2.1.jar", "terasort","tera121/in", "tera121/out"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-examples-1.2.1.jar", "randomtextwriter", "1000", "tera/in"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-examples-1.2.1.jar", "randomwriter", "rand/in"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-examples-1.2.1.jar", "randomwriter", "-D", "test.randomwrite.bytes_per_map=400", "-D", "test.randomwriter.maps_per_host=2", "rand/in"});

//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-examples-1.2.1.jar", "compress","/in", "/comp"}); // 1. change result file name from part-r-... to part-m-... 2. result is different when multiple input files
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-examples-1.2.1.jar", "randomtextwriter", "-D", "test.randomtextwrite.bytes_per_map=300", "-D", "test.randomtextwrite.maps_per_host=3", "randtxt/in"});
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/hadoop-examples-1.2.1.jar", "randomwriter", "-D", "test.randomwrite.bytes_per_map=400", "-D", "test.randomwriter.maps_per_host=2", "randomwrite/in"});

            //fuzzyjoinlp
//          org.apache.hadoop.util.RunJar.main(new String[] {"data/fuzzyjoin-hadoop-0.0.2-SNAPSHOT.jar", "recordbuild", "-conf", "data/dblp.quickstart.xml"});
            
            //ICDE2011
            //- k-means clustering
            //- tpc-h style query
            //- set-similarity join
            
            // HiBench Series - sort, wordcount, dfsio, nutchindexing, pagerank, mahout bayesian classification, mahout k-means clustering (witsssssssdfdh compression test)
            // sort - prepare
//          int num_maps = 8;
//          int num_reds = 12;
//          int data_size = 240000000;
//          org.apache.hadoop.util.RunJar.main(new String[] {"jars/hadoop-examples-public.jar", "randomtextwriter", "-Dtest.randomtextwrite.bytes_per_map=" + (data_size / num_maps), "-Dtest.randomtextwrite.maps_per_host=" + num_maps, "-Dmapred.output.compress=false", "rtw"});
            
            
            //wordcount - prepare
//          int num_maps = 2;
//          int data_size = 3200000;
//          org.apache.hadoop.util.RunJar.main(new String[] {"jars/hadoop-examples-public.jar", "randomtextwriter", "-Dtest.randomtextwrite.bytes_per_map=" + (data_size / num_maps), "-Dtest.randomtextwrite.maps_per_host="+num_maps, "input"});
            
            //dfsio - prepare   
//          Integer rd_num_files = 4;
//          Integer rd_file_size = 20;
//          Integer buffer_size = 4096;
//          org.apache.hadoop.util.RunJar.main(new String[] {"jars/datatools.jar", "org.apache.hadoop.fs.dfsioe.TestDFSIOEnh", "-write", "-skipAnalyze", "-nrFiles", rd_num_files.toString(), "-fileSize", rd_file_size.toString(), "-bufferSize", buffer_size.toString() });

            //dfsio - run read
//          buffer_size = 131072;
//          org.apache.hadoop.util.RunJar.main(new String[] {"jars/datatools.jar", "org.apache.hadoop.fs.dfsioe.TestDFSIOEnh", "-read", "-nrFiles", rd_num_files.toString(),
//              "-fileSize", rd_file_size.toString(), "-bufferSize", buffer_size.toString(), "-plotInteval", "1000", 
//              "-sampleUnit", "m", "-sampleInteval", "200", "-sumThreshold", "0.5", "-tputReportTotal", "-resFile", "/result_read.txt", "tputFile", "/ghroughput_read.csv" });

            //dfsio - run write
//           OPTION="-write -nrFiles ${WT_NUM_OF_FILES} -fileSize ${WT_FILE_SIZE} -bufferSize 4096 -plotInteval 1000 -sampleUnit m -sampleInteval 200 -sumThreshold 0.5 -tputReportTotal"
//          ${OPTION} -resFile ${DIR}/result_write.txt -tputFile ${DIR}/throughput_write.csv
//          org.apache.hadoop.util.RunJar.main(new String[] {"jars/datatools.jar", "org.apache.hadoop.fs.dfsioe.TestDFSIOEnh", "-write", "-skipAnalyze", "-nrFiles", rd_num_files.toString(), "-fileSize", rd_file_size.toString(), "-bufferSize", buffer_size.toString() });
    	} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}