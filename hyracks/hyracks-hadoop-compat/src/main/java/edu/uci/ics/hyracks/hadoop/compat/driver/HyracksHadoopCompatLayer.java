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
package edu.uci.ics.hyracks.hadoop.compat.driver;

//import java.io.File;
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.io.OutputStream;
//import java.net.URL;
//import java.net.URLClassLoader;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Map.Entry;
//import java.util.Properties;
//import java.util.Set;

//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.mapred.FileOutputCommitter;
//import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
//import org.apache.hadoop.conf.Configuration;
//import org.kohsuke.args4j.CmdLineParser;


//import edu.uci.ics.hyracks.api.deployment.DeploymentId;
//import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.hadoop.compat.client.HyracksHadoopClient;
import edu.uci.ics.hyracks.hadoop.compat.client.HyracksHadoopRunningJob;
import edu.uci.ics.hyracks.hadoop.compat.client.HyracksRunningJob;
//import edu.uci.ics.hyracks.hadoop.compat.util.CompatibilityConfig;
//import edu.uci.ics.hyracks.hadoop.compat.util.ConfigurationConstants;
//import edu.uci.ics.hyracks.hadoop.compat.util.DCacheHandler;
import edu.uci.ics.hyracks.hadoop.compat.util.HyracksHadoopAdapter;
//import edu.uci.ics.hyracks.hadoop.compat.util.Utilities;

public class HyracksHadoopCompatLayer {

    HyracksHadoopClient hyracksHadoopClient;
    HyracksHadoopAdapter hyrackshadoopAdapter;

    public HyracksHadoopCompatLayer(JobConf jobConf) {//throws Exception {
    	try{
    		initialize(jobConf);
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
    }
  
    private void initialize(JobConf jobConf) throws Exception {
    	hyrackshadoopAdapter = new HyracksHadoopAdapter(jobConf);
        hyracksHadoopClient = new HyracksHadoopClient();
    }   
 
    public HyracksHadoopRunningJob submitJobs(JobConf jobConf) throws Exception {
        JobSpecification spec = hyrackshadoopAdapter.getJobSpecification(jobConf);
        HyracksHadoopRunningJob hyrackshadoopRunningJob = hyracksHadoopClient.submitJob(spec);

        return hyrackshadoopRunningJob;
    }

    public RunningJob run(JobConf jobConf) throws Exception {
//        long startTime = System.nanoTime();
//        NewCompatibilityLayer newcompatLayer = new NewCompatibilityLayer(jobConf);
//        try {
        
            boolean pigmaponly = false;
            String mapper = jobConf.get("mapreduce.job.map.class");
            if (mapper != null){
                if(mapper.contains("PigMapOnly"))
                    pigmaponly = true;
            }
            else if( jobConf.getMapperClass().getName().contains("PIgMapOnly") )
                pigmaponly = true;
            
            if(pigmaponly){
              jobConf.set("mapred.output.key.class", "org.apache.pig.impl.io.NullableBytesWritable");
              jobConf.set("mapreduce.job.output.key.class", "org.apache.pig.impl.io.NullableBytesWritable");
              jobConf.set("mapred.output.value.class", "org.apache.pig.data.BinSedesTuple");
              jobConf.set("mapreduce.job.output.value.class", "org.apache.pig.data.BinSedesTuple");
              
//              jobConf.set("mapred.output.key.class", "org.apache.hadoop.io.NullWritable");
//              jobConf.set("mapreduce.job.output.key.class", "org.apache.hadoop.io.NullWritable");
//              jobConf.set("mapred.output.value.class", "org.apache.pig.data.BinSedesTuple");
//              jobConf.set("mapreduce.job.output.value.class", "org.apache.pig.data.BinSedesTuple");
            }
            
System.out.println("[HyracksHadoopCompatLayer][run] JobConf.writeXml start!");      
jobConf.writeXml(System.out);
System.out.println("[HyracksHadoopCompatLayer][run] JobConf.writeXml end!"); 
            
            return submitJobs(jobConf);
//        	this.hyrackshadoopRunningJob = submitJobs(jobConf);
            
//            newcompatLayer.waitForCompletion(newhyraxRunningJob.getJobId());
//            long end_time = System.nanoTime();
//            System.out.println("TOTAL TIME (from Launch to Completion):"
//                    + ((end_time - startTime) / (float) 1000000000.0) + " seconds.");
//        } catch (Exception e) {
//            e.printStackTrace();
//            throw e;
//        }
        
//        return hyrackshadoopRunningJob;
    }    
}