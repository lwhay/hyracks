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
package edu.uci.ics.hyracks.hadoop.compat.client;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.io.File;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.UnknownHostException;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.deployment.DeploymentId;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.hadoop.compat.util.ConfigurationConstants;
import edu.uci.ics.hyracks.hadoop.compat.util.Utilities;

public class HyracksHadoopClient {

    private static HyracksConnection connection;
	private static final String jobProfilingKey = "jobProfilingKey";
//    Set<String> systemLibs;
    
    public HyracksHadoopClient() throws Exception {
        initialize(false); // false when listen on ip, true when listen on localhost (127.0.0.1)
    }
        
//    public HyracksHadoopClient(Properties clusterProperties, String ip) throws Exception {
//        initialize(clusterProperties, ip);
//    }   

//    public HyracksHadoopClient(String clusterConf, char delimiter) throws Exception {
//        Properties properties = Utilities.getProperties(clusterConf, delimiter);
//        initialize(properties, null);
//    }
    
    private void initialize(boolean localhost) throws Exception {
    	String ip = null;
    	if (localhost) {
    		ip = "127.0.0.1";	
    	}
    	else {
    		try {
       		 ip = getIPv4InetAddress().getHostAddress();
    		} catch (Exception e) {
               e.toString();
    		}
    	}

        connection = new HyracksConnection(ip, 1098); //1098
    }  
    
    private InetAddress getIPv4InetAddress() throws SocketException, UnknownHostException {
        String os = System.getProperty("os.name").toLowerCase();

        if(os.indexOf("nix") >= 0 || os.indexOf("nux") >= 0) {   
            NetworkInterface ni = NetworkInterface.getByName("eth0");

            Enumeration<InetAddress> ias = ni.getInetAddresses();

            InetAddress iaddress;
            do {
                iaddress = ias.nextElement();
            } while(!(iaddress instanceof Inet4Address));

            return iaddress;
        }

        return InetAddress.getLocalHost();
    }
       
    private void initialize(Properties properties, String ip) throws Exception {
        String clusterController = (String) properties.get(ConfigurationConstants.clusterControllerHost);
        if (ip != null)
        	connection = new HyracksConnection(ip, 1098);
        else
        	connection = new HyracksConnection("127.0.0.1", 1098);
    }  

    public JobStatus getJobStatus(JobId jobId) throws Exception {
        return connection.getJobStatus(jobId);
    }     
     
    public HyracksHadoopRunningJob submitJob(JobSpecification spec) throws Exception {
        String jobProfilingVal = System.getenv(jobProfilingKey);
        boolean doProfiling = ("true".equalsIgnoreCase(jobProfilingVal));
        JobId jobId;
        ClassLoader ctxCL = Thread.currentThread().getContextClassLoader();
        DeploymentId deploymentId = deployJars(ctxCL);
System.out.println("[NewHyracksClient][submitJob] deploymentId: " + deploymentId.toString());        
        if (doProfiling) {
            System.out.println("PROFILING");
            jobId = connection.startJob(deploymentId, spec, EnumSet.of(JobFlag.PROFILE_RUNTIME));
        } else {
            jobId = connection.startJob(deploymentId, spec);
        }   
        HyracksHadoopRunningJob runningJob = new HyracksHadoopRunningJob(jobId, spec, this);
        return runningJob;
    } 

    public void waitForCompleton(JobId jobId) throws Exception {
        connection.waitForCompletion(jobId);
    }
    
    public DeploymentId deployJars(ClassLoader classLoader) throws Exception {
    	URLClassLoader clsLdr = (URLClassLoader) classLoader;
System.out.println("ClassLoader to be used in deploying jar: " + classLoader);    	
        List<File> jars = new ArrayList<File>();
        URL[] urls = clsLdr.getURLs();
        for (URL url : urls)
            if (url.toString().endsWith(".jar")){
                jars.add(new File(url.getPath()));
            } 
        DeploymentId deploymentId = installApplication(jars);
        return deploymentId;
    }
    
    public DeploymentId installApplication(List<File> jars) throws Exception {
        List<String> allJars = new ArrayList<String>();
        for (File jar : jars) {
            allJars.add(jar.getAbsolutePath());
System.out.println("Jar to be deployed: " + jar.getAbsolutePath());            
        }
        long startTime = System.currentTimeMillis();
        DeploymentId deploymentId = connection.deployBinary(allJars);
        long endTime = System.currentTimeMillis();
System.out.println("TOTAL TIME to deploy all Jars:"
                + ((endTime - startTime) / (float) 1000000000.0) + " seconds.");
        return deploymentId;
    }
    
    public static HyracksConnection getConnection() {
		return connection;
	}

	public static void setConnection(HyracksConnection connection) {
		HyracksHadoopClient.connection = connection;
	}

}