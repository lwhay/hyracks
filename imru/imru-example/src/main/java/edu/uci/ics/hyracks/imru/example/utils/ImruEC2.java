package edu.uci.ics.hyracks.imru.example.utils;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

import edu.uci.ics.hyracks.ec2.HyracksEC2Cluster;
import edu.uci.ics.hyracks.imru.api.IIMRUJob;
import edu.uci.ics.hyracks.imru.file.IMRUFileSplit;
import edu.uci.ics.hyracks.imru.file.IMRUInputSplitProvider;
import edu.uci.ics.hyracks.imru.jobgen.ClusterConfig;
import edu.uci.ics.hyracks.imru.util.Rt;

public class ImruEC2 {
    public static String IMRU_PREFIX = "hyracks-auto-deploy-";

    public HyracksEC2Cluster cluster;
    String ccHostName;

    public ImruEC2(File credentialsFile, File privateKey) throws Exception {
        cluster = new HyracksEC2Cluster(credentialsFile, privateKey,
                IMRU_PREFIX);
    }
    
    protected void init() {
        if (ccHostName == null) {
            ccHostName = cluster.getClusterControllerPublicDnsName();
            Rt.p("Admin URL: " + cluster.getAdminURL());
        }
    }

    public void setup(File hyracksEc2Root, int instanceCount, String machineType)
            throws Exception {
        cluster.setTotalInstances(instanceCount);
        cluster.setMachineType(machineType);
        cluster.printNodeStatus();
        if (cluster.getTotalMachines("stopped") > 0)
            cluster.startInstances();
        if (cluster.getTotalMachines("pending") > 0) {
            cluster.waitForInstanceStart();
            cluster.printNodeStatus();
        }
        cluster.sshTest();
        cluster.install(hyracksEc2Root);
        cluster.stopHyrackCluster();
        cluster.startHyrackCluster();
    }

    public String getSuggestedLocations(String[] localPath, String folder) {
        String[] nodeNames = cluster.getNodeNames();
        String[] remotePath = new String[localPath.length];
        StringBuilder path = new StringBuilder();
        for (int i = 0; i < localPath.length; i++) {
            remotePath[i] = nodeNames[i % nodeNames.length]
                    + ":/home/ubuntu/data/" + folder + "/"
                    + new File(localPath[i]).getName();
            path.append(remotePath[i] + ",");
        }
        return path.toString();
    }

    public String uploadData(String[] localPath, String folder)
            throws Exception {
        String[] nodeNames = cluster.getNodeNames();
        String[] remotePath = new String[localPath.length];
        StringBuilder path = new StringBuilder();
        for (int i = 0; i < localPath.length; i++) {
            remotePath[i] = nodeNames[i % nodeNames.length]
                    + ":/home/ubuntu/data/" + folder + "/"
                    + new File(localPath[i]).getName();
            path.append(remotePath[i] + ",");
        }
        cluster.uploadData(localPath, remotePath);
        return path.toString();
    }

    public void spreadData(String[] localPath, String remotePath)
            throws Exception {
        init();
        //        IMRUFileSplit[] inputSplits = IMRUInputSplitProvider.getInputSplits(
        //                inputPaths, null);
        //        String[] trainOperatorLocations = ClusterConfig.setLocationConstraint(
        //                null, null, inputSplits, new Random());
        //        String[] mergeOperatorLocations = new HashSet<String>(
        //                Arrays.asList(trainOperatorLocations)).toArray(new String[0]);

        File[] localFiles = new File[localPath.length];
        String[] nodeNames = cluster.getNodeNames();
        String[] remotePaths = new String[localPath.length];
        StringBuilder path = new StringBuilder();
        for (int i = 0; i < localPath.length; i++) {
            localFiles[i] = new File(localPath[i]);
            remotePaths[i] = remotePath + "/" + localFiles[i].getName();
            path.append(remotePaths[i] + ",");
        }
        String cmdline = "";
        cmdline += "-host " + ccHostName;
        cmdline += " -port 3099";
        cmdline += " -app ec2data";
        cmdline += " -model-file-name helloworld";
        cmdline = cmdline.trim();
        System.out.println("Using command line: " + cmdline);
        String[] args = cmdline.split(" ");
        Client.distributeData(localFiles, nodeNames, remotePaths, args);
    }

    public <M extends Serializable, D extends Serializable, R extends Serializable> M run(
            IIMRUJob<M, D, R> job, M initialModel, String appName, String paths)
            throws Exception {
        //        cluster.printLogs(-1);
        init();
        String cmdline = "";
        cmdline += "-host " + ccHostName;
        cmdline += " -port 3099";
        cmdline += " -app " + appName;
        cmdline += " -example-paths " + paths;
        cmdline += " -model-file-name helloworld";
        cmdline = cmdline.trim();
        System.out.println("Using command line: " + cmdline);
        String[] args = cmdline.split(" ");
        return Client.run(job, initialModel, args);
    }
}
