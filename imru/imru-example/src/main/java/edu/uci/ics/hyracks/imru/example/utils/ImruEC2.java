package edu.uci.ics.hyracks.imru.example.utils;

import java.io.File;
import java.io.Serializable;

import edu.uci.ics.hyracks.ec2.HyracksEC2Cluster;
import edu.uci.ics.hyracks.imru.api.IModel;
import edu.uci.ics.hyracks.imru.api2.IIMRUJob;
import edu.uci.ics.hyracks.imru.util.Rt;

public class ImruEC2 {
    public static String IMRU_PREFIX = "hyracks-auto-deploy-";

    HyracksEC2Cluster cluster;

    public ImruEC2(File credentialsFile, File privateKey) throws Exception {
        cluster = new HyracksEC2Cluster(credentialsFile, privateKey, IMRU_PREFIX);
    }

    public void setup(File hyracksEc2Root, int instanceCount, String machineType) throws Exception {
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

    public String getSuggestedLocations(String[] localPath) {
        String[] nodeNames = cluster.getNodeNames();
        String[] remotePath = new String[localPath.length];
        StringBuilder path = new StringBuilder();
        for (int i = 0; i < localPath.length; i++) {
            remotePath[i] = nodeNames[i % nodeNames.length] + ":/home/ubuntu/data/hello" + i + ".txt";
            path.append(remotePath[i] + ",");
        }
        return path.toString();
    }

    public String uploadData(String[] localPath) throws Exception {
        String[] nodeNames = cluster.getNodeNames();
        String[] remotePath = new String[localPath.length];
        StringBuilder path = new StringBuilder();
        for (int i = 0; i < localPath.length; i++) {
            remotePath[i] = nodeNames[i % nodeNames.length] + ":/home/ubuntu/data/hello" + i + ".txt";
            path.append(remotePath[i] + ",");
        }
        cluster.uploadData(localPath, remotePath);
        return path.toString();
    }

    public <M extends IModel, D extends Serializable, R extends Serializable> M run(IIMRUJob<M, D, R> job,
            String appName, String paths) throws Exception {
        cluster.printLogs(-1);
        String clusterIp = cluster.getClusterControllerPublicDnsName();
        Rt.p("Admin URL: " + cluster.getAdminURL());
        String cmdline = "";
        cmdline += "-host " + clusterIp;
        cmdline += " -port 3099";
        cmdline += " -app " + appName;
        cmdline += " -temp-path /tmp/imru_" + appName;
        cmdline += " -example-paths " + paths;
        cmdline += " -abondon-intermediate-models";
        cmdline += " -model-file-name helloworld";
        cmdline = cmdline.trim();
        System.out.println("Using command line: " + cmdline);
        String[] args = cmdline.split(" ");
        return Client.run(job, args);
    }
}
