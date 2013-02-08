/*
 * Copyright 2009-2010 by The Regents of the University of California
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
package edu.uci.ics.hyracks.ec2;

import java.io.File;
import java.util.Vector;

import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.ec2.model.StopInstancesResult;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;

/**
 * @author wangrui
 */
public class HyracksEC2Node {
    public static final String NAME_PREFIX = "NC";
    public static final String HYRACKS_PATH = "/home/ubuntu/hyracks-ec2";
    HyracksEC2Cluster cluster;
    int nodeId;
    String name;
    Instance instance;

    public HyracksEC2Node(HyracksEC2Cluster cluster, int nodeId, Instance instance) {
        this.cluster = cluster;
        this.nodeId = nodeId;
        this.instance = instance;
        this.name = NAME_PREFIX + nodeId;
    }

    public int getNodeId() {
        return nodeId;
    }

    public String getName() {
        return name;
    }

    public Instance getInstance() {
        return instance;
    }

    public void install(File hyracksEc2Root) throws Exception {
        SSH ssh = cluster.ec2.ssh(instance);
        try {
            Rt.p("rync hyracks-ec2 to " + name);
            //            ssh.execute("sudo apt-get update");
            //            ssh.execute("sudo apt-get install openjdk-7-jre");
            cluster.ec2.rsync(instance, ssh, hyracksEc2Root, HYRACKS_PATH);
            //            cluster.ec2.rsync(instance, ssh, new File(imruRoot, "hyracks/hyracks-ec2/target/appassembler"),
            //                    "/home/ubuntu/fullstack_imru/hyracks/hyracks-ec2/target/appassembler");
            //            cluster.ec2.rsync(instance, ssh, new File(imruRoot, "imru/imru-example/data"),
            //                    "/home/ubuntu/fullstack_imru/imru/imru-example/data");
            ssh.execute("chmod -R u+x " + HYRACKS_PATH + "/bin/*");
            //            ssh.execute("chmod -R 755 /home/ubuntu/fullstack_imru/hyracks/hyracks-ec2/target/appassembler/bin/*");
            //            ssh.execute("chmod -R 755 /home/ubuntu/fullstack_imru/hyracks/hyracks-ec2/target/appassembler/bin/*");
            //            ec2.rsync(instance, ssh, hadoopRoot, "/home/ubuntu/hadoop-0.20.2");
            //            ssh.execute("chmod -R 755 /home/ubuntu/hadoop-0.20.2/bin/*");
        } finally {
            ssh.close();
        }
    }

    public void uploadData(String local, String remote) throws Exception {
        SSH ssh = cluster.ec2.ssh(instance);
        try {
            Rt.p("rync " + local + " to " + name + ":" + remote);
            cluster.ec2.rsync(instance, ssh, new File(local), remote);
        } finally {
            ssh.close();
        }
    }

    public void startCC() throws Exception {
        SSH ssh = cluster.ec2.ssh(instance);
        try {
            String result1 = ssh.execute("ps -ef|grep hyrackscc", true);
            if (result1.contains("java")) {
                Rt.p("CC is already running");
                return;
            }
            Rt.p("starting CC");
            ssh.execute("cd " + HYRACKS_PATH);
            ssh.execute("nohup bin/startccWithHostIp.sh " + instance.getPrivateIpAddress());
            //        ec2.ssh(instance, "cd /home/ubuntu/fullstack_imru/hyracks/hyracks-ec2/target/appassembler;"
            //                + "bin/startccWithHostIp.sh " + instance.getPrivateIpAddress());
        } finally {
            ssh.close();
        }
    }

    public void startNC() throws Exception {
        SSH ssh = cluster.ec2.ssh(instance);
        try {
            String result1 = ssh.execute("ps -ef|grep hyracksnc", true);
            if (result1.contains("java")) {
                Rt.p(name + " is already running");
                return;
            }
            Rt.p("starting " + name);
            ssh.execute("cd " + HYRACKS_PATH);
            ssh.execute("nohup bin/startncWithHostIpAndNodeId.sh " + cluster.controller.instance.getPrivateIpAddress()
                    + " " + instance.getPrivateIpAddress() + " " + name);
            //        ec2.ssh(instance, "cd /home/ubuntu/fullstack_imru/hyracks/hyracks-ec2/target/appassembler;"
            //                + "bin/startncWithHostIpAndNodeId.sh " + clusterControllerInstance.getPrivateIpAddress() + " "
            //                + instance.getPrivateIpAddress() + " " + nodeId);
        } finally {
            ssh.close();
        }
    }

    public void stopCC() throws Exception {
        SSH ssh = cluster.ec2.ssh(instance);
        try {
            ssh.execute("cd " + HYRACKS_PATH);
            ssh.execute("bin/stopcc.sh");
        } finally {
            ssh.close();
        }
    }

    public void stopNC() throws Exception {
        SSH ssh = cluster.ec2.ssh(instance);
        try {
            ssh.execute("cd " + HYRACKS_PATH);
            ssh.execute("bin/stopnc.sh");
        } finally {
            ssh.close();
        }
    }

    public void stopAll() throws Exception {
        SSH ssh = cluster.ec2.ssh(instance);
        try {
            ssh.execute("cd " + HYRACKS_PATH);
            ssh.execute("bin/stopcc.sh");
            ssh.execute("bin/stopnc.sh");
        } finally {
            ssh.close();
        }
    }

    public void stopInstance() {
        Vector<String> instanceIds = new Vector<String>();
        instanceIds.add(instance.getInstanceId());
        StopInstancesRequest stopInstancesRequest = new StopInstancesRequest().withForce(false).withInstanceIds(
                instanceIds);
        StopInstancesResult result = cluster.ec2.ec2.stopInstances(stopInstancesRequest);
        Rt.p(result);
    }

    /**
     * terminate (delete) all instances
     */
    public void terminateInstance() {
        Vector<String> instanceIds = new Vector<String>();
        instanceIds.add(instance.getInstanceId());
        TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest()
                .withInstanceIds(instanceIds);
        TerminateInstancesResult result = cluster.ec2.ec2.terminateInstances(terminateInstancesRequest);
        Rt.p(result);
    }

    public void printProcesses() throws Exception {
        SSH ssh = cluster.ec2.ssh(instance);
        try {
            if (nodeId == 0) {
                Rt.p("CC log:");
                ssh.execute("ps -ef|grep hyrackscc|grep java");
            }
            Rt.p(name + " log:");
            ssh.execute("ps -ef|grep hyracksnc|grep java", true);
        } finally {
            ssh.close();
        }
    }

    public void printLogs() throws Exception {
        SSH ssh = cluster.ec2.ssh(instance);
        try {
            if (nodeId == 0) {
                Rt.np("CC log:");
                ssh.cat("/tmp/t1/logs/cc.log");
            }
            Rt.np(name + " log:");
            ssh.cat("/tmp/t2/logs/" + name + ".log");
            ssh.cat(HYRACKS_PATH + "/nohup.out");
        } finally {
            ssh.close();
        }
    }
}
