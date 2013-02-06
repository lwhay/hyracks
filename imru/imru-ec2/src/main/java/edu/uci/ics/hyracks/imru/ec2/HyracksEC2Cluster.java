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
package edu.uci.ics.hyracks.imru.ec2;

import java.io.File;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.StartInstancesResult;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.ec2.model.StopInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;

import edu.uci.ics.hyracks.imru.util.R;

/**
 * @author wangrui
 */
public class HyracksEC2Cluster {
    public static int MAX_COUNT = 3;
    public static final String HYRACKS_SECURITY_GROUP = "Hyracks-security-group";
    EC2Wrapper ec2;
    String keyName;
    String instancePrefix;
    String securityGroup = HYRACKS_SECURITY_GROUP;
    HyracksEC2Node controller;
    HyracksEC2Node[] nodes;
    String machineType = "t1.micro";
    // ubuntu image: ami-3d4ff254
    // fullstack_imru: ami-8937a0e0
    String imageId = "ami-8937a0e0";

    public HyracksEC2Cluster(File credentialsFile, File pemDir, String keyName, String instancePrefix) throws Exception {
        this.ec2 = new EC2Wrapper(credentialsFile, pemDir);
        this.keyName = keyName;
        this.instancePrefix = instancePrefix;
        refresh();
    }

    public String getMachineType() {
        return machineType;
    }

    public void setMachineType(String machineType) {
        this.machineType = machineType;
    }

    public String getImageId() {
        return imageId;
    }

    public void setImageId(String imageId) {
        this.imageId = imageId;
    }

    public void refresh() {
        Vector<HyracksEC2Node> v = new Vector<HyracksEC2Node>();
        HyracksEC2Node controller = null;
        for (Instance instance : ec2.listInstances(instancePrefix)) {
            HyracksEC2Node node = new HyracksEC2Node(this);
            String name = ec2.getName(instance);
            if (!name.startsWith(instancePrefix))
                throw new Error("not a IMRU instance");
            node.nodeId = Integer.parseInt(name.substring(instancePrefix.length()));
            node.instance = instance;
            if (node.nodeId == 0)
                controller = node;
            v.add(node);
        }
        HyracksEC2Node[] nodes = v.toArray(new HyracksEC2Node[v.size()]);
        Arrays.sort(nodes, new Comparator<HyracksEC2Node>() {
            @Override
            public int compare(HyracksEC2Node o1, HyracksEC2Node o2) {
                return o1.nodeId - o2.nodeId;
            }
        });
        this.controller = controller;
        this.nodes = nodes;
    }

    public void createSecurityGroup() {
        int[] ports = { 22, 1099, 3099, 16001 };
        ec2.createSecurityGroup(securityGroup, "Hyracks Security Group", ports);
    }

    /**
     * Wait until it's possible to ssh to all instances
     * 
     * @throws Exception
     */
    public void waitSSH() throws Exception {
        LinkedList<HyracksEC2Node> queue = new LinkedList<HyracksEC2Node>();
        for (HyracksEC2Node node : nodes)
            queue.add(node);
        while (queue.size() > 0) {
            HyracksEC2Node node = queue.remove();
            int n = 0;
            while (true) {
                try {
                    String result = ec2.ssh(node.instance, "whoami");
                    if (!result.contains("timed out") && !result.contains("refused"))
                        break;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                Thread.sleep(1000);
                if ((n % 5) == 0)
                    R.p("NC" + node.nodeId + " hasn't started ssh yet");
                n++;
            }
        }
    }

    /**
     * Make sure all instances are added to known host list
     * 
     * @throws Exception
     */
    public void sshTest() throws Exception {
        for (HyracksEC2Node node : nodes) {
            String result = ec2.ssh(node.instance, "whoami");
            if (result.contains("failed"))
                throw new Exception("host key verification of " + node.instance.getPublicDnsName() + " failed");
        }
    }

    public void install(File imruRoot) throws Exception {
        for (HyracksEC2Node node : nodes)
            node.install(imruRoot);
    }

    public void setTotalInstances(int count) throws Exception {
        if (count > MAX_COUNT)
            throw new Error("For safety reason, please modify " + this.getClass().getName() + ".MAX_COUNT first");
        if (nodes.length < count) {
            addInstances(count - nodes.length);
        } else {
            for (int i = count; i < nodes.length; i++) {
                nodes[i].stopNC();
                nodes[i].terminateInstance();
            }
            refresh();
        }
    }

    public int getTotalPendingMachines() {
        refresh();
        int pending = 0;
        for (HyracksEC2Node node : nodes) {
            //pending, running, shutting-down, terminated, stopping, stopped
            String state = node.instance.getState().getName();
            if ("pending".equals(state))
                pending++;
        }
        return pending;
    }

    public void waitForInstanceStart() throws Exception {
        // takes up to 30 seconds
        int n = 0;
        while (true) {
            int pending = getTotalPendingMachines();
            if (pending == 0)
                return;
            if ((n % 5) == 0)
                R.p(pending + " machines are pending");
            n++;
            Thread.sleep(1000);
        }
    }

    public int getTotalStoppingMachines() {
        refresh();
        int pending = 0;
        for (HyracksEC2Node node : nodes) {
            //pending, running, shutting-down, terminated, stopping, stopped
            String state = node.instance.getState().getName();
            if ("stopping".equals(state) || "shutting-down".equals(state))
                pending++;
        }
        return pending;
    }

    public void waitForInstanceStop() throws Exception {
        // takes up to 30 seconds
        int n = 0;
        while (true) {
            int pending = getTotalStoppingMachines();
            if (pending == 0)
                return;
            if ((n % 5) == 0)
                R.p(pending + " machines are stopping");
            n++;
            Thread.sleep(1000);
        }
    }

    public void addInstances(int count) {
        //        ec2.setEndpoint("ec2.us-east-1.amazonaws.com");

        RunInstancesRequest runInstancesRequest = new RunInstancesRequest().withInstanceType(machineType).withImageId(
                imageId).withMinCount(count).withMaxCount(count).withSecurityGroupIds(securityGroup).withKeyName(
                keyName);

        RunInstancesResult runInstances = ec2.ec2.runInstances(runInstancesRequest);

        BitSet bs = new BitSet();
        for (HyracksEC2Node node : nodes)
            bs.set(node.nodeId);
        List<Instance> instances = runInstances.getReservation().getInstances();
        int idx = 0;
        for (Instance instance : instances) {
            idx = bs.nextClearBit(idx);
            CreateTagsRequest createTagsRequest = new CreateTagsRequest();
            createTagsRequest.withResources(instance.getInstanceId()) //
                    .withTags(new Tag("Name", instancePrefix + idx));
            ec2.ec2.createTags(createTagsRequest);
            bs.set(idx);
            idx++;
        }
        refresh();
    }

    public void startInstances() {
        refresh();
        Vector<String> instanceIds = new Vector<String>();
        for (HyracksEC2Node node : nodes) {
            //pending, running, shutting-down, terminated, stopping, stopped
            String state = node.instance.getState().getName();
            if ("stopped".equals(state))
                instanceIds.add(node.instance.getInstanceId());
        }
        StartInstancesRequest startInstancesRequest = new StartInstancesRequest().withInstanceIds(instanceIds);
        StartInstancesResult result = ec2.ec2.startInstances(startInstancesRequest);
        R.p(result);
    }

    public void stopInstances() {
        Vector<String> instanceIds = new Vector<String>();
        for (HyracksEC2Node node : nodes)
            instanceIds.add(node.instance.getInstanceId());
        StopInstancesRequest stopInstancesRequest = new StopInstancesRequest().withForce(false).withInstanceIds(
                instanceIds);
        StopInstancesResult result = ec2.ec2.stopInstances(stopInstancesRequest);
        R.p(result);
    }

    /**
     * terminate (delete) all instances
     */
    public void terminateInstances() {
        Vector<String> instanceIds = new Vector<String>();
        for (HyracksEC2Node node : nodes)
            instanceIds.add(node.instance.getInstanceId());
        TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest()
                .withInstanceIds(instanceIds);
        TerminateInstancesResult result = ec2.ec2.terminateInstances(terminateInstancesRequest);
        R.p(result);
    }

    public void printNodeStatus() {
        refresh();
        R.np("Hyracks EC2 Nodes:");
        for (HyracksEC2Node node : nodes)
            R.np("NC" + node.nodeId + ": " + node.instance.getState().getName());
    }

    public String getAdminURL() {
        return "http://" + controller.instance.getPublicDnsName() + ":16001/adminconsole/";
    }

    public void startHyrackCluster() throws Exception {
        controller.startCC();
        for (HyracksEC2Node node : nodes)
            node.startNC();
    }

    public void stopHyrackCluster() throws Exception {
        for (HyracksEC2Node node : nodes) {
            if (node.nodeId == 0)
                node.stopAll();
            else
                node.stopNC();
        }
    }

    public void printLogs() throws Exception {
        for (HyracksEC2Node node : nodes)
            node.showLogs();
    }
}
