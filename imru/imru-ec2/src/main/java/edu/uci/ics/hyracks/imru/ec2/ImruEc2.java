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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Vector;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Image;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.LaunchSpecification;
import com.amazonaws.services.ec2.model.RequestSpotInstancesRequest;
import com.amazonaws.services.ec2.model.RequestSpotInstancesResult;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Snapshot;
import com.amazonaws.services.ec2.model.SpotInstanceRequest;
import com.amazonaws.services.ec2.model.StopInstancesRequest;
import com.amazonaws.services.ec2.model.StopInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;

import edu.uci.ics.hyracks.imru.util.R;

/**
 * Automatically deployment of EC2 cluster
 * 
 * @author wangrui
 */
public class ImruEc2 {
    public static final String IMRU_PREFIX = "IMRU-auto-deploy-";
    File pemDir;
    File imruRoot;
    File hadoopRoot;
    AWSCredentials credentials = null;
    PrintStream p = System.out;
    AmazonEC2 ec2;

    public ImruEc2(File credentialsFile, File pemDir, File imruRoot, File hadoopRoot) throws Exception {
        this.pemDir = pemDir;
        this.imruRoot = imruRoot;
        this.hadoopRoot = hadoopRoot;
        credentials = new PropertiesCredentials(new FileInputStream(credentialsFile));
        ec2 = new AmazonEC2Client(credentials);
    }

    public Instance[] listIMRUInstances() {
        Vector<Instance> instances = new Vector<Instance>();
        DescribeInstancesResult result = ec2.describeInstances();
        for (Reservation reservation : result.getReservations()) {
            for (Instance instance : reservation.getInstances()) {
                String name = null;
                for (Tag tag : instance.getTags()) {
                    if ("Name".equalsIgnoreCase(tag.getKey())) {
                        name = tag.getValue();
                    }
                }
                if (name == null || !name.contains(IMRU_PREFIX))
                    continue;
                instances.add(instance);
            }
        }
        return instances.toArray(new Instance[instances.size()]);
    }

    public void prepareTemplateInstance(Instance instance) throws Exception {
        File pemFile = new File(pemDir, instance.getKeyName() + ".pem");
        if (!pemFile.exists())
            throw new IOException("Can't find " + pemFile);
        SSH ssh = new SSH("ubuntu", instance.getPublicDnsName(), 22, pemFile);
        try {
            ssh.execute("sudo apt-get update");
            ssh.execute("sudo apt-get install openjdk-7-jre");
            ssh.upload(new File(imruRoot, "hyracks/hyracks-server/target/appassembler"),
                    "/home/ubuntu/fullstack_imru/hyracks/hyracks-server/target/appassembler");
            ssh.upload(new File(imruRoot, "imru/imru-dist/target/appassembler"),
                    "/home/ubuntu/fullstack_imru/imru/imru-dist/target/appassembler");
            ssh.upload(new File(imruRoot, "imru/imru-example/data"),
                    "/home/ubuntu/fullstack_imru/imru/imru-example/data");
            ssh.execute("chmod -R 755 /home/ubuntu/fullstack_imru/hyracks/hyracks-server/target/appassembler/bin/*");
            ssh.execute("chmod -R 755 /home/ubuntu/fullstack_imru/imru/imru-dist/target/appassembler/bin/*");
            ssh.upload(hadoopRoot, "/home/ubuntu/hadoop-0.20.2");
            ssh.execute("chmod -R 755 /home/ubuntu/hadoop-0.20.2/bin/*");
            ssh.execute("cat /home/ubuntu/fullstack_imru/imru/imru-dist/target/appassembler/conf/master");
            ssh.execute("cat /home/ubuntu/fullstack_imru/imru/imru-dist/target/appassembler/conf/slaves");
            ssh.execute("cd /home/ubuntu/fullstack_imru/imru/imru-dist/target/appassembler");
            ssh.execute("bin/startCluster.sh");
        } finally {
            ssh.close();
        }
    }

    public void showInstance(Instance instance) {
        p.println("---------------");
        p.println("InstanceId: " + instance.getInstanceId());
        p.println("ImageId: " + instance.getImageId());
        p.println("InstanceType: " + instance.getInstanceType());
        p.println("State: " + instance.getState());
        p.println("KeyName: " + instance.getKeyName());
        p.println("Tags: " + instance.getTags());
        p.println("PublicDnsName: " + instance.getPublicDnsName());
        p.println("PrivateDnsName: " + instance.getPrivateDnsName());
        p.println("PublicIpAddress: " + instance.getPublicIpAddress());
        p.println("PrivateIpAddress: " + instance.getPrivateIpAddress());
    }

    public void runInstances(String keyName) {
        runInstances(keyName, "t1.micro",
        //                "ami-3d4ff254", //ubuntu
                "ami-554eda3c", //IMRU template
                1, "quicklaunch-1");
    }

    public void runInstances(String keyName, String machineType, String imageId, int count, String securityGroup) {
        //        ec2.setEndpoint("ec2.us-east-1.amazonaws.com");

        RunInstancesRequest runInstancesRequest = new RunInstancesRequest().withInstanceType(machineType).withImageId(
                imageId).withMinCount(count).withMaxCount(count).withSecurityGroupIds(securityGroup).withKeyName(
                keyName);

        RunInstancesResult runInstances = ec2.runInstances(runInstancesRequest);

        List<Instance> instances = runInstances.getReservation().getInstances();
        int idx = 1;
        for (Instance instance : instances) {
            System.out.println(idx);
            CreateTagsRequest createTagsRequest = new CreateTagsRequest();
            createTagsRequest.withResources(instance.getInstanceId()) //
                    .withTags(new Tag("Name", IMRU_PREFIX + idx));
            ec2.createTags(createTagsRequest);
            idx++;
        }
    }

    public void showdownAllIMRUInstancesForce() {
        Vector<String> instanceIds = new Vector<String>();
        for (Instance instance : listIMRUInstances())
            instanceIds.add(instance.getInstanceId());
        StopInstancesRequest stopInstancesRequest = new StopInstancesRequest().withForce(false).withInstanceIds(
                instanceIds);
        StopInstancesResult result = ec2.stopInstances(stopInstancesRequest);
        R.p(result);
    }

    public void terminateAllIMRUInstancesForce() {
        Vector<String> instanceIds = new Vector<String>();
        for (Instance instance : listIMRUInstances())
            instanceIds.add(instance.getInstanceId());
        TerminateInstancesRequest terminateInstancesRequest = new TerminateInstancesRequest()
                .withInstanceIds(instanceIds);
        TerminateInstancesResult result = ec2.terminateInstances(terminateInstancesRequest);
        R.p(result);
    }

    public static void main(String[] args) throws Exception {
        SSH ssh = new SSH("wangrui", "localhost", 22, new File(System.getProperty("user.home") + "/.ssh/id_rsa"));
        ssh.execute("ifconfig");
        ssh.close();
        System.exit(0);

        File home = new File(System.getProperty("user.home"));
        File pemDir = home;
        File credentialsFile = new File(home, "AwsCredentials.properties");
        File imruRoot = new File(home, "fullstack_imru");
        File hadoopRoot = new File(home, "hadoop-0.20.2");
        ImruEc2 imru = new ImruEc2(credentialsFile, pemDir, imruRoot, hadoopRoot);
        //        imru.showdownAllIMRUInstancesForce();
        //        imru.terminateAllIMRUInstancesForce();
        //                imru.runInstances("secondTestByRui");
        for (Instance instance : imru.listIMRUInstances()) {
            imru.showInstance(instance);
        }
    }
}
