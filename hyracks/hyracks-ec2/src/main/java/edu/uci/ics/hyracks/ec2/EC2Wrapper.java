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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Vector;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceState;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.SecurityGroup;
import com.amazonaws.services.ec2.model.Tag;

/**
 * @author wangrui
 */
public class EC2Wrapper {
    File pemDir;
    AWSCredentials credentials = null;
    AmazonEC2 ec2;

    public EC2Wrapper(File credentialsFile, File pemDir) throws Exception {
        this.pemDir = pemDir;
        credentials = new PropertiesCredentials(new FileInputStream(credentialsFile));
        ec2 = new AmazonEC2Client(credentials);
    }

    public void reload() {
        ec2 = new AmazonEC2Client(credentials);
    }

    public File getPemFile(Instance instance) throws IOException {
        File pemFile = new File(pemDir, instance.getKeyName() + ".pem");
        if (!pemFile.exists())
            throw new IOException("Can't find " + pemFile);
        return pemFile;
    }

    public SSH ssh(Instance instance) throws Exception {
        SSH ssh = new SSH("ubuntu", instance.getPublicDnsName(), 22, getPemFile(instance));
        return ssh;
    }

    public String ssh(Instance instance, String cmd) throws Exception {
        return Rt.runAndShowCommand("ssh", "-i", this.getPemFile(instance).getAbsolutePath(),
                instance.getPublicDnsName(), "-l", "ubuntu", cmd);
    }

    public void rsync(Instance instance, SSH ssh, File localDir, String remoteDir) throws Exception {
        String rsync = "/usr/bin/rsync";
        if (new File(rsync).exists()) {
            grantAccessToLocalMachine(instance);
            if (localDir.isDirectory()) {
                if (!remoteDir.endsWith("/"))
                    remoteDir += "/";
                ssh.execute("if test ! \"(\" -e '" + remoteDir + "' \")\";then mkdir -p '" + remoteDir + "';fi;");
                Rt.runAndShowCommand(rsync, "-vrultzCc", localDir.getAbsolutePath() + "/",
                        "ubuntu@" + instance.getPublicDnsName() + ":" + remoteDir);
            } else {
                String s = new File(remoteDir).getParent();
                ssh.execute("if test ! \"(\" -e '" + s + "' \")\";then mkdir -p '" + s + "';fi;");
                Rt.runAndShowCommand(rsync, "-vrultzCc", localDir.getAbsolutePath(),
                        "ubuntu@" + instance.getPublicDnsName() + ":" + remoteDir);
            }
        } else {
            System.err.println("WARNING: Please install rsync to speed up synchronization");
            ssh.upload(localDir, remoteDir);
        }
    }

    public Instance[] listInstances(String prefix) {
        Vector<Instance> instances = new Vector<Instance>();
        DescribeInstancesResult result = ec2.describeInstances();
        for (Reservation reservation : result.getReservations()) {
            for (Instance instance : reservation.getInstances()) {
                InstanceState state = instance.getState();
                if ("terminated".equals(state.getName()))
                    continue;
                String name = null;
                for (Tag tag : instance.getTags()) {
                    if ("Name".equalsIgnoreCase(tag.getKey())) {
                        name = tag.getValue();
                    }
                }
                if (name == null || !name.startsWith(prefix))
                    continue;
                instances.add(instance);
            }
        }
        return instances.toArray(new Instance[instances.size()]);
    }

    public void showInstance(Instance instance) {
        PrintStream p = System.out;
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

    public String getName(Instance instance) {
        String name = null;
        for (Tag tag : instance.getTags()) {
            if ("Name".equals(tag.getKey())) {
                name = tag.getValue();
            }
        }
        return name;
    }

    public void createSecurityGroup(String groupName, String desc, int[] ports) {
        DescribeSecurityGroupsResult result = ec2.describeSecurityGroups();
        SecurityGroup imruGroup = null;
        for (SecurityGroup group : result.getSecurityGroups()) {
            if (groupName.equalsIgnoreCase(group.getGroupName())) {
                imruGroup = group;
            }
        }
        if (imruGroup == null) {
            Rt.p("creating security group " + groupName);
            CreateSecurityGroupRequest securityGroupRequest = new CreateSecurityGroupRequest(groupName, desc);
            ec2.createSecurityGroup(securityGroupRequest);
        }
        result = ec2.describeSecurityGroups();
        for (SecurityGroup group : result.getSecurityGroups()) {
            if (groupName.equals(group.getGroupName())) {
                imruGroup = group;
                break;
            }
        }
        HashSet<Integer> opened = new HashSet<Integer>();
        HashSet<Integer> opened2 = new HashSet<Integer>();
        for (IpPermission p : imruGroup.getIpPermissions()) {
            opened.add(p.getToPort());
            opened2.add(p.getToPort());
        }
        String ipAddr = "0.0.0.0/0";
        ArrayList<String> ipRanges = new ArrayList<String>();
        ipRanges.add(ipAddr);
        ArrayList<IpPermission> ipPermissions = new ArrayList<IpPermission>();
        for (int port : ports) {
            if (opened.contains(port)) {
                opened2.remove(port);
                continue;
            }
            Rt.p("add " + port + " to " + groupName);
            IpPermission ipPermission = new IpPermission();
            ipPermission.setIpProtocol("tcp");
            ipPermission.setFromPort(port);
            ipPermission.setToPort(port);
            ipPermission.setIpRanges(ipRanges);
            ipPermissions.add(ipPermission);
        }
        if (ipPermissions.size() > 0) {
            AuthorizeSecurityGroupIngressRequest ingressRequest = new AuthorizeSecurityGroupIngressRequest(groupName,
                    ipPermissions);
            ec2.authorizeSecurityGroupIngress(ingressRequest);
        }
        if (opened2.size() > 0) {
            Rt.p("TODO: Need to remove these port: " + opened2);
        }
    }

    HashSet<String> accessibleInstances = new HashSet<String>();

    public void grantAccessToLocalMachine(Instance instance) throws Exception {
        if (!accessibleInstances.contains(instance.getInstanceId())) {
            accessibleInstances.add(instance.getInstanceId());
            SSH ssh = ssh(instance);
            try {
                String authorizedKeys = new String(Rt.read(ssh.get("/home/ubuntu/.ssh/authorized_keys")));
                //                R.p(authorizedKeys);
                String pubKey = Rt.readFile(new File(System.getProperty("user.home") + "/.ssh/id_rsa.pub")).trim();
                boolean contains = false;
                for (String s : authorizedKeys.split("\r?\n")) {
                    //                    R.p(s);
                    //                    R.p(pubKey);
                    if (s.trim().equals(pubKey.trim()))
                        contains = true;
                }
                if (!contains) {
                    Rt.p("adding local public key to " + instance.getPublicDnsName() + " for rsync");
                    ssh.put("/tmp/~imru_auto_pubkey.tmp", new ByteArrayInputStream((pubKey + "\n").getBytes()));
                    ssh.execute("cat /tmp/~imru_auto_pubkey.tmp >> ~/.ssh/authorized_keys");
                    ssh.execute("rm /tmp/~imru_auto_pubkey.tmp");
                }
            } finally {
                ssh.close();
            }
        }
    }
}
