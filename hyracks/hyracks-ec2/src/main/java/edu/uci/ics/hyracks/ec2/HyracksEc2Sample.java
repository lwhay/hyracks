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
import java.io.IOException;

import com.amazonaws.services.ec2.model.Instance;

/**
 * Automatically deployment of EC2 cluster sample code
 * 
 * @author wangrui
 */
public class HyracksEc2Sample {
    public static final String IMRU_PREFIX = "hyracks-auto-deploy-";
    File imruRoot;
    HyracksEC2Cluster cluster;

    public HyracksEc2Sample(File credentialsFile, File privateKey, File imruRoot) throws Exception {
        cluster = new HyracksEC2Cluster(credentialsFile, privateKey, IMRU_PREFIX);
        this.imruRoot = imruRoot;
        //         startStopTest();
        cluster.createSecurityGroup();
        cluster.setTotalInstances(2);
        cluster.printNodeStatus();
        if (cluster.getTotalMachines("pending") > 0) {
            cluster.waitForInstanceStart();
            cluster.printNodeStatus();
        }
        cluster.sshTest();
        cluster.install(imruRoot);
        cluster.startHyrackCluster();
        cluster.printLogs(-1,50);
        Rt.np("Admin URL: " + cluster.getAdminURL());
    }

    void startStopTest() throws Exception {
        cluster.stopInstances();
        cluster.waitForInstanceStop();
        cluster.startInstances();
        cluster.waitForInstanceStart();
        cluster.waitSSH();
        cluster.sshTest();
    }

    public static void main(String[] args) throws Exception {
        File home = new File(System.getProperty("user.home"));
        File credentialsFile = new File(home, "AwsCredentials.properties");
        if (!credentialsFile.exists())
            throw new IOException(
                    credentialsFile.getAbsolutePath()
                            + " doesn't exist.\r\n"
                            + "Insert your AWS Credentials from http://aws.amazon.com/security-credentials to a file with content\r\naccessKey=xx\r\n"
                            + "secretKey=xx");

        File hyracksEc2Root = new File(home, "fullstack_imru/hyracks/hyracks-ec2/target/appassembler");
        String keyName = "firstTestByRui";
        File privateKey = new File(home, keyName + ".pem");
        if (!privateKey.exists())
            throw new Error("Key pair needed. Please create "
                    + "a key pair in https://console.aws.amazon.com/ec2/ and download it to " + home.getAbsolutePath()
                    + "/");
        HyracksEc2Sample imru = new HyracksEc2Sample(credentialsFile, privateKey, hyracksEc2Root);
    }
}
