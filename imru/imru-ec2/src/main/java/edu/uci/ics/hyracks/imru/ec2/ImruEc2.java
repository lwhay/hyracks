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
import java.io.IOException;

import com.amazonaws.services.ec2.model.Instance;

import edu.uci.ics.hyracks.imru.util.R;

/**
 * Automatically deployment of EC2 cluster sample code
 * 
 * @author wangrui
 */
public class ImruEc2 {
    public static final String IMRU_PREFIX = "IMRU-auto-deploy-";
    File imruRoot;
    HyracksEC2Cluster cluster;

    public ImruEc2(File credentialsFile, File privateKey, File imruRoot) throws Exception {
        cluster = new HyracksEC2Cluster(credentialsFile, privateKey, IMRU_PREFIX);
        this.imruRoot = imruRoot;
        //         startStopTest();
        cluster.createSecurityGroup();
        cluster.setTotalInstances(2);
        cluster.printNodeStatus();
        if (cluster.getTotalPendingMachines() > 0) {
            cluster.waitForInstanceStart();
            cluster.printNodeStatus();
        }
        cluster.sshTest();
        cluster.install(imruRoot);
        cluster.startHyrackCluster();
        cluster.printLogs();
        R.np("Admin URL: " + cluster.getAdminURL());
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

        File imruRoot = new File(home, "fullstack_imru");
        String keyName = "firstTestByRui";
        File privateKey = new File(home, keyName + ".pem");
        if (!privateKey.exists())
            throw new Error("Key pair needed. Please create "
                    + "a key pair in https://console.aws.amazon.com/ec2/ and download it to " + home.getAbsolutePath()
                    + "/");
        ImruEc2 imru = new ImruEc2(credentialsFile, privateKey, imruRoot);
    }
}
