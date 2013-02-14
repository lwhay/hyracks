package edu.uci.ics.hyracks.example.wordcount;

import java.io.File;
import java.util.EnumSet;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.ec2.HyracksEC2Cluster;
import edu.uci.ics.hyracks.imru.example.utils.Client;
import edu.uci.ics.hyracks.imru.util.Rt;

public class WordCountEC2 {
    public static void main(String[] args) throws Exception {
        File home = new File(System.getProperty("user.home"));
        File credentialsFile = new File(home, "AwsCredentials.properties");
        File privateKey = new File(home, "firstTestByRui.pem");
        HyracksEC2Cluster cluster = new HyracksEC2Cluster(credentialsFile, privateKey);
        //connect to hyracks
        IHyracksClientConnection hcc = cluster.getHyracksConnection();

        //update application
        Client.uploadApp(hcc, "text", false, 3288, "/tmp/imruModels");

        try {
            cluster.write(0, "/tmp/a.txt", "0a 1b 1c".getBytes());
            cluster.write(1, "/tmp/b.txt", "0b 1c 1c".getBytes());

            JobSpecification job = WordCountAllInOneExample.createJob(
                    WordCountAllInOneExample.parseFileSplits("NC0:/tmp/a.txt,NC1:/tmp/b.txt"),
                    WordCountAllInOneExample.parseFileSplits("NC0:/tmp/out0.txt,NC1:/tmp/out1.txt"));

            JobId jobId = hcc.startJob("text", job, EnumSet.noneOf(JobFlag.class));
            hcc.waitForCompletion(jobId);

            Rt.np("Output 0:");
            Rt.np(new String(cluster.read(0, "/tmp/out0.txt")));
            Rt.np("Output 1:");
            Rt.np(new String(cluster.read(1, "/tmp/out1.txt")));
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            Thread.sleep(1000);
            System.exit(0);
        }
    }
}
