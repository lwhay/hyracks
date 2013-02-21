package edu.uci.ics.hyracks.imru.example.trainmerge.ann;

import java.io.File;

import edu.uci.ics.hyracks.imru.example.utils.Client;
import edu.uci.ics.hyracks.imru.example.utils.ImruEC2;

public class NNEC2 {
    public static void main(String[] args) throws Exception {
        File home = new File(System.getProperty("user.home"));
        File credentialsFile = new File(home, "AwsCredentials.properties");
        File privateKey = new File(home, "firstTestByRui.pem");
        File hyracksEc2Root = new File(home,
                "fullstack_imru/hyracks/hyracks-ec2/target/appassembler");
        boolean setupClusterFirst = true;
        boolean uploadData = true;
        setupClusterFirst = false;
        uploadData = false;
        String[] localPaths = { "/home/wangrui/b/data/train-labels.idx1-ubyte",
                "/home/wangrui/b/data/train-images.idx3-ubyte", };
        ImruEC2 ec2 = new ImruEC2(credentialsFile, privateKey);
        if (setupClusterFirst)
            ec2.setup(hyracksEc2Root, 2, "t1.micro");
        else {
            ec2.cluster.stopHyrackCluster();
            ec2.cluster.startHyrackCluster();
        }
            
        if (uploadData)
            ec2.spreadData(localPaths, "/home/ubuntu/data/ann");
        String path = "";
        String[] nodes = ec2.cluster.getNodeNames();
        int totalExample = 100;
        int examplesPerNodes = totalExample / nodes.length;
        for (int i = 0; i < nodes.length; i++) {
            int start = examplesPerNodes * i;
            int len = examplesPerNodes;
            if (i > 0)
                path += ",";
            path += nodes[i] + ":/home/ubuntu/data/ann?start=" + start
                    + "&len=" + len;
        }
        try {
            File modelFile = new File("/home/wangrui/b/data/test.txt");
            NeuralNetwork network = new NeuralNetwork(modelFile);
            int turns = 30;
            int transferThreshold = 20;
            NeuralNetwork finalModel = ec2.run(
                    new Job(turns, transferThreshold), network, "ann", path);
            System.out.println("FinalModel: " + finalModel.errorRate);
            network.save(modelFile);
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            System.exit(0);
        }
    }
}
