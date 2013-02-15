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

package edu.uci.ics.hyracks.imru.example.kmeans;

import edu.uci.ics.hyracks.imru.example.utils.Client;

/**
 * Start a local cluster within the process and run the kmeans example.
 */
public class KMeans {
    static String[] defaultArgs(boolean debugging) throws Exception {
        // if no argument is given, the following code
        // create default arguments to run the example
        String cmdline = "";
        if (debugging) {
            // debugging mode, everything run in one process
            cmdline += " -debug";
            // disable logging
            cmdline += " -disable-logging";
            // hostname of cluster controller
            cmdline += " -host localhost";
        } else {
            // hostname of cluster controller
            cmdline += "-host " + Client.getLocalIp();
        }

        boolean useHDFS = false;
        if (System.getProperty("local") != null)
            useHDFS = false;
        String home = System.getProperty("user.home");
        String exampleData = home + "/fullstack_imru/imru/imru-example/data/kmeans";
        // port of cluster controller
        cmdline += " -port 3099";
        // application name
        // cmdline += " -app kmeans";
        // hadoop config path
        if (useHDFS)
            cmdline += " -hadoop-conf " + System.getProperty("user.home") + "/hadoop-0.20.2/conf";
        // Input data
        if (useHDFS)
            cmdline += " -example-paths /kmeans/input.txt,/kmeans/input2.txt";
        else
            cmdline += " -example-paths " + exampleData + "/kmeans0.txt," + exampleData + "/kmeans1.txt";
        // aggregation type
        cmdline += " -agg-tree-type generic";
        // aggregation parameter
        cmdline += " -agg-count 1";
        // write to the same file
        cmdline += " -model-file-name kmeans";
        cmdline = cmdline.trim();
        System.out.println("Using command line: " + cmdline);
        return cmdline.split(" ");
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0)
            args = defaultArgs(false);

        int k = 3;

        double minDis = Double.MAX_VALUE;
        KMeansModel bestModel = null;
        for (int modelId = 0; modelId < 3; modelId++) {
            System.out.println("trial " + modelId);
            KMeansModel initModel = Client.run(new RandomSelectJob(k), new KMeansModel(k, 1),args);
            System.out.println("InitModel: " + initModel);

            initModel.roundsRemaining = 20;

            KMeansModel finalModel = Client.run(new KMeansJob(k), initModel,args);
            System.out.println("FinalModel: " + finalModel);
            System.out.println("DistanceSum: " + finalModel.lastDistanceSum);
            if (finalModel.lastDistanceSum < minDis) {
                minDis = finalModel.lastDistanceSum;
                bestModel = finalModel;
            }
        }
        System.out.println("BestModel: " + bestModel);
        System.exit(0);
    }
}
