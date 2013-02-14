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

package edu.uci.ics.hyracks.imru.example.helloworld;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.imru.example.utils.Client;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * Upload helloworld example to a cluster and run it.
 */
public class HelloWorld {
    static String[] defaultArgs(boolean debugging) throws Exception {
        // if no argument is given, the following code
        // create default arguments to run the example
        String cmdline = "";
        int totalNodes = 5;
        if (debugging) {
            // debugging mode, everything run in one process
            cmdline += " -debug";
            // disable logging
            cmdline += " -disable-logging";
            // hostname of cluster controller
            cmdline += " -host localhost";
            cmdline += " -debugNodes " + totalNodes;
        } else {
            // hostname of cluster controller
            cmdline += "-host " + Client.getLocalIp();
        }
        boolean useHDFS = false;
        String exampleData = System.getProperty("user.home") + "/fullstack_imru/imru/imru-example/data/helloworld";
        // port of cluster controller
        cmdline += " -port 3099";
        // application name
        cmdline += " -app helloworld";
        // hadoop config path
        if (useHDFS)
            cmdline += " -hadoop-conf " + System.getProperty("user.home") + "/hadoop-0.20.2/conf";
        // Path on local machine to save intermediate models
//        cmdline += " -save-intermediate-models /tmp/cache/models";
        // Path on cluster controller to hold models
        cmdline += " -cc-temp-path /tmp/cache/cc";
        // HDFS path of input data
        if (useHDFS)
            cmdline += " -example-paths /helloworld/input.txt,/helloworld/input2.txt";
        else {
            int n = 52;
            n = 6;
            if (debugging) {
                cmdline += " -example-paths NC0:" + exampleData + "/hello0.txt";
                for (int i = 1; i < n; i++)
                    cmdline += ",NC" + (i % totalNodes) + ":" + exampleData + "/hello" + i + ".txt";
            } else {
                cmdline += " -example-paths " + exampleData + "/hello0.txt";
                for (int i = 1; i < n; i++)
                    cmdline += "," + exampleData + "/hello" + i + ".txt";
            }
        }
        // aggregation
        //        cmdline += " -agg-tree-type nary -fan-in 2";
        cmdline += " -agg-tree-type generic -agg-count 1";
        // cmdline += " -agg-tree-type none";

        // write to the same file
        cmdline += " -model-file-name helloworld";
        //        cmdline += " -model-file-name helloworld";

        cmdline = cmdline.trim();
        System.out.println("Using command line: " + cmdline);
        return cmdline.split(" ");
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0)
            //            args = defaultArgs(false);
            args = defaultArgs(true);

        String finalModel = Client.run(new HelloWorldJob(), "", args);
        System.out.println("FinalModel: " + finalModel);
        System.exit(0);
    }
}
