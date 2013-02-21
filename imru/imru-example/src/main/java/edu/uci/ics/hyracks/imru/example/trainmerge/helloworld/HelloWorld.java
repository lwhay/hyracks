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

package edu.uci.ics.hyracks.imru.example.trainmerge.helloworld;

import edu.uci.ics.hyracks.imru.example.utils.Client;

/**
 * Upload helloworld example to a cluster and run it.
 */
public class HelloWorld {
    static String[] defaultArgs(boolean debugging) throws Exception {
        // if no argument is given, the following code
        // create default arguments to run the example
        String cmdline = "";
        int totalNodes = 3;
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
        String exampleData = System.getProperty("user.home")
                + "/fullstack_imru/imru/imru-example/data/helloworld";
        // port of cluster controller
        cmdline += " -port 3099";
        // application name
        cmdline += " -app helloworld";
        // Path on cluster controller to hold models
        cmdline += " -cc-temp-path /tmp/cache/cc";
        int n = 52;
        n = 6;
        cmdline += " -example-paths NC0:" + exampleData + "/hello0.txt";
        for (int i = 1; i < n; i++)
            cmdline += ",NC" + (i % totalNodes) + ":" + exampleData + "/hello"
                    + i + ".txt";

        cmdline += " -model-file-name helloworld";

        cmdline = cmdline.trim();
        System.out.println("Using command line: " + cmdline);
        return cmdline.split(" ");
    }

    public static void main(String[] args) throws Exception {
        boolean debug = true;
        if (args.length == 0)
            args = defaultArgs(debug);

        try {
            String finalModel = Client.run(new TrainJob(), "start", args);
            System.out.println("FinalModel: " + finalModel);
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            System.exit(0);
        }
    }
}
