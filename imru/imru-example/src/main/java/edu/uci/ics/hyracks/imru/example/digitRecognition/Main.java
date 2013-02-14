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

package edu.uci.ics.hyracks.imru.example.digitRecognition;

import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.imru.example.utils.Client;

/**
 * Start a local cluster within the process and run the helloworld example.
 * 
 * @author wangrui
 */
public class Main {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            // if no argument is given, the following code
            // create default arguments to run the example
            String cmdline = "";
            // debugging mode, everything run in one process
            cmdline += "-debug";
            // disable logging
            cmdline += " -disable-logging";
            // hostname of cluster controller
            cmdline += " -host localhost";
            // port of cluster controller
            cmdline += " -port 3099";
            // application name
            cmdline += " -app digitRecognition";
            // hadoop config path
            cmdline += " -hadoop-conf " + System.getProperty("user.home")
                    + "/hadoop-0.20.2/conf";
            // path of input data
            cmdline += " -example-paths /mnist/examples.zip";
            // aggregation type
            cmdline += " -agg-tree-type generic";
            // aggregation parameter
            cmdline += " -agg-count 1";
            // use the same model
            cmdline += " -model-file-name digitRecognition";
            System.out.println("Using command line: " + cmdline);
            args = cmdline.split(" ");
        }

        NeuralNetwork finalModel = Client.run(new Job(),new NeuralNetwork(), args);
        System.out.println("FinalModel: " + finalModel.errorRate);
        System.exit(0);
    }
}
