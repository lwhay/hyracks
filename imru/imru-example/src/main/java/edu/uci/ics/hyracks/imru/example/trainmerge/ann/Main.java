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

package edu.uci.ics.hyracks.imru.example.trainmerge.ann;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import edu.uci.ics.hyracks.imru.example.utils.Client;
import edu.uci.ics.hyracks.imru.util.Rt;

/**
 * To run this demo, download MINST database from
 * http://yann.lecun.com/exdb/mnist/
 * Two files needed:
 * http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz
 * http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz
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
            // path of input data
            cmdline += " -example-paths /home/wangrui/b/data?start=0&len=100";
            // use the same model
            cmdline += " -model-file-name digitRecognition";
            System.out.println("Using command line: " + cmdline);
            args = cmdline.split(" ");
        }

        try {
            File modelFile = new File("/home/wangrui/b/data/test.txt");
            NeuralNetwork network = new NeuralNetwork(modelFile);
            int turns = 30;
            int transferThreshold = 20;
            NeuralNetwork finalModel = Client.run(new Job(turns,
                    transferThreshold), network, args);
            System.out.println("FinalModel: " + finalModel.errorRate);
            network.save(modelFile);
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            System.exit(0);
        }
    }
}
