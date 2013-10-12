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

package edu.uci.ics.hyracks.imru.example.bgd;

import edu.uci.ics.hyracks.imru.example.utils.Client;

/**
 * Batch Gradient Descent example
 */
public class BGD {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            String cmdline = "";
            if (Client.isServerAvailable(Client.getLocalIp(), 3099)) {
                // hostname of cluster controller
                cmdline += "-host " + Client.getLocalIp() + " -port 3099";
                System.out.println("Connecting to " + Client.getLocalIp());
            } else {
                // debugging mode, everything run in one process
                cmdline += "-host localhost -port 3099 -debug -disable-logging";
                System.out.println("Starting hyracks cluster");
            }

            String exampleData = System.getProperty("user.home")
                    + "/hyracks/imru/imru-example/data/bgd/bgd.txt";
            cmdline += " -example-paths " + exampleData;

            System.out.println("Using command line: " + cmdline);
            args = cmdline.split(" ");
        }

        int numRounds = 15;
        int features = 3;
        Model model = Client.run(new BGDJob(features), new Model(features,
                numRounds), args);
        System.out.println("Rounds: " + model.roundsCompleted);
        System.out.println("Model:");
        for (int i = 0; i < model.weights.length; i++)
            System.out.println(i + ":\t" + model.weights[i]);
        System.out.println("Error: " + model.error + "%");
        System.exit(0);
    }
}
