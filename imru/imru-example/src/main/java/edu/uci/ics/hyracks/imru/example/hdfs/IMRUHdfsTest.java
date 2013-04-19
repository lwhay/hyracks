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

package edu.uci.ics.hyracks.imru.example.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

import edu.uci.ics.hyracks.imru.api.DataWriter;
import edu.uci.ics.hyracks.imru.api.IIMRUJob;
import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api.IMRUDataException;
import edu.uci.ics.hyracks.imru.api.IMRUReduceContext;
import edu.uci.ics.hyracks.imru.example.utils.Client;

/**
 * This example demonstrate how data flows through IMRU.
 * The input is six files. Each file has one character.
 * The map operator pass each file's content to reduce operator.
 * The reduce operator pass the combined content to update operator.
 * The final model is the combined content annotated with each
 * operator.
 */
public class IMRUHdfsTest {
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            String cmdline = "";
            cmdline += "-host localhost -port 3099 -debug -disable-logging";
            cmdline += " -debugNodes 2";
            cmdline += " -hadoop-conf /home/wangrui/a/imru/hadoop-0.20.2/conf";
            cmdline += " -example-paths /helloworld/hello0.txt,/helloworld/hello1.txt";
            System.out.println("Using command line: " + cmdline);
            args = cmdline.split(" ");
        }

        String finalModel = Client.run(new IIMRUJob<String, String, String>() {
            @Override
            public int getCachedDataFrameSize() {
                return 256;
            }

            @Override
            public void parse(IMRUContext ctx, InputStream input,
                    DataWriter<String> output) throws IOException {
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(input));
                String line = reader.readLine();
                reader.close();
                for (String s : line.split(" ")) {
                    System.out.println(ctx.getNodeId() + "-"
                            + ctx.getOperatorName() + ": " + s);
                    output.addData(s);
                }
            }

            @Override
            public String map(IMRUContext ctx, Iterator<String> input,
                    String model) throws IOException {
                return input.next();
            }

            @Override
            public String reduce(IMRUContext ctx, Iterator<String> input)
                    throws IMRUDataException {
                String combined = new String();
                while (input.hasNext())
                    combined += input.next();
                return combined;
            }

            @Override
            public String update(IMRUContext ctx, Iterator<String> input,
                    String model) throws IMRUDataException {
                return reduce(ctx, input);
            }

            @Override
            public boolean shouldTerminate(String model) {
                return true;
            }
        }, "", args);
        System.out.println("FinalModel: " + finalModel);
        System.exit(0);
    }
}
