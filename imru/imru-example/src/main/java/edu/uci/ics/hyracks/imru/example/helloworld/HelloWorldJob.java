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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

import edu.uci.ics.hyracks.imru.api.IMRUContext;
import edu.uci.ics.hyracks.imru.api2.DataWriter;
import edu.uci.ics.hyracks.imru.api2.IIMRUJob;
import edu.uci.ics.hyracks.imru.api2.IMRUDataException;

/**
 * Core IMRU application specific code.
 * The dataflow is parse->map->reduce->update
 */
public class HelloWorldJob implements IIMRUJob<HelloWorldModel, HelloWorldData, HelloWorldResult> {
    /**
     * Return initial model
     */
    @Override
    public HelloWorldModel initModel() {
        return new HelloWorldModel();
    }

    /**
     * Frame size must be large enough to store at least one data object
     */
    @Override
    public int getCachedDataFrameSize() {
        return 256;
    }

    /**
     * Parse input data and output data objects
     */
    @Override
    public void parse(IMRUContext ctx, InputStream input, DataWriter<HelloWorldData> output) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line = reader.readLine();
        reader.close();
        for (String s : line.split(" ")) {
            System.out.println(ctx.getOperatorName()+": " + s);
            output.addData(new HelloWorldData(s));
        }
    }

    /**
     * For a list of data objects, return one result
     */
    @Override
    public HelloWorldResult map(IMRUContext ctx, Iterator<HelloWorldData> input, HelloWorldModel model)
            throws IOException {
        HelloWorldResult result = new HelloWorldResult();
        while (input.hasNext()) {
            String word = input.next().word;
            result.length += word.length();
            System.out.println(ctx.getOperatorName()+": " + word + " -> " + result.length);
        }
        return result;
    }

    /**
     * Combine multiple results to one result
     */
    @Override
    public HelloWorldResult reduce(IMRUContext ctx, Iterator<HelloWorldResult> input)
            throws IMRUDataException {
        HelloWorldResult combined = new HelloWorldResult();
        StringBuilder sb = new StringBuilder();
        while (input.hasNext()) {
            HelloWorldResult result = input.next();
            sb.append(result.length + "+");
            combined.length += result.length;
        }
        if (sb.length() > 0)
            sb.deleteCharAt(sb.length() - 1);
        System.out.println(ctx.getOperatorName()+": " + sb + " -> " + combined.length);
        return combined;
    }

    /**
     * update the model using combined result
     */
    @Override
    public void update(IMRUContext ctx, Iterator<HelloWorldResult> input, HelloWorldModel model)
            throws IMRUDataException {
        StringBuilder sb = new StringBuilder();
        int oldLength = model.totalLength;
        while (input.hasNext()) {
            HelloWorldResult result = input.next();
            sb.append("+" + result.length);
            model.totalLength += result.length;
        }
        System.out.println(ctx.getOperatorName()+": " + oldLength + sb + " -> " + model.totalLength);
        model.roundsRemaining--;
    }

    /**
     * Return true to exit loop
     */
    @Override
    public boolean shouldTerminate(HelloWorldModel model) {
        return model.roundsRemaining == 0;
    }
}
