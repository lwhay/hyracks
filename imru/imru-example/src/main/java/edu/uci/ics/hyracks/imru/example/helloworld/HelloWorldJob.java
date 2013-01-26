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

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.imru.api2.DataWriter;
import edu.uci.ics.hyracks.imru.api2.IMRUJob;

/**
 * Core IMRU application specific code.
 * The dataflow is parse->map->reduce->update
 */
public class HelloWorldJob extends IMRUJob<HelloWorldModel, HelloWorldData, HelloWorldResult> {
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
    public void parse(IHyracksTaskContext ctx, InputStream input, DataWriter<HelloWorldData> output) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String line = reader.readLine();
        reader.close();
        for (String s : line.split(" ")) {
            System.out.println("parse: " + s);
            output.addData(new HelloWorldData(s));
        }
    }

    /**
     * For a list of data objects, return one result
     */
    @Override
    public HelloWorldResult map(IHyracksTaskContext ctx, Iterator<HelloWorldData> input, HelloWorldModel model)
            throws IOException {
        HelloWorldResult result = new HelloWorldResult();
        while (input.hasNext()) {
            String word = input.next().word;
            result.length += word.length();
            System.out.println("map: " + word + " -> " + result.length);
        }
        return result;
    }

    /**
     * Combine multiple results to one result
     */
    @Override
    public HelloWorldResult reduce(IHyracksTaskContext ctx, Iterator<HelloWorldResult> input)
            throws HyracksDataException {
        HelloWorldResult combined = new HelloWorldResult();
        StringBuilder sb = new StringBuilder();
        while (input.hasNext()) {
            HelloWorldResult result = input.next();
            sb.append(result.length + "+");
            combined.length += result.length;
        }
        if (sb.length() > 0)
            sb.deleteCharAt(sb.length() - 1);
        System.out.println("reduce: " + sb + " -> " + combined.length);
        return combined;
    }

    /**
     * update the model using combined result
     */
    @Override
    public void update(IHyracksTaskContext ctx, Iterator<HelloWorldResult> input, HelloWorldModel model)
            throws HyracksDataException {
        StringBuilder sb = new StringBuilder();
        int oldLength = model.totalLength;
        while (input.hasNext()) {
            HelloWorldResult result = input.next();
            sb.append("+" + result.length);
            model.totalLength += result.length;
        }
        System.out.println("update: " + oldLength + sb + " -> " + model.totalLength);
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
