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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.imru.api2.DataWriter;
import edu.uci.ics.hyracks.imru.api2.IMRUJobV3;

public class KMeansJobV3 extends IMRUJobV3<KMeansModel, DataPoint, KMeansCentroids> {
    int k;
    KMeansModel initModel;

    public KMeansJobV3(int k, KMeansModel initModel) {
        this.k = k;
        this.initModel = initModel;
    }

    /**
     * Return initial model
     */
    @Override
    public KMeansModel initModel() {
        return initModel;
    }

    /**
     * Frame size must be large enough to store at least one tuple
     */
    @Override
    public int getCachedDataFrameSize() {
        return 256;
    }

    /**
     * Parse input data and output tuples
     */
    @Override
    public void parse(IHyracksTaskContext ctx, InputStream input, DataWriter<DataPoint> output) throws IOException {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
            while (true) {
                String line = reader.readLine();
                if (line == null)
                    break;
                System.out.println("parse: " + line);
                String[] ss = line.split("[ |\t]+");
                DataPoint dataPoint = new DataPoint();
                dataPoint.x = Double.parseDouble(ss[0]);
                dataPoint.y = Double.parseDouble(ss[1]);
                output.addData(dataPoint);
            }
            reader.close();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public KMeansCentroids map(IHyracksTaskContext ctx, Iterator<DataPoint> input, KMeansModel model)
            throws IOException {
        KMeansCentroids result = new KMeansCentroids(k);
        while (input.hasNext()) {
            DataPoint dataPoint = input.next();
            // Classify data points using existing centroids
            double min = Double.MAX_VALUE;
            int belong = -1;
            for (int i = 0; i < k; i++) {
                double dis = model.centroids[i].dis(dataPoint);
                if (dis < min) {
                    min = dis;
                    belong = i;
                }
            }
            result.centroids[belong].add(dataPoint);
        }
        //        System.out.println("map "+model);
        return result;
    }

    /**
     * Combine multiple results to one result
     */
    @Override
    public KMeansCentroids reduce(IHyracksTaskContext ctx, Iterator<KMeansCentroids> input) throws HyracksDataException {
        KMeansCentroids combined = new KMeansCentroids(k);
        while (input.hasNext()) {
            KMeansCentroids result = input.next();
            for (int i = 0; i < k; i++)
                combined.centroids[i].add(result.centroids[i]);
        }
        return combined;
    }

    /**
     * update the model using combined result
     */
    @Override
    public void update(IHyracksTaskContext ctx, Iterator<KMeansCentroids> input, KMeansModel model)
            throws HyracksDataException {
        KMeansCentroids combined = reduce(ctx, input);
        boolean changed = false;
        for (int i = 0; i < k; i++)
            changed = changed || model.centroids[i].set(combined.centroids[i]);
        model.roundsRemaining--;
        if (!changed)
            model.roundsRemaining = 0;
        System.out.println("Model: " + model);
    }

    /**
     * Return true to exit loop
     */
    @Override
    public boolean shouldTerminate(KMeansModel model) {
        return model.roundsRemaining <= 0;
    }
}
