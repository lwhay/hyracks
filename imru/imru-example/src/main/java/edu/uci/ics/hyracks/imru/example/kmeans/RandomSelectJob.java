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
import java.io.Serializable;
import java.util.Iterator;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.imru.api2.DataWriter;
import edu.uci.ics.hyracks.imru.api2.IMRUJob;
import edu.uci.ics.hyracks.imru.api2.IMRUJobV3;
import edu.uci.ics.hyracks.imru.api2.TupleReader;
import edu.uci.ics.hyracks.imru.api2.TupleWriter;

/**
 * Random select data examples as centroids
 * 
 * @author wangrui
 */
public class RandomSelectJob extends IMRUJobV3<KMeansModel, DataPoint, KMeansStartingPoints> {
    int k;

    public RandomSelectJob(int k) {
        this.k = k;
    }

    /**
     * Return initial model
     */
    @Override
    public KMeansModel initModel() {
        KMeansModel initModel = new KMeansModel(k);
        initModel.roundsRemaining=1;
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
    public KMeansStartingPoints map(IHyracksTaskContext ctx, Iterator<DataPoint> input, KMeansModel model)
            throws IOException {
        KMeansStartingPoints startingPoints = new KMeansStartingPoints(k);
        while (input.hasNext()) {
            DataPoint dataPoint = input.next();
            // random select some data points as
            // starting points
            startingPoints.add(dataPoint);
        }
        return startingPoints;
    }

    /**
     * Combine multiple results to one result
     */
    @Override
    public KMeansStartingPoints reduce(IHyracksTaskContext ctx, Iterator<KMeansStartingPoints> input)
            throws HyracksDataException {
        KMeansStartingPoints startingPoints = null;
        while (input.hasNext()) {
            KMeansStartingPoints result = input.next();
            if (startingPoints == null)
                startingPoints = result;
            else
                startingPoints.add(result);

        }
        return startingPoints;
    }

    /**
     * update the model using combined result
     */
    @Override
    public void update(IHyracksTaskContext ctx, Iterator<KMeansStartingPoints> input, KMeansModel model)
            throws HyracksDataException {
        KMeansStartingPoints obj = reduce(ctx, input);
        KMeansStartingPoints startingPoints = (KMeansStartingPoints) obj;
        for (int i = 0; i < k; i++) {
            model.centroids[i].set(startingPoints.ps[i]);
        }
        System.out.println("InitModel:"+model.roundsRemaining);
        for (int i = 0; i < k; i++) {
            System.out.println(" " + model.centroids[i]);
        }
        model.roundsRemaining--;
    }

    /**
     * Return true to exit loop
     */
    @Override
    public boolean shouldTerminate(KMeansModel model) {
        return model.roundsRemaining <= 0;
    }
}
