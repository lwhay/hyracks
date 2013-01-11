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
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.imru.api2.IMRUJob;
import edu.uci.ics.hyracks.imru.api2.TupleReader;
import edu.uci.ics.hyracks.imru.api2.TupleWriter;

/**
 * Core IMRU application specific code. The dataflow is
 * parse->map->reduce->update
 */
public class KMeansJob implements IMRUJob<KMeansModel, Serializable> {
    int k;

    public KMeansJob(int k) {
        this.k = k;
    }

    /**
     * Return initial model
     */
    @Override
    public KMeansModel initModel() {
        KMeansModel initModel = new KMeansModel(k);
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
     * Number of fields for each tuple
     */
    @Override
    public int getFieldCount() {
        return 1;
    }

    /**
     * Parse input data and output tuples
     */
    @Override
    public void parse(IHyracksTaskContext ctx, InputStream input,
            TupleWriter output) throws HyracksDataException {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    input));
            while (true) {
                String line = reader.readLine();
                if (line == null)
                    break;
                System.out.println("parse: " + line);
                String[] ss = line.split("[ |\t]+");
                output.writeDouble(Double.parseDouble(ss[0]));
                output.writeDouble(Double.parseDouble(ss[1]));
                output.finishField();
                output.finishTuple();
            }
            reader.close();
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    /**
     * For each tuple, return one result. Or by using nextTuple(), return one
     * result after processing multiple tuples.
     */
    @Override
    public Serializable map(TupleReader input, KMeansModel model,
            int cachedDataFrameSize) throws IOException {
        KMeansCentroids result = new KMeansCentroids(k);
        KMeansStartingPoints startingPoints = new KMeansStartingPoints(k);
        while (true) {
            input.seekToField(0);
            DataPoint dataPoint = new DataPoint();
            dataPoint.x = input.readDouble();
            dataPoint.y = input.readDouble();
            if (model.firstRound) {
                // In the first round, random select some data points as
                // starting points
                startingPoints.add(dataPoint);
            } else {
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
            if (!input.hasNextTuple())
                break;
            input.nextTuple();
        }
        return model.firstRound ? startingPoints : result;
    }

    /**
     * Combine multiple results to one result
     */
    @Override
    public Serializable reduce(Iterator<Serializable> input)
            throws HyracksDataException {
        KMeansCentroids combined = new KMeansCentroids(k);
        KMeansStartingPoints startingPoints = null;
        boolean firstRound = false;
        while (input.hasNext()) {
            Serializable obj = input.next();
            if (obj instanceof KMeansCentroids) {
                KMeansCentroids result = (KMeansCentroids) obj;
                for (int i = 0; i < k; i++) {
                    combined.centroids[i].add(result.centroids[i]);
                }
            } else {
                firstRound = true;
                KMeansStartingPoints result = (KMeansStartingPoints) obj;
                if (startingPoints == null)
                    startingPoints = result;
                else
                    startingPoints.add(result);
            }

        }
        return firstRound ? startingPoints : combined;
    }

    /**
     * update the model using combined result
     */
    @Override
    public void update(Iterator<Serializable> input, KMeansModel model)
            throws HyracksDataException {
        Serializable obj = reduce(input);
        if (model.firstRound) {
            KMeansStartingPoints startingPoints = (KMeansStartingPoints) obj;
            for (int i = 0; i < k; i++) {
                model.centroids[i].set(startingPoints.ps[i]);
            }
            System.out.println("InitModel:");
            for (int i = 0; i < k; i++) {
                System.out.println(" " + model.centroids[i]);
            }
        } else {
            KMeansCentroids combined = (KMeansCentroids) obj;
            for (int i = 0; i < k; i++) {
                model.centroids[i].x = combined.centroids[i].x
                        / combined.centroids[i].count;
                model.centroids[i].y = combined.centroids[i].y
                        / combined.centroids[i].count;
            }
        }
        model.roundsRemaining--;
        if (model.firstRound)
            model.firstRound = false;
    }

    /**
     * Return true to exit loop
     */
    @Override
    public boolean shouldTerminate(KMeansModel model) {
        return model.roundsRemaining == 0;
    }
}
