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

public class KMeansJobV3 extends
        IMRUJobV3<KMeansModel, DataPoint, Serializable> {
    int k;

    public KMeansJobV3(int k) {
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
     * Parse input data and output tuples
     */
    @Override
    public void parse(IHyracksTaskContext ctx, InputStream input,
            DataWriter<DataPoint> output) throws IOException {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(
                    input));
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
    public Serializable map(IHyracksTaskContext ctx, Iterator<DataPoint> input,
            KMeansModel model) throws IOException {
        KMeansCentroids result = new KMeansCentroids(k);
        KMeansStartingPoints startingPoints = new KMeansStartingPoints(k);
        while (input.hasNext()) {
            DataPoint dataPoint = input.next();
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
        }
        return model.firstRound ? startingPoints : result;
    }

    /**
     * Combine multiple results to one result
     */
    @Override
    public Serializable reduce(IHyracksTaskContext ctx,
            Iterator<Serializable> input) throws HyracksDataException {
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
    public void update(IHyracksTaskContext ctx, Iterator<Serializable> input,
            KMeansModel model) throws HyracksDataException {
        Serializable obj = reduce(ctx,input);
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
