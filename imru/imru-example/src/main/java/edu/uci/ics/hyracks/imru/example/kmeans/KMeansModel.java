package edu.uci.ics.hyracks.imru.example.kmeans;

import java.util.Random;

import edu.uci.ics.hyracks.imru.api.IModel;

/**
 * IMRU model which will be used in map() and updated in update()
 */
public class KMeansModel implements IModel {
    Centroid[] centroids;
    public int roundsRemaining = 20;
    public boolean firstRound = true;

    public KMeansModel(int k) {
        centroids = new Centroid[k];
        for (int i = 0; i < k; i++)
            centroids[i] = new Centroid();
    }

}
