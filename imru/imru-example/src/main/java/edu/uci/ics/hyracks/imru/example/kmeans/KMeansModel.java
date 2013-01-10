package edu.uci.ics.hyracks.imru.example.kmeans;

import java.util.Random;

import edu.uci.ics.hyracks.imru.api.IModel;

/**
 * IMRU model which will be used in map() and updated in update()
 */
public class KMeansModel implements IModel {
    Centroid[] centroids;

    public KMeansModel(int k) {
        centroids = new Centroid[k];
        Random random = new Random();
        for (int i = 0; i < k; i++) {
            centroids[i] = new Centroid();
            centroids[i].x = centroids[i].y = random.nextDouble() * 20;
        }
    }

    public int roundsRemaining = 20;
}
