package edu.uci.ics.hyracks.imru.example.kmeans;

import java.io.Serializable;
import java.util.Random;

public class KMeansStartingPoints implements Serializable {
    DataPoint[] ps;
    int n = 0;
    Random random = new Random();

    public KMeansStartingPoints(int k) {
        ps = new DataPoint[k];
    }

    public void add(DataPoint dp) {
        if (n < ps.length) {
            ps[n] = dp;
        } else {
            int position = random.nextInt(n + 1);
            if (position < ps.length)
                ps[position] = dp;
        }
        n++;
    }

    public void add(KMeansStartingPoints result) {
        for (int i = 0; i < ps.length; i++) {
            if (random.nextInt(n + result.n) >= n)
                ps[i] = result.ps[i];
        }
        n += result.n;
    }
}
