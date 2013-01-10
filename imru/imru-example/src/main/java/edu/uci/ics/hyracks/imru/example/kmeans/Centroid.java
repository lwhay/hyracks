package edu.uci.ics.hyracks.imru.example.kmeans;

import java.io.Serializable;

public class Centroid implements Serializable {
    double x, y;
    int count = 0;

    public double dis(DataPoint dp) {
        return Math.sqrt((x - dp.x) * (x - dp.x) + (y - dp.y) * (y - dp.y));
    }

    public void add(DataPoint dp) {
        x += dp.x;
        y += dp.y;
        count++;
    }

    public void add(Centroid c) {
        x += c.x;
        y += c.y;
        count += c.count;
    }

    public void set(DataPoint p) {
        x = p.x;
        y = p.y;
    }

    @Override
    public String toString() {
        return "(" + x + "," + y + ")";
    }
}
