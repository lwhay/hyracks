package edu.uci.ics.hyracks.imru.example.bgd;

import java.io.Serializable;

public class Data implements Serializable {
    public int label;
    public int[] fieldIds;
    public float[] values;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(label);
        for (int i = 0; i < fieldIds.length; i++)
            sb.append(" " + fieldIds[i] + ":" + values[i]);
        return sb.toString();
    }
}
