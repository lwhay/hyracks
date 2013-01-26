package edu.uci.ics.hyracks.imru.example.digitRecognition;

import java.io.IOException;
import java.io.Serializable;

public class Data implements Serializable {
    public int label;
    public byte[][] image;
}
