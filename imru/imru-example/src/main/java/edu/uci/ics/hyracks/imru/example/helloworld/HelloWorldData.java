package edu.uci.ics.hyracks.imru.example.helloworld;

import java.io.Serializable;

public class HelloWorldData implements Serializable {
    String word;

    public HelloWorldData(String word) {
        this.word=word;
    }
}
