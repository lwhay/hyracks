package edu.uci.ics.hyracks.imru.example.helloworld;

import edu.uci.ics.hyracks.imru.api.IModel;

/**
 * IMRU model which will be used in map() and updated in update()
 * 
 * @author wangrui
 * 
 */
public class HelloWorldModel implements IModel {
    public int totalLength;
    public int roundsRemaining = 5;
}
