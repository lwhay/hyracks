package edu.uci.ics.hyracks.imru.example.trainmerge.helloworld;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;

import edu.uci.ics.hyracks.imru.file.IMRUFileSplit;
import edu.uci.ics.hyracks.imru.trainmerge.TrainMergeContext;
import edu.uci.ics.hyracks.imru.trainmerge.TrainMergeJob;
import edu.uci.ics.hyracks.imru.util.Rt;

public class TrainJob implements TrainMergeJob<String> {
    int curNodeId = -1;

    @Override
    public void process(TrainMergeContext context, IMRUFileSplit input,
            String model, int curNodeId, int totalNodes) throws IOException {
        this.curNodeId = curNodeId;
        BufferedReader reader = input.getReader();
        int targetNode = curNodeId;
        for (String line = reader.readLine(); line != null; line = reader
                .readLine()) {
            targetNode = (targetNode + 1) % totalNodes;
            if (targetNode == curNodeId)
                targetNode = (targetNode + 1) % totalNodes;
            String updatedModel = line + "(" + curNodeId + "->" + targetNode
                    + ")";
            Rt.p(model + " -> " + updatedModel);
            context.send(updatedModel, targetNode);
        }
        try {
            Thread.sleep(500); //give some time to reply
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        context.setModel("end");
    }

    @Override
    public void receive(TrainMergeContext context, int sourceParition,
            Serializable receivedObject) throws IOException {
        Rt.p("received at (" + curNodeId + "): " + receivedObject);
        if (!receivedObject.toString().contains("reply")) {
            if (!context.send(receivedObject + " reply (" + curNodeId + "->"
                    + sourceParition + ")", sourceParition))
                Rt.p("reply faild (all writer closed)");
        }
    }
};