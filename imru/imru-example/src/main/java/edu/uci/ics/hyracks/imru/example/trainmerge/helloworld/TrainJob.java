package edu.uci.ics.hyracks.imru.example.trainmerge.helloworld;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Random;

import edu.uci.ics.hyracks.imru.file.IMRUFileSplit;
import edu.uci.ics.hyracks.imru.trainmerge.TrainMergeContext;
import edu.uci.ics.hyracks.imru.trainmerge.TrainMergeJob;
import edu.uci.ics.hyracks.imru.util.Rt;

public class TrainJob implements TrainMergeJob<String> {
    @Override
    public void train(TrainMergeContext<String> context, IMRUFileSplit input,
            String model, int curMergerId, int totalMergers) throws IOException {
        BufferedReader reader = input.getReader();
        int targetMerger = curMergerId;
        for (String line = reader.readLine(); line != null; line = reader
                .readLine()) {
            // Because model is a string in this example which 
            // won't be updated automatically,
            // so we need to retrieve it in each turn.
            // But for a object model, there is no need to do this
            model = (String) context.getModel();
            String updatedModel = model + " " + line + "("
                    + context.getNodeId() + ")";
            context.setModel(updatedModel);
            Rt.p(model + " -> " + updatedModel);
            Rt.p(input.getPath() + " " + context.getNodeId() + " "
                    + curMergerId);
            targetMerger = (targetMerger + 1) % totalMergers;
            if (targetMerger == curMergerId)
                targetMerger = (targetMerger + 1) % totalMergers;
            context.send(targetMerger);
        }
        Rt.p("exit "+totalMergers);
    }

    @Override
    public String merge(TrainMergeContext<String> context, String model,
            String receivedModel) throws IOException {
        Rt.p("merge model (" + context.getNodeId() + "): " + model + " with "
                + receivedModel);
        return model + " " + receivedModel;
    }
};