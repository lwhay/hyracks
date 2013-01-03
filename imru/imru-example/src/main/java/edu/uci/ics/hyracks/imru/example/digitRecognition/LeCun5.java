package edu.uci.ics.hyracks.imru.example.digitRecognition;

import java.util.Random;

public class LeCun5 extends NNStructure {
    @Override
    public String getName() {
        return "LeCun5";
    }

    public NNLayer[] buildNetwork()  {
        NNLayer[] layers = new NNLayer[5];
        for (int i = 0; i < 5; i++) {
            layers[i] = new NNLayer();
        }
        layers[0].ns = new Neuron[29 * 29];
        layers[0].createWeights(0);
        for (int i = 0; i < layers[0].ns.length; i++) {
            Neuron n = layers[0].ns[i] = new Neuron();
            n.id = i;
            n.createConnections(0);
        }
        buildConvLayer(layers[0], layers[1], 29, 1, 13, 5, 6);
        buildConvLayer(layers[1], layers[2], 13, 6, 5, 5, 50);
        buildFullConnectedLayer(layers[2], layers[3], 5 * 5 * 50, 100);
        buildFullConnectedLayer(layers[3], layers[4], 100, 10);
        Random random = new Random();
        for (NNLayer layer : layers)
            for (int i = 0; i < layer.weights.length; i++)
                layer.weights[i] = random.nextDouble() * 0.02 - 0.01;
        return layers;
    }
}
