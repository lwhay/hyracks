/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.imru.example.bgd.deserialized;

import java.io.Serializable;

import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;
import edu.uci.ics.hyracks.imru.api.IIMRUTupleParserFactory;
import edu.uci.ics.hyracks.imru.deserialized.AbstractDeserializingIMRUJobSpecification;
import edu.uci.ics.hyracks.imru.deserialized.IDeserializedMapFunctionFactory;
import edu.uci.ics.hyracks.imru.deserialized.IDeserializedReduceFunctionFactory;
import edu.uci.ics.hyracks.imru.deserialized.IDeserializedUpdateFunctionFactory;
import edu.uci.ics.hyracks.imru.example.bgd.data.LibsvmExampleTupleParserFactory;
import edu.uci.ics.hyracks.imru.example.bgd.data.LinearModel;
import edu.uci.ics.hyracks.imru.example.bgd.deserialized.DeserializedBGDJobSpecification.LossGradient;

public class DeserializedBGDJobSpecification extends
        AbstractDeserializingIMRUJobSpecification<LinearModel, LossGradient> {

    public static class LossGradient implements Serializable {
        float loss;
        float[] gradient;
    }

    private final int numFeatures;


    public DeserializedBGDJobSpecification(int numFeatures) {
        this.numFeatures = numFeatures;
    }

    @Override
    public int getCachedDataFrameSize() {
        return 1024 * 1024;
    }

    @Override
    public IIMRUTupleParserFactory getTupleParserFactory() {
        return new LibsvmExampleTupleParserFactory(numFeatures);
    }

    @Override
    public boolean shouldTerminate(LinearModel model) {
        return model.roundsRemaining == 0;
    }

    @Override
    public IDeserializedMapFunctionFactory<LossGradient, LinearModel> getDeserializedMapFunctionFactory() {
        return new DeserializedBGDMapFunctionFactory();
    }

    @Override
    public IDeserializedReduceFunctionFactory<LossGradient> getDeserializedReduceFunctionFactory() {
        return new DeserializedBGDReduceFunctionFactory();
    }

    @Override
    public IDeserializedUpdateFunctionFactory<LossGradient, LinearModel> getDeserializedUpdateFunctionFactory() {
        return new DeserializedBGDUpdateFunctionFactory();
    }

}
