package com.yahoo.scalops.function;

import java.io.DataOutput;

import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public interface IClojureEvaluator {

    public void evaluator(IFrameTupleReference tuple, DataOutput output);

}
