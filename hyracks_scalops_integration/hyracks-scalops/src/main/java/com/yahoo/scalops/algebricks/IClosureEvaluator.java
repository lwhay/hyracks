package com.yahoo.scalops.algebricks;

import java.io.DataOutput;

import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public abstract class IClosureEvaluator implements IFunctionInfo {

    public FunctionIdentifier getFunctionIdentifier() {
    	return new FunctionIdentifier("scalops", getName(), false);
    }
    
	public abstract String getName();
	
    public abstract void evaluator(IFrameTupleReference tuple, DataOutput output);

}
