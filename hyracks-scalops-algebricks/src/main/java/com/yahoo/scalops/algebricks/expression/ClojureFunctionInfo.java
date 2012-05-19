package com.yahoo.scalops.algebricks.expression;

import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class ClojureFunctionInfo implements IFunctionInfo {

    private FunctionIdentifier fid;

    public ClojureFunctionInfo(String functionName) {
        fid = new FunctionIdentifier("scalops", functionName, false);
    }

    public FunctionIdentifier getFunctionIdentifier() {
        return fid;
    }

}
