package com.yahoo.scalops.algebricks.expression;

import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;

public class BuiltinFunctionInfo implements IFunctionInfo {

    private FunctionIdentifier fid;

    public BuiltinFunctionInfo(FunctionIdentifier fid) {
        this.fid = fid;
    }

    public FunctionIdentifier getFunctionIdentifier() {
        return fid;
    }

}
