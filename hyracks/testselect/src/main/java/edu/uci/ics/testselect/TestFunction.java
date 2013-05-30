package edu.uci.ics.testselect;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.IFunctionInfo;


public class TestFunction implements IFunctionInfo {
    private final FunctionIdentifier fid;

    public TestFunction(FunctionIdentifier fid) {
        this.fid = fid;
    }

    @Override
    public FunctionIdentifier getFunctionIdentifier() {
        return fid;
    }
}