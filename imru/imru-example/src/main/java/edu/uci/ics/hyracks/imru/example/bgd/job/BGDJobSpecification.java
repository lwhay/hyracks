package edu.uci.ics.hyracks.imru.example.bgd.job;

import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;
import edu.uci.ics.hyracks.imru.api.IIMRUJobSpecification;
import edu.uci.ics.hyracks.imru.api.IMapFunctionFactory;
import edu.uci.ics.hyracks.imru.api.IReduceFunctionFactory;
import edu.uci.ics.hyracks.imru.api.IUpdateFunctionFactory;
import edu.uci.ics.hyracks.imru.example.bgd.data.LibsvmExampleTupleParserFactory;
import edu.uci.ics.hyracks.imru.example.bgd.data.LinearModel;

public class BGDJobSpecification implements IIMRUJobSpecification<LinearModel> {

    private static final long serialVersionUID = 1L;
    private final int numFeatures;


    public BGDJobSpecification(int numFeatures) {
        this.numFeatures = numFeatures;
    }

    @Override
    public int getCachedDataFrameSize() {
        return 1024 * 1024;
    }

    @Override
    public ITupleParserFactory getTupleParserFactory() {
        return new LibsvmExampleTupleParserFactory(numFeatures);
    }

    @Override
    public IMapFunctionFactory<LinearModel> getMapFunctionFactory() {
        return new BGDMapFunctionFactory();
    }

    @Override
    public IReduceFunctionFactory getReduceFunctionFactory() {
        return new BGDReduceFunctionFactory(numFeatures);
    }

    @Override
    public IUpdateFunctionFactory<LinearModel> getUpdateFunctionFactory() {
        return new BGDUpdateFunctionFactory();
    }

    @Override
    public boolean shouldTerminate(LinearModel model) {
        return model.roundsRemaining == 0;
    }

}
