package com.yahoo.scalops.algebricks;

import java.io.DataOutput;


import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ClosureEvaluatorFactory implements IEvaluatorFactory {
    private static final long serialVersionUID = 1L;
    private ClosureEvaluator code;

    public ClosureEvaluatorFactory(ClosureEvaluator code) {
        this.code = code;
    }
    
    public IEvaluator createEvaluator(IDataOutputProvider outputProvider) throws AlgebricksException {
        final DataOutput output = outputProvider.getDataOutput();

        return new IEvaluator() {

            public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                code.evaluator(tuple, output);
            }

        };
    }

}
