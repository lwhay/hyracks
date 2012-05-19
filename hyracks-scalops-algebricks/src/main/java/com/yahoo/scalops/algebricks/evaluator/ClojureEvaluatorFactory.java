package com.yahoo.scalops.algebricks.evaluator;

import java.io.DataOutput;

import com.yahoo.scalops.function.IClojureEvaluator;
import com.yahoo.scalops.function.IFunctionDescriptor;

import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;

public class ClojureEvaluatorFactory implements IEvaluatorFactory {
    private static final long serialVersionUID = 1L;
    private final IFunctionDescriptor functionDescriptor;

    public ClojureEvaluatorFactory(IFunctionDescriptor functionDescriptor) {
        this.functionDescriptor = functionDescriptor;
    }

    public IEvaluator createEvaluator(IDataOutputProvider outputProvider) throws AlgebricksException {
        final DataOutput output = outputProvider.getDataOutput();
        final IClojureEvaluator evaluator = functionDescriptor.createClojure();

        return new IEvaluator() {

            public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
                evaluator.evaluator(tuple, output);
            }

        };
    }

}
