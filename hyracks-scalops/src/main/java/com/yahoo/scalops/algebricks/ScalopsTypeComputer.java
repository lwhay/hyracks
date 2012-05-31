package com.yahoo.scalops.algebricks;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IExpressionTypeComputer;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;

public class ScalopsTypeComputer implements IExpressionTypeComputer {

    public Object getType(ILogicalExpression expr, IMetadataProvider<?, ?> metadataProvider,
            IVariableTypeEnvironment env) throws AlgebricksException {
        return new Object();
    }

}
