package com.yahoo.scalops.algebricks;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.LogicalVariable;

public class TranslationContext {

    private int varCounter = 0;

    public LogicalVariable newVar() {
        return new LogicalVariable(varCounter++);
    }

}
