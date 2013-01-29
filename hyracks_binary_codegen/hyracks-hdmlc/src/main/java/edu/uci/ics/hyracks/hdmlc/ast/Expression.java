package edu.uci.ics.hyracks.hdmlc.ast;

import edu.uci.ics.hyracks.hdmlc.exceptions.SystemException;

public abstract class Expression {
    public abstract ExpressionTag getTag();

    public abstract boolean isConstant();

    public abstract int asInteger() throws SystemException;
}