package edu.uci.ics.hyracks.hdmlc.ast;

import edu.uci.ics.hyracks.hdmlc.exceptions.SystemException;

public class IdentifierExpression extends Expression {
    private String name;

    public IdentifierExpression(String name) {
        this.name = name;
    }

    @Override
    public ExpressionTag getTag() {
        return ExpressionTag.IDENTIFIER;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean isConstant() {
        return false;
    }

    @Override
    public int asInteger() throws SystemException {
        return 0;
    }
}