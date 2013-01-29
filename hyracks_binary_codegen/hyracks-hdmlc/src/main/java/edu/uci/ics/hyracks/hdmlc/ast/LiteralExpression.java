package edu.uci.ics.hyracks.hdmlc.ast;

import edu.uci.ics.hyracks.hdmlc.exceptions.SystemException;

public class LiteralExpression extends Expression {
    private LiteralType type;

    private String image;

    public LiteralExpression(LiteralType type, String image) {
        this.type = type;
        this.image = image;
    }

    @Override
    public ExpressionTag getTag() {
        return ExpressionTag.LITERAL;
    }

    public LiteralType getType() {
        return type;
    }

    public void setType(LiteralType type) {
        this.type = type;
    }

    public String getImage() {
        return image;
    }

    public void setImage(String image) {
        this.image = image;
    }

    @Override
    public boolean isConstant() {
        return true;
    }

    @Override
    public int asInteger() throws SystemException {
        if (type != LiteralType.INTEGER) {
            throw new SystemException("Cannot get integer from: " + type);
        }
        return Integer.parseInt(image);
    }
}