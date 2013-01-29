package edu.uci.ics.hyracks.hdmlc.typesystem;

import edu.uci.ics.hyracks.hdmlc.ast.Expression;
import edu.uci.ics.hyracks.hdmlc.exceptions.SystemException;

public class ArrayType extends Type {
    private Type eType;
    private Expression length;
    private TypeTraits tTraits;

    public ArrayType(TypeSystem ts, Type eType, Expression length) {
        super(ts);
        this.eType = eType;
        this.length = length;
    }

    @Override
    public TypeTag getTag() {
        return TypeTag.ARRAY;
    }

    public Type getElementType() {
        return eType;
    }

    public Expression getLength() {
        return length;
    }

    @Override
    public TypeTraits getTypeTraits() throws SystemException {
        if (tTraits != null) {
            return tTraits;
        }
        TypeTraits etTraits = eType.getTypeTraits();
        if (etTraits.fixedLength < 0 || !length.isConstant()) {
            tTraits = new TypeTraits();
            tTraits.fixedLength = -1;
            return tTraits;
        } else {
            tTraits = new TypeTraits();
            tTraits.fixedLength = etTraits.fixedLength * length.asInteger();
            return tTraits;
        }
    }
}