package edu.uci.ics.hyracks.hdmlc.typesystem;

import edu.uci.ics.hyracks.hdmlc.ast.UnionDeclaration;
import edu.uci.ics.hyracks.hdmlc.exceptions.SystemException;

public class UnionType extends Type {
    private UnionDeclaration ud;

    public UnionType(TypeSystem ts, UnionDeclaration ud) {
        super(ts);
        this.ud = ud;
    }

    @Override
    public TypeTag getTag() {
        return TypeTag.UNION;
    }

    public UnionDeclaration getUnionDeclaration() {
        return ud;
    }

    @Override
    public TypeTraits getTypeTraits() throws SystemException {
        return null;
    }
}