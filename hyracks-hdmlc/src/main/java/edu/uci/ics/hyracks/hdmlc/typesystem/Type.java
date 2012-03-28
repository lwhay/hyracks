package edu.uci.ics.hyracks.hdmlc.typesystem;

import edu.uci.ics.hyracks.hdmlc.exceptions.SystemException;

public abstract class Type {
    protected final TypeSystem ts;

    public Type(TypeSystem ts) {
        this.ts = ts;
    }

    public abstract TypeTag getTag();

    public abstract TypeTraits getTypeTraits() throws SystemException;
}