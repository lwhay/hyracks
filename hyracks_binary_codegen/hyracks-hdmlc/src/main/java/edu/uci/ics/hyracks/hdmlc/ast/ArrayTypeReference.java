package edu.uci.ics.hyracks.hdmlc.ast;

public class ArrayTypeReference extends TypeReference {
    private TypeReference tRef;
    
    private Expression size;
    
    public ArrayTypeReference(TypeReference tRef, Expression size) {
        this.tRef = tRef;
        this.size = size;
    }

    @Override
    public TypeReferenceTag getTag() {
        return TypeReferenceTag.ARRAY;
    }

    public TypeReference getTypeReference() {
        return tRef;
    }

    public void setTypeReference(TypeReference tRef) {
        this.tRef = tRef;
    }

    public Expression getSize() {
        return size;
    }

    public void setSize(Expression size) {
        this.size = size;
    }
}