package edu.uci.ics.hyracks.hdmlc.ast;

public class FieldDeclaration {
    private String name;

    private TypeReference tRef;

    public FieldDeclaration(String name, TypeReference tRef) {
        this.name = name;
        this.tRef = tRef;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TypeReference getTypeReference() {
        return tRef;
    }

    public void setTypeReference(TypeReference tRef) {
        this.tRef = tRef;
    }
}