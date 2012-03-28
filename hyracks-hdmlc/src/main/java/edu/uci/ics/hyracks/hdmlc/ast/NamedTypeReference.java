package edu.uci.ics.hyracks.hdmlc.ast;

import java.util.List;

public class NamedTypeReference extends TypeReference {
    private String name;

    private List<Expression> typeArgs;

    public NamedTypeReference(String name, List<Expression> typeArgs) {
        this.name = name;
        this.typeArgs = typeArgs;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<Expression> getTypeArgs() {
        return typeArgs;
    }

    public void setTypeArgs(List<Expression> typeArgs) {
        this.typeArgs = typeArgs;
    }

    @Override
    public TypeReferenceTag getTag() {
        return TypeReferenceTag.NAMED;
    }
}