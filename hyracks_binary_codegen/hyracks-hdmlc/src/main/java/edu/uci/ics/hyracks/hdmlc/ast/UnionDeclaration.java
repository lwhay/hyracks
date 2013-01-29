package edu.uci.ics.hyracks.hdmlc.ast;

import java.util.List;

public class UnionDeclaration extends Declaration {
    private List<FieldDeclaration> fields;

    public UnionDeclaration(String name, List<FieldDeclaration> fields) {
        super(name);
        this.fields = fields;
    }

    @Override
    public DeclarationTag getTag() {
        return DeclarationTag.UNION;
    }

    public List<FieldDeclaration> getFields() {
        return fields;
    }

    public void setFields(List<FieldDeclaration> fields) {
        this.fields = fields;
    }
}