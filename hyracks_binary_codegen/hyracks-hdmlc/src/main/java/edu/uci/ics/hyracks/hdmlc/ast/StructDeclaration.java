package edu.uci.ics.hyracks.hdmlc.ast;

import java.util.List;

public class StructDeclaration extends Declaration {
    private List<FieldDeclaration> fields;

    public StructDeclaration(String name, List<FieldDeclaration> fields) {
        super(name);
        this.fields = fields;
    }

    @Override
    public DeclarationTag getTag() {
        return DeclarationTag.STRUCT;
    }

    public List<FieldDeclaration> getFields() {
        return fields;
    }

    public void setFields(List<FieldDeclaration> fields) {
        this.fields = fields;
    }
}