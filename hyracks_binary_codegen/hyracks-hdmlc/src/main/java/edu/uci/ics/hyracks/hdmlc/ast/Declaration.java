package edu.uci.ics.hyracks.hdmlc.ast;

public abstract class Declaration {
    protected String name;

    public Declaration(String name) {
        this.name = name;
    }

    public abstract DeclarationTag getTag();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}