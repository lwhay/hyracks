package edu.uci.ics.hyracks.hdmlc.ast;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

public class TaggedUnionDeclaration extends Declaration {
    private List<Pair<Expression, FieldDeclaration>> choices;
    
    public TaggedUnionDeclaration(String name, List<Pair<Expression, FieldDeclaration>> choices) {
        super(name);
        this.choices = choices;
    }

    @Override
    public DeclarationTag getTag() {
        return DeclarationTag.TAGGED_UNION;
    }

    public List<Pair<Expression, FieldDeclaration>> getChoices() {
        return choices;
    }

    public void setChoices(List<Pair<Expression, FieldDeclaration>> choices) {
        this.choices = choices;
    }
}