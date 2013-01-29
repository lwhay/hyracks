package edu.uci.ics.hyracks.hdmlc.ast;

import java.util.List;

public class CompilationUnit {
    private List<String> packageDecl;

    private List<Declaration> declarations;

    public CompilationUnit(List<String> packageDecl, List<Declaration> declarations) {
        this.packageDecl = packageDecl;
        this.declarations = declarations;
    }

    public List<String> getPackageDecl() {
        return packageDecl;
    }

    public void setPackageDecl(List<String> packageDecl) {
        this.packageDecl = packageDecl;
    }

    public List<Declaration> getDeclarations() {
        return declarations;
    }

    public void setDeclarations(List<Declaration> declarations) {
        this.declarations = declarations;
    }
}