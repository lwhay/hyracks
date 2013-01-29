package edu.uci.ics.hyracks.hdmlc.compiler;

import java.io.Reader;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import edu.uci.ics.hyracks.hdmlc.ast.ArrayTypeReference;
import edu.uci.ics.hyracks.hdmlc.ast.CompilationUnit;
import edu.uci.ics.hyracks.hdmlc.ast.Declaration;
import edu.uci.ics.hyracks.hdmlc.ast.Expression;
import edu.uci.ics.hyracks.hdmlc.ast.ExternalDeclaration;
import edu.uci.ics.hyracks.hdmlc.ast.FieldDeclaration;
import edu.uci.ics.hyracks.hdmlc.ast.NamedTypeReference;
import edu.uci.ics.hyracks.hdmlc.ast.StructDeclaration;
import edu.uci.ics.hyracks.hdmlc.ast.TaggedUnionDeclaration;
import edu.uci.ics.hyracks.hdmlc.ast.TypeReference;
import edu.uci.ics.hyracks.hdmlc.ast.UnionDeclaration;
import edu.uci.ics.hyracks.hdmlc.exceptions.SystemException;
import edu.uci.ics.hyracks.hdmlc.parser.HDMLGrammar;
import edu.uci.ics.hyracks.hdmlc.typesystem.ArrayType;
import edu.uci.ics.hyracks.hdmlc.typesystem.ExternalType;
import edu.uci.ics.hyracks.hdmlc.typesystem.StructType;
import edu.uci.ics.hyracks.hdmlc.typesystem.TaggedUnionType;
import edu.uci.ics.hyracks.hdmlc.typesystem.Type;
import edu.uci.ics.hyracks.hdmlc.typesystem.TypeSystem;
import edu.uci.ics.hyracks.hdmlc.typesystem.UnionType;

public class HDMLCompiler {
    public HDMLCompiler() {
    }

    public CompilationUnit parse(Reader in) throws Exception {
        try {
            HDMLGrammar g = new HDMLGrammar(in);
            return g.CompilationUnit();
        } finally {
            in.close();
        }
    }

    public TypeSystem compile(CompilationUnit cu) throws Exception {
        TypeSystem ts = new TypeSystem();
        compilePhase1(ts, cu);
        compilePhase2(ts, cu);
        computeTypeTraits(ts);
        for (Map.Entry<String, Type> e : ts.getTypeMap().entrySet()) {
            System.err.println(e.getKey() + ": " + e.getValue().getTypeTraits());
        }
        return ts;
    }

    private void compilePhase1(TypeSystem ts, CompilationUnit cu) throws SystemException {
        ts.setPackage(cu.getPackageDecl());

        for (Declaration decl : cu.getDeclarations()) {
            switch (decl.getTag()) {
                case EXTERNAL: {
                    ExternalDeclaration ed = (ExternalDeclaration) decl;
                    ExternalType et = new ExternalType(ts, ed);
                    ts.getTypeMap().put(ed.getName(), et);
                    break;
                }

                case STRUCT: {
                    StructDeclaration sd = (StructDeclaration) decl;
                    StructType st = new StructType(ts, sd);
                    ts.getTypeMap().put(sd.getName(), st);
                    break;
                }

                case TAGGED_UNION: {
                    TaggedUnionDeclaration tud = (TaggedUnionDeclaration) decl;
                    TaggedUnionType tut = new TaggedUnionType(ts, tud);
                    ts.getTypeMap().put(tud.getName(), tut);
                    break;
                }

                case UNION: {
                    UnionDeclaration ud = (UnionDeclaration) decl;
                    UnionType ut = new UnionType(ts, ud);
                    ts.getTypeMap().put(ud.getName(), ut);
                    break;
                }
            }
        }
    }

    private void compilePhase2(TypeSystem ts, CompilationUnit cu) throws SystemException {
        for (Declaration decl : cu.getDeclarations()) {
            switch (decl.getTag()) {
                case STRUCT: {
                    StructDeclaration sd = (StructDeclaration) decl;
                    StructType st = (StructType) ts.getTypeMap().get(sd.getName());
                    for (FieldDeclaration fd : sd.getFields()) {
                        String name = fd.getName();
                        TypeReference tRef = fd.getTypeReference();
                        Type fType = lookupType(ts, tRef);
                        st.addField(new StructType.Field(name, fType));
                    }
                    break;
                }

                case TAGGED_UNION: {
                    TaggedUnionDeclaration tud = (TaggedUnionDeclaration) decl;
                    TaggedUnionType tut = (TaggedUnionType) ts.getTypeMap().get(tud.getName());
                    for (Pair<Expression, FieldDeclaration> p : tud.getChoices()) {
                        FieldDeclaration fd = p.getRight();
                        String name = fd.getName();
                        TypeReference tRef = fd.getTypeReference();
                        Expression tagValue = p.getLeft();
                        tut.addField(new TaggedUnionType.Field(name, tagValue, lookupType(ts, tRef)));
                    }
                    break;
                }

                case UNION: {
                    break;
                }
            }
        }
    }

    private Type lookupType(TypeSystem ts, TypeReference tRef) throws SystemException {
        switch (tRef.getTag()) {
            case ARRAY: {
                ArrayTypeReference atr = (ArrayTypeReference) tRef;
                Type eType = lookupType(ts, atr.getTypeReference());
                return new ArrayType(ts, eType, atr.getSize());
            }

            case NAMED: {
                NamedTypeReference ntr = (NamedTypeReference) tRef;
                Type t = ts.getTypeMap().get(ntr.getName());
                if (t == null) {
                    throw new SystemException("Undefined type: " + ntr.getName());
                }
                return t;
            }
        }
        return null;
    }

    private void computeTypeTraits(TypeSystem ts) throws SystemException {
        Map<String, Type> typeMap = ts.getTypeMap();
        for (Type t : typeMap.values()) {
            t.getTypeTraits();
        }
    }
}