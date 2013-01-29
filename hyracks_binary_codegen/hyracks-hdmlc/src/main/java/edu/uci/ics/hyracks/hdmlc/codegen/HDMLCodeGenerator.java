package edu.uci.ics.hyracks.hdmlc.codegen;

import java.util.HashMap;
import java.util.Map;

import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JFieldRef;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JVar;
import com.sun.codemodel.writer.SingleStreamCodeWriter;

import edu.uci.ics.hyracks.hdmlc.typesystem.ExternalType;
import edu.uci.ics.hyracks.hdmlc.typesystem.StructType;
import edu.uci.ics.hyracks.hdmlc.typesystem.StructType.Field;
import edu.uci.ics.hyracks.hdmlc.typesystem.TaggedUnionType;
import edu.uci.ics.hyracks.hdmlc.typesystem.Type;
import edu.uci.ics.hyracks.hdmlc.typesystem.TypeSystem;
import edu.uci.ics.hyracks.hdmlc.typesystem.TypeTraits;

public class HDMLCodeGenerator {
    private static final String ABSTRACT_POINTABLE_CLASS = "edu.uci.ics.hyracks.data.std.api.AbstractPointable";

    private static final String INTEGER_POINTABLE_CLASS = "edu.uci.ics.hyracks.data.std.primitive.IntegerPointable";

    private static final String BYTES_FIELD_NAME = "bytes";

    private static final String START_FIELD_NAME = "start";

    private final String packageName;

    private final TypeSystem ts;

    private final Map<Type, JClass> classMap;

    public HDMLCodeGenerator(TypeSystem ts) {
        this.packageName = ts.getPackage();
        this.ts = ts;
        classMap = new HashMap<Type, JClass>();
    }

    public void codegen() throws Exception {
        JCodeModel cm = new JCodeModel();
        for (Map.Entry<String, Type> e : ts.getTypeMap().entrySet()) {
            codegenPhase1(cm, e.getKey(), e.getValue());
        }
        for (Map.Entry<String, Type> e : ts.getTypeMap().entrySet()) {
            codegenPhase2(cm, e.getKey(), e.getValue());
        }
        cm.build(new SingleStreamCodeWriter(System.err));
    }

    private void codegenPhase1(JCodeModel cm, String name, Type t) throws Exception {
        switch (t.getTag()) {
            case STRUCT:
                codegenStructPhase1(cm, name, (StructType) t);
                break;

            case EXTERNAL:
                codegenExternalPhase1(cm, name, (ExternalType) t);
                break;

            case TAGGED_UNION:
                codegenTaggedUnionPhase1(cm, name, (TaggedUnionType) t);
                break;
        }
    }

    private void codegenPhase2(JCodeModel cm, String name, Type t) throws Exception {
        switch (t.getTag()) {
            case STRUCT:
                codegenStructPhase2(cm, name, (StructType) t);
                break;

            case EXTERNAL:
                codegenExternalPhase2(cm, name, (ExternalType) t);
                break;

            case TAGGED_UNION:
                codegenTaggedUnionPhase2(cm, name, (TaggedUnionType) t);
                break;
        }
    }

    private void codegenTaggedUnionPhase1(JCodeModel cm, String name, TaggedUnionType t) throws Exception {
        JDefinedClass c = cm._class(createClassName(name));
        classMap.put(t, c);
    }

    private String createClassName(String name) {
        return packageName + "." + name + "Pointable";
    }

    private void codegenExternalPhase1(JCodeModel cm, String name, ExternalType t) {
        JClass c = cm.directClass(t.getClassName());
        classMap.put(t, c);
    }

    private void codegenStructPhase1(JCodeModel cm, String name, StructType t) throws Exception {
        JDefinedClass c = cm._class(createClassName(name));
        classMap.put(t, c);
    }

    private void codegenTaggedUnionPhase2(JCodeModel cm, String name, TaggedUnionType t) {

    }

    private void codegenExternalPhase2(JCodeModel cm, String name, ExternalType t) {
    }

    private void codegenStructPhase2(JCodeModel cm, String name, StructType t) throws Exception {
        JDefinedClass c = (JDefinedClass) classMap.get(t);
        JClass superClass = cm.directClass(ABSTRACT_POINTABLE_CLASS);
        c._extends(superClass);
        int offset = 0;
        int nVarLenFields = 0;
        JFieldRef bytesField = JExpr.ref(BYTES_FIELD_NAME);
        JFieldRef startField = JExpr.ref(START_FIELD_NAME);
        for (Field f : t.getFields()) {
            Type fType = f.type;
            TypeTraits tt = fType.getTypeTraits();
            if (tt.fixedLength < 0) {
                ++nVarLenFields;
                continue;
            }
            String fName = f.name;
            JMethod method = c.method(JMod.PUBLIC, cm.VOID, "get" + caseConvert(fName));
            JClass pClass = classMap.get(fType);
            System.err.println(pClass);
            JVar p = method.param(pClass, "p");
            JBlock body = method.body();
            body.invoke(p, "set").arg(bytesField).arg(startField.plus(JExpr.lit(offset)))
                    .arg(JExpr.lit(tt.fixedLength));
            offset += tt.fixedLength;
        }
        int endOfFixedLengthFields = offset;
        if (nVarLenFields > 0) {
            int vlfIdx = 0;
            for (Field f : t.getFields()) {
                Type fType = f.type;
                TypeTraits tt = fType.getTypeTraits();
                if (tt.fixedLength >= 0) {
                    continue;
                }
                String fName = f.name;
                JMethod method = c.method(JMod.PUBLIC, cm.VOID, "get" + caseConvert(fName));
                JClass pClass = classMap.get(fType);
                if (pClass == null) {
                    continue;
                }
                System.err.println(fType + " " + pClass);
                JVar p = method.param(pClass, "p");
                JBlock body = method.body();
                JVar pStartVar = body.decl(cm.INT, "pStart");
                JVar pLenVar = body.decl(cm.INT, "pLen");
                JClass ipClass = cm.directClass(INTEGER_POINTABLE_CLASS);
                if (vlfIdx == 0) {
                    body.assign(pStartVar, JExpr.lit(0));
                    body.assign(pLenVar,
                            ipClass.staticInvoke("getInteger").arg(bytesField).arg(startField.plus(JExpr.lit(offset)))
                                    .arg(JExpr.lit(4)));
                } else {
                    body.assign(
                            pStartVar,
                            ipClass.staticInvoke("getInteger").arg(bytesField)
                                    .arg(startField.plus(JExpr.lit(offset - 4))).arg(JExpr.lit(4)));
                    body.assign(pLenVar,
                            ipClass.staticInvoke("getInteger").arg(bytesField).arg(startField.plus(JExpr.lit(offset)))
                                    .arg(JExpr.lit(4)).minus(pStartVar));
                }
                body.invoke(p, "set").arg(bytesField)
                        .arg(startField.plus(pStartVar.plus(JExpr.lit(endOfFixedLengthFields + nVarLenFields * 4))))
                        .arg(pLenVar);
                offset += 4;
                ++vlfIdx;
            }
        }
    }

    private String caseConvert(String fName) {
        char fl = fName.charAt(0);
        return Character.toUpperCase(fl) + fName.substring(1);
    }
}