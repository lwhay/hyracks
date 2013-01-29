package edu.uci.ics.hyracks.hdmlc.typesystem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.hdmlc.ast.StructDeclaration;
import edu.uci.ics.hyracks.hdmlc.exceptions.SystemException;

public class StructType extends Type {
    private final StructDeclaration sd;

    private final Map<String, Field> fieldNameMap;

    private final List<Field> fields;

    private TypeTraits traits;

    private boolean inTraitsComputation;

    public StructType(TypeSystem ts, StructDeclaration sd) {
        super(ts);
        this.sd = sd;
        fieldNameMap = new HashMap<String, Field>();
        fields = new ArrayList<Field>();
    }

    @Override
    public TypeTag getTag() {
        return TypeTag.STRUCT;
    }

    public StructDeclaration getStructDeclaration() {
        return sd;
    }

    public void addField(Field f) throws SystemException {
        if (fieldNameMap.containsKey(f.name)) {
            throw new SystemException("Field with duplicate name: " + f.name);
        }
        fields.add(f);
        fieldNameMap.put(f.name, f);
    }

    public List<Field> getFields() {
        return fields;
    }

    public static class Field {
        public final String name;
        public Type type;

        public Field(String name, Type type) {
            this.name = name;
            this.type = type;
        }
    }

    @Override
    public TypeTraits getTypeTraits() throws SystemException {
        if (inTraitsComputation) {
            traits = new TypeTraits();
            traits.fixedLength = -1;
        } else if (traits == null) {
            inTraitsComputation = true;
            int length = 0;
            for (Field f : fields) {
                Type fType = f.type;
                TypeTraits ftt = fType.getTypeTraits();
                length = (length >= 0 && ftt.fixedLength >= 0) ? (length + ftt.fixedLength) : -1;
            }
            inTraitsComputation = false;
            if (traits == null) {
                traits = new TypeTraits();
                traits.fixedLength = length;
            }
        }
        return traits;
    }
}