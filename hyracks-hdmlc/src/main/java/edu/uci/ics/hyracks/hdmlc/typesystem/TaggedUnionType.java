package edu.uci.ics.hyracks.hdmlc.typesystem;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.hyracks.hdmlc.ast.Expression;
import edu.uci.ics.hyracks.hdmlc.ast.TaggedUnionDeclaration;
import edu.uci.ics.hyracks.hdmlc.exceptions.SystemException;

public class TaggedUnionType extends Type {
    private final TaggedUnionDeclaration tud;

    private final List<Field> fields;

    private final Map<String, Field> fieldNameMap;

    private TypeTraits traits;

    private boolean inTraitsComputation;

    public TaggedUnionType(TypeSystem ts, TaggedUnionDeclaration tud) {
        super(ts);
        this.tud = tud;
        fields = new ArrayList<Field>();
        fieldNameMap = new HashMap<String, Field>();
    }

    @Override
    public TypeTag getTag() {
        return TypeTag.TAGGED_UNION;
    }

    public TaggedUnionDeclaration getDeclaration() {
        return tud;
    }

    public void addField(Field f) throws SystemException {
        if (fieldNameMap.containsKey(f.name)) {
            throw new SystemException("Field with duplicate name: " + f.name);
        }
        fields.add(f);
        fieldNameMap.put(f.name, f);
    }

    public static class Field {
        public final String name;

        public final Expression tagValue;

        public Type type;

        public Field(String name, Expression tagValue, Type type) {
            this.name = name;
            this.tagValue = tagValue;
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
                length = (length >= 0 && ftt.fixedLength >= 0) ? Math.max(length, ftt.fixedLength) : -1;
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