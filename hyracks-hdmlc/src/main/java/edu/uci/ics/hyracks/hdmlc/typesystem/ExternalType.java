package edu.uci.ics.hyracks.hdmlc.typesystem;

import edu.uci.ics.hyracks.hdmlc.ast.ExternalDeclaration;
import edu.uci.ics.hyracks.hdmlc.exceptions.SystemException;

public class ExternalType extends Type {
    private static final String FIXED_LENGTH_PROPERTY = "fixed-length";
    private static final String LENGTH_PROPERTY = "length";
    private static final String CLASS_NAME = "class";

    private ExternalDeclaration ed;
    private TypeTraits traits;
    private String className;

    public ExternalType(TypeSystem ts, ExternalDeclaration ed) throws SystemException {
        super(ts);
        this.ed = ed;
        traits = new TypeTraits();
        String flv = ed.getProperties().getProperty(FIXED_LENGTH_PROPERTY);
        boolean fl = flv == null ? false : Boolean.valueOf(flv);
        if (fl) {
            String lv = ed.getProperties().getProperty(LENGTH_PROPERTY);
            if (lv == null) {
                throw new SystemException("No length specified for fixed length type: " + ed.getName()
                        + ". Specified properties = " + ed.getProperties());
            }
            traits.fixedLength = Integer.parseInt(lv);
        } else {
            traits.fixedLength = -1;
        }

        className = ed.getProperties().getProperty(CLASS_NAME);
    }

    @Override
    public TypeTag getTag() {
        return TypeTag.EXTERNAL;
    }

    public ExternalDeclaration getExternalDeclaration() {
        return ed;
    }

    @Override
    public TypeTraits getTypeTraits() throws SystemException {
        return traits;
    }

    public String getClassName() {
        return className;
    }
}