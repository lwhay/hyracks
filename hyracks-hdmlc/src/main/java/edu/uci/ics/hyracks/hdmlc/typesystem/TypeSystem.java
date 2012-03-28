package edu.uci.ics.hyracks.hdmlc.typesystem;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TypeSystem {
    private String packageName;

    private final Map<String, Type> typeMap;

    public TypeSystem() {
        typeMap = new HashMap<String, Type>();
    }

    public void setPackage(List<String> packageDecl) {
        StringBuilder buffer = new StringBuilder();
        boolean first = true;
        for (String s : packageDecl) {
            if (!first) {
                buffer.append('.');
            }
            first = false;
            buffer.append(s);
        }
        packageName = buffer.toString();
    }

    public String getPackage() {
        return packageName;
    }

    public Map<String, Type> getTypeMap() {
        return typeMap;
    }
}