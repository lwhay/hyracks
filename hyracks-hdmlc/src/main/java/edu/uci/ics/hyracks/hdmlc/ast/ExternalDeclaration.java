package edu.uci.ics.hyracks.hdmlc.ast;

import java.util.Properties;

public class ExternalDeclaration extends Declaration {
    private Properties properties;

    public ExternalDeclaration(String name, Properties properties) {
        super(name);
        this.properties = properties;
    }

    @Override
    public DeclarationTag getTag() {
        return DeclarationTag.EXTERNAL;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}