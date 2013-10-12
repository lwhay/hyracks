package edu.uci.ics.hyracks.imru.api;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;

public interface IIMRUDataGenerator extends Serializable {
    public void generate(IMRUContext ctx, OutputStream output)
            throws IOException;
}
