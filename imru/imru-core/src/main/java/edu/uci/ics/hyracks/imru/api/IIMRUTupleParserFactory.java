package edu.uci.ics.hyracks.imru.api;

import java.io.Serializable;

import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;

public interface IIMRUTupleParserFactory extends Serializable {
    public ITupleParser createTupleParser(IMRUContext ctx);
}