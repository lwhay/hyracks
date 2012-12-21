package edu.uci.ics.hyracks.imru.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IUpdateFunction {
    void open() throws HyracksDataException;
    void close() throws HyracksDataException;
}
