package edu.uci.ics.hyracks.imru.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class IMRUDataException extends HyracksDataException {
    public IMRUDataException() {
    }

    public IMRUDataException(Throwable e) {
        super(e);
    }

    public IMRUDataException(String s) {
        super(s);
    }

    public IMRUDataException(String message, Throwable cause) {
        super(message, cause);
    }
}
