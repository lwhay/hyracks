package edu.uci.ics.hyracks.imru.api;

import edu.uci.ics.hyracks.api.exceptions.HyracksException;

public class IMRUException extends HyracksException {
    public IMRUException() {
    }

    public IMRUException(Throwable e) {
        super(e);
    }

    public IMRUException(String s) {
        super(s);
    }

    public IMRUException(String message, Throwable cause) {
        super(message, cause);
    }
}
