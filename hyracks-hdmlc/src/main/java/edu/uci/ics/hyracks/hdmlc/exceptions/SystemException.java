package edu.uci.ics.hyracks.hdmlc.exceptions;

public class SystemException extends Exception {
    private static final long serialVersionUID = 1L;

    public SystemException() {
    }

    public SystemException(String message) {
        super(message);
    }

    public SystemException(Throwable cause) {
        super(cause);
    }

    public SystemException(String message, Throwable cause) {
        super(message, cause);
    }
}