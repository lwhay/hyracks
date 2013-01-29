package edu.uci.ics.hyracks.hdmlc.typesystem;

public class TypeTraits {
    public int fixedLength;

    @Override
    public String toString() {
        return "Traits: [ length = " + fixedLength + " ]";
    }
}