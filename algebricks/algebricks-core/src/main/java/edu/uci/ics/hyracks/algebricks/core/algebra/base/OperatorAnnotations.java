package edu.uci.ics.hyracks.algebricks.core.algebra.base;

public interface OperatorAnnotations {
    // hints for group by
    public static final String USE_HASH_SORT_GROUP_BY = "USE_HASH_SORT_GROUP_BY"; // -->
    public static final String USE_HYBRID_HASH_GROUP_BY = "USE_HYBRID_HASH_GROUP_BY";

    // Boolean
    public static final String CARDINALITY = "CARDINALITY"; // -->
    // Integer
    public static final String MAX_NUMBER_FRAMES = "MAX_NUMBER_FRAMES"; // -->
    // Integer
    
    // for hybrid-hash-gby
    public static final String INPUT_RECORD_COUNT = "INPUT_RECORD_COUNT";
    public static final String INPUT_RECORD_SIZE_IN_BYTES = "INPUT_RECORD_SIZE_IN_BYTES";
}
