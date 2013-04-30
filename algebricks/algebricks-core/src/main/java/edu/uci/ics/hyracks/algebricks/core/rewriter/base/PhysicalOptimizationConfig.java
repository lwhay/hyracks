package edu.uci.ics.hyracks.algebricks.core.rewriter.base;

import java.util.Properties;

import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalPlan;

public class PhysicalOptimizationConfig {
    private static final int MB = 1048576;
    private static final String FRAMESIZE = "FRAMESIZE";
    private static final String MAX_FRAMES_EXTERNAL_SORT = "MAX_FRAMES_EXTERNAL_SORT";
    private static final String MAX_FRAMES_EXTERNAL_GROUP_BY = "MAX_FRAMES_EXTERNAL_GROUP_BY";

    private static final String DEFAULT_HASH_GROUP_TABLE_SIZE = "DEFAULT_HASH_GROUP_TABLE_SIZE";
    private static final String DEFAULT_EXTERNAL_GROUP_TABLE_SIZE = "DEFAULT_EXTERNAL_GROUP_TABLE_SIZE";
    private static final String DEFAULT_IN_MEM_HASH_JOIN_TABLE_SIZE = "DEFAULT_IN_MEM_HASH_JOIN_TABLE_SIZE";

    private static final String DEFAULT_HYBRID_HASH_GROUP_ROW_COUNT = "DEFAULT_HYBRID_HASH_GROUP_ROW_COUNT";
    private static final String DEFAULT_HYBRID_HASH_GROUP_KEY_CARDINALITY = "DEFAULT_HYBRID_HASH_GROUP_KEY_CARDINALITY";

    private Properties properties = new Properties();

    public PhysicalOptimizationConfig() {
        int frameSize = 32768;
        setInt(FRAMESIZE, frameSize);
        setInt(MAX_FRAMES_EXTERNAL_SORT, (int) (((long) 32 * MB) / frameSize));
        setInt(MAX_FRAMES_EXTERNAL_GROUP_BY, (int) (((long) 32 * MB) / frameSize));

        // use http://www.rsok.com/~jrm/printprimes.html to find prime numbers
        setInt(DEFAULT_HASH_GROUP_TABLE_SIZE, 262133);
        setInt(DEFAULT_EXTERNAL_GROUP_TABLE_SIZE, 262133);
        setInt(DEFAULT_IN_MEM_HASH_JOIN_TABLE_SIZE, 10485767);

        // default parameters for hybrid-hash-group-by: try to underestimate the key cardinality ratio
        setInt(DEFAULT_HYBRID_HASH_GROUP_ROW_COUNT, 1000000);
        setInt(DEFAULT_HYBRID_HASH_GROUP_KEY_CARDINALITY, 10000);
    }

    public int getFrameSize() {
        return getInt(FRAMESIZE, 32768);
    }

    public void setFrameSize(int frameSize) {
        setInt(FRAMESIZE, frameSize);
    }

    public int getMaxFramesExternalSort() {
        int frameSize = getFrameSize();
        return getInt(MAX_FRAMES_EXTERNAL_SORT, (int) (((long) 512 * MB) / frameSize));
    }

    public void setMaxFramesExternalSort(int frameLimit) {
        setInt(MAX_FRAMES_EXTERNAL_SORT, frameLimit);
    }

    public int getMaxFramesExternalGroupBy() {
        int frameSize = getFrameSize();
        return getInt(MAX_FRAMES_EXTERNAL_GROUP_BY, (int) (((long) 256 * MB) / frameSize));
    }

    public void setMaxFramesExternalGroupBy(int frameLimit) {
        setInt(MAX_FRAMES_EXTERNAL_GROUP_BY, frameLimit);
    }

    public int getHashGroupByTableSize() {
        return getInt(DEFAULT_HASH_GROUP_TABLE_SIZE, 262133);
    }

    public void setHashGroupByTableSize(int tableSize) {
        setInt(DEFAULT_HASH_GROUP_TABLE_SIZE, tableSize);
    }

    public int getExternalGroupByTableSize() {
        return getInt(DEFAULT_EXTERNAL_GROUP_TABLE_SIZE, 262133);
    }

    public void setExternalGroupByTableSize(int tableSize) {
        setInt(DEFAULT_EXTERNAL_GROUP_TABLE_SIZE, tableSize);
    }

    public int getInMemHashJoinTableSize() {
        return getInt(DEFAULT_IN_MEM_HASH_JOIN_TABLE_SIZE, 10485767);
    }

    public void setInMemHashJoinTableSize(int tableSize) {
        setInt(DEFAULT_IN_MEM_HASH_JOIN_TABLE_SIZE, tableSize);
    }

    public int getHybridHashGroupByInputRowCount(ILogicalPlan gbyNestedPlan) {
        return getInt(gbyNestedPlan.toString() + ".rows", 1000000);
    }

    public void setHybridHashGroupByInputRowCount(ILogicalPlan gbyNestedPlan, int rowCount) {
        setInt(gbyNestedPlan.toString() + ".rows", rowCount);
    }

    public int getHybridHashGroupByKeyCardinality(ILogicalPlan gbyNestedPlan) {
        return getInt(gbyNestedPlan.toString() + ".card", 10000);
    }

    public void setHybridHashGroupByKeyCardinality(ILogicalPlan gbyNestedPlan, int card) {
        setInt(gbyNestedPlan.toString() + ".card", card);
    }

    public void setHybridHashGroupByRecordInBytes(ILogicalPlan gbyNestedPlan, int recSize) {
        setInt(gbyNestedPlan.toString() + ".rsize", recSize);
    }

    public int getHybridHashGroupByRecordInBytes(ILogicalPlan gbyNestedPlan) {
        return getInt(gbyNestedPlan.toString() + ".rsize", 64);
    }

    private void setInt(String property, int value) {
        properties.setProperty(property, Integer.toString(value));
    }

    private int getInt(String property, int defaultValue) {
        String value = properties.getProperty(property);
        if (value == null)
            return defaultValue;
        else
            return Integer.parseInt(value);
    }

}
