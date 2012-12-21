package edu.uci.ics.hyracks.imru.test.jobgen;

import junit.framework.Assert;

import org.junit.Test;

import edu.uci.ics.hyracks.imru.jobgen.ReduceAggregationTreeFactory;


public class NAryAggregationIMRUJobFactoryTest {
    @Test
    public void testAggregationTreeNodeCounts() {
        Integer[] levels;
        levels = ReduceAggregationTreeFactory.aggregationTreeNodeCounts(120, 2);
        Assert.assertEquals(7, levels.length);
        Assert.assertEquals(60, (int) levels[0]);
        Assert.assertEquals(30, (int) levels[1]);
        Assert.assertEquals(15, (int) levels[2]);
        Assert.assertEquals(8, (int) levels[3]);
        Assert.assertEquals(4, (int) levels[4]);
        Assert.assertEquals(2, (int) levels[5]);
        Assert.assertEquals(1, (int) levels[6]);

        levels = ReduceAggregationTreeFactory.aggregationTreeNodeCounts(120, 4);
        Assert.assertEquals(4, levels.length);
        Assert.assertEquals(30, (int) levels[0]);
        Assert.assertEquals(8, (int) levels[1]);
        Assert.assertEquals(2, (int) levels[2]);
        Assert.assertEquals(1, (int) levels[3]);
    }
}
