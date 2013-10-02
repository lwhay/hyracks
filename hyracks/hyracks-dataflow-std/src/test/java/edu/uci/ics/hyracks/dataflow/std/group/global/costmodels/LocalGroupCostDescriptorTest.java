package edu.uci.ics.hyracks.dataflow.std.group.global.costmodels;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class LocalGroupCostDescriptorTest {

    final String DATA_FOLDER = "/Volumes/Home/Datasets/AggBench/global/small_sample";

    final String DATA_LABEL = "z0_1000000000_1000000000";

    final String DATA_SUFFIX = ".dat.small.part.";

    @Test
    public void test() throws HyracksDataException {

        int framesLimit = 64;
        int frameSize = 32768;
        long inputCount = 600571;
        long outputCount = 100000;
        int groupStateSize = 64;
        double fudgeFactor = 1.4;
        int tableSize = 8171;
        String[] nodes = new String[] { "nc1", "nc2", "nc3", "nc4", "nc5", "nc6", "nc7", "nc8" };
        String[] inputNodes = new String[] { "nc1", "nc2", "nc3", "nc4" };
        String[] filePaths = new String[inputNodes.length];
        for (int i = 0; i < filePaths.length; i++) {
            filePaths[i] = DATA_FOLDER + "/" + DATA_LABEL + DATA_SUFFIX + i;
        }

        double htCapRatio = 1.0;
        int htSlotSize = 8;
        int htRefSize = 8;
        double bfErrorRatio = 0.15;

        Map<GrouperProperty, List<GlobalAggregationPlan>> planSets = LocalGroupCostDescriptor
                .exploreForNonDominatedGlobalAggregationPlans(framesLimit, frameSize, inputCount, outputCount,
                        groupStateSize, fudgeFactor, tableSize, nodes, inputNodes, filePaths, htCapRatio, htSlotSize,
                        htRefSize, bfErrorRatio);

        for (GrouperProperty prop : planSets.keySet()) {
            System.out.println(prop.toString());
            for (GlobalAggregationPlan plan : planSets.get(prop)) {
                System.out.println(plan.toString());
                System.out.println();
            }

        }
    }

}
