package edu.uci.ics.hyracks.tests.integration.globalagg;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFamily;
import edu.uci.ics.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.data.std.accessors.MurmurHash3BinaryHashFunctionFamily;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.normalizers.UTF8StringNormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.DoubleParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.DoubleSumFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.MultiFieldsAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IPv6MarkStringParserFactory;
import edu.uci.ics.hyracks.dataflow.std.group.global.costmodels.GlobalAggregationPlan;
import edu.uci.ics.hyracks.dataflow.std.group.global.costmodels.GlobalAggregationPlanCost;
import edu.uci.ics.hyracks.dataflow.std.group.global.costmodels.GrouperProperty;
import edu.uci.ics.hyracks.dataflow.std.group.global.costmodels.LocalGroupCostDescriptor;

public class LocalGroupCostDescriptorTest extends AbstractGlobalAggIntegrationTest {

    final String DATA_FOLDER = "/Volumes/Home/Datasets/AggBench/global/small_sample";

    final String DATA_LABEL = "z0_1000000000_1000000000";

    final String DATA_SUFFIX = ".dat.small.part.";

    @Test
    public void test() throws Exception {

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

        int[] keyFields = new int[] { 0 };
        int[] decorFields = new int[] {};

        final RecordDescriptor inputRecordDescriptor = new RecordDescriptor(new ISerializerDeserializer[] {
                // IP
                UTF8StringSerializerDeserializer.INSTANCE,
                // ad revenue
                DoubleSerializerDeserializer.INSTANCE });

        final RecordDescriptor outputRecordDescriptor = new RecordDescriptor(new ISerializerDeserializer[] {
                // IP
                UTF8StringSerializerDeserializer.INSTANCE,
                // ad revenue
                DoubleSerializerDeserializer.INSTANCE });

        final ITupleParserFactory tupleParserFactory = new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
                IPv6MarkStringParserFactory.getInstance(4), DoubleParserFactory.INSTANCE }, '|');

        final IAggregatorDescriptorFactory aggregateFactory = new MultiFieldsAggregatorFactory(
                new IFieldAggregateDescriptorFactory[] { new DoubleSumFieldAggregatorFactory(1, false) });

        final IAggregatorDescriptorFactory partialMergeFactory = aggregateFactory;
        final IAggregatorDescriptorFactory finalMergeFactory = aggregateFactory;

        Map<GrouperProperty, List<GlobalAggregationPlan>> planSets = LocalGroupCostDescriptor
                .exploreForNonDominatedGlobalAggregationPlans(framesLimit, frameSize, inputCount, outputCount,
                        groupStateSize, fudgeFactor, tableSize, nodes, inputNodes, filePaths, htCapRatio, htSlotSize,
                        htRefSize, bfErrorRatio);

        final IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
                .of(UTF8StringPointable.FACTORY) };

        final IBinaryHashFunctionFamily[] hashFamilies = new IBinaryHashFunctionFamily[] { MurmurHash3BinaryHashFunctionFamily.INSTANCE };

        final INormalizedKeyComputerFactory firstNormalizerFactory = new UTF8StringNormalizedKeyComputerFactory();

        int plansTested = 0;
        for (GrouperProperty prop : planSets.keySet()) {
            System.out.println(prop.toString());
            for (GlobalAggregationPlan plan : planSets.get(prop)) {
                GlobalAggregationPlanCost planCost = new GlobalAggregationPlanCost(inputCount, outputCount,
                        groupStateSize);
                LocalGroupCostDescriptor.computePlanCost(plan, planCost, framesLimit, frameSize, fudgeFactor,
                        tableSize, htCapRatio, htSlotSize, htRefSize, bfErrorRatio);
                System.out.println(plan.toString());
                System.out.println();
                if (plansTested++ < 10) {
                    JobSpecification spec = LocalGroupCostDescriptor.createHyracksJobSpec(framesLimit, keyFields,
                            decorFields, inputCount, outputCount, groupStateSize, fudgeFactor, tableSize,
                            inputRecordDescriptor, outputRecordDescriptor, tupleParserFactory, aggregateFactory,
                            partialMergeFactory, finalMergeFactory, comparatorFactories, hashFamilies,
                            firstNormalizerFactory, plan);
                    runTest(spec);
                }
            }

        }
    }

}
