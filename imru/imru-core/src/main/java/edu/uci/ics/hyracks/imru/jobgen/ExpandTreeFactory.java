package edu.uci.ics.hyracks.imru.jobgen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.connectors.LocalityAwareMToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.connectors.MToNPartitioningConnectorDescriptor;
import edu.uci.ics.hyracks.imru.api.IIMRUJobSpecification;
import edu.uci.ics.hyracks.imru.base.IConfigurationFactory;
import edu.uci.ics.hyracks.imru.dataflow.ExpandOperatorDescriptor;
import edu.uci.ics.hyracks.imru.dataflow.ReduceOperatorDescriptor;
import edu.uci.ics.hyracks.imru.dataflow.UpdateOperatorDescriptor;

/**
 * @author wangrui
 */
public class ExpandTreeFactory {
    private static void add(JobSpecification spec, IOperatorDescriptor updateOp, int updatePort, int fanOut,
            LinkedList<String> locations, IIMRUJobSpecification imruSpec, int level, String modelInPath,
            IConfigurationFactory confFactory, String modelOutPath) {
        if (locations.size() == 0)
            return;
        String[] ss = new String[Math.min(locations.size(), fanOut)];
        for (int i = 0; i < ss.length; i++)
            ss[i] = locations.remove();
        for (int i = 0; i < ss.length; i++) {
            ExpandOperatorDescriptor op = new ExpandOperatorDescriptor(spec, imruSpec, modelInPath, confFactory,
                    modelOutPath, "NAryExpandL" + level + "_", i * fanOut < locations.size());
            IConnectorDescriptor conn = new MToNPartitioningConnectorDescriptor(spec,
                    OneToOneTuplePartitionComputerFactory.INSTANCE);
            conn.setDisplayName("ExpandTreeLevel" + level + "To" + (level + 1) + "Connector");
            spec.connect(conn, updateOp, updatePort, op, 0);
            PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, op, new String[] { ss[i] });
            add(spec, op, 0, fanOut, locations, imruSpec, level, modelInPath, confFactory, modelOutPath);
        }
    }

    public static void buildExpandTree(JobSpecification spec, IOperatorDescriptor updateOp, int updatePort, int fanOut,
            String[] consumerOpLocations, IIMRUJobSpecification imruSpec, String modelInPath,
            IConfigurationFactory confFactory, String modelOutPath) {
        LinkedList<String> locations = new LinkedList<String>(new HashSet<String>(Arrays.asList(consumerOpLocations)));
        add(spec, updateOp, updatePort, fanOut, locations, imruSpec, 0, modelInPath, confFactory, modelOutPath);
    }
}
