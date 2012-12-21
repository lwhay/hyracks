package edu.uci.ics.hyracks.imru.example.bgd;

import java.io.File;
import java.util.EnumSet;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.pregelix.core.jobgen.clusterconfig.ClusterConfig;

public class ImruTest {
    public static final String NC1_ID = "nc1";
    public static final String NC2_ID = "nc2";

    public static final int DEFAULT_HYRACKS_CC_PORT = 1099;
    public static final int TEST_HYRACKS_CC_PORT = 1099;
    public static final int TEST_HYRACKS_CC_CLIENT_PORT = 3099;
    public static final String CC_HOST = "localhost";

    public static final int FRAME_SIZE = 65536;

    private static ClusterControllerService cc;
    private static NodeControllerService nc1;
    private static NodeControllerService nc2;
    private static IHyracksClientConnection hcc;

    public static void init() throws Exception {
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = CC_HOST;
        ccConfig.clusterNetIpAddress = CC_HOST;
        ccConfig.clusterNetPort = TEST_HYRACKS_CC_PORT;
        ccConfig.clientNetPort = TEST_HYRACKS_CC_CLIENT_PORT;
        ccConfig.defaultMaxJobAttempts = 0;
        ccConfig.jobHistorySize = 10;

        // cluster controller
        cc = new ClusterControllerService(ccConfig);
        cc.start();

        // two node controllers
        NCConfig ncConfig1 = new NCConfig();
        ncConfig1.ccHost = "localhost";
        ncConfig1.clusterNetIPAddress = "localhost";
        ncConfig1.ccPort = TEST_HYRACKS_CC_PORT;
        ncConfig1.dataIPAddress = "127.0.0.1";
        ncConfig1.nodeId = NC1_ID;
        nc1 = new NodeControllerService(ncConfig1);
        nc1.start();

        NCConfig ncConfig2 = new NCConfig();
        ncConfig2.ccHost = "localhost";
        ncConfig2.clusterNetIPAddress = "localhost";
        ncConfig2.ccPort = TEST_HYRACKS_CC_PORT;
        ncConfig2.dataIPAddress = "127.0.0.1";
        ncConfig2.nodeId = NC2_ID;
        nc2 = new NodeControllerService(ncConfig2);
        nc2.start();

        // hyracks connection
        hcc = new HyracksConnection(CC_HOST, TEST_HYRACKS_CC_CLIENT_PORT);
        ClusterConfig
                .setClusterPropertiesPath("imru/imru-core/src/main/resources/conf/cluster.properties");
        ClusterConfig
                .setStorePath("imru/imru-core/src/main/resources/conf/stores.properties");
        ClusterConfig.loadClusterConfig(CC_HOST, TEST_HYRACKS_CC_CLIENT_PORT);
    }

    public static void destroyApp(String hyracksAppName) throws Exception {
        hcc.destroyApplication(hyracksAppName);
    }

    public static void createApp(String hyracksAppName, File harFile)
            throws Exception {
        hcc.createApplication(hyracksAppName, harFile);
    }

    public static void deinit() throws Exception {
        nc2.stop();
        nc1.stop();
        cc.stop();
    }

    public static void runJob(JobSpecification spec, String appName)
            throws Exception {
        spec.setFrameSize(FRAME_SIZE);
        JobId jobId = hcc.startJob(appName, spec, EnumSet
                .of(JobFlag.PROFILE_RUNTIME));
        hcc.waitForCompletion(jobId);
    }
}
