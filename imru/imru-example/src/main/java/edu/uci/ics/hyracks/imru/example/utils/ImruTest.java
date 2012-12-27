package edu.uci.ics.hyracks.imru.example.utils;

import java.io.File;
import java.util.EnumSet;
import java.util.logging.Handler;
import java.util.logging.Logger;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;

//import edu.uci.ics.pregelix.core.jobgen.clusterconfig.ClusterConfig;

public class ImruTest {
    public static final String NC2_ID = "nc2";

    public static final int DEFAULT_HYRACKS_CC_PORT = 1099;
    public static final int TEST_HYRACKS_CC_PORT = 1099;
    public static final int TEST_HYRACKS_CC_CLIENT_PORT = 3099;

    public static final int FRAME_SIZE = 65536;

    private static ClusterControllerService cc;
    private static NodeControllerService nc1;
    private static NodeControllerService nc2;
    private static IHyracksClientConnection hcc;

    public static void startCC(String host, int clusterNetPort,
            int clientNetPort) throws Exception {
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = host;
        ccConfig.clusterNetIpAddress = host;
        ccConfig.clusterNetPort = clusterNetPort;
        ccConfig.clientNetPort = clientNetPort;
        ccConfig.defaultMaxJobAttempts = 0;
        ccConfig.jobHistorySize = 10;

        // cluster controller
        cc = new ClusterControllerService(ccConfig);
        cc.start();

        // hyracks connection
        hcc = new HyracksConnection(host, clientNetPort);
    }

    public static void startNC1(String NC1_ID, String host, int clusterNetPort)
            throws Exception {
        NCConfig ncConfig1 = new NCConfig();
        ncConfig1.ccHost = host;
        ncConfig1.clusterNetIPAddress = host;
        ncConfig1.ccPort = clusterNetPort;
        ncConfig1.dataIPAddress = "127.0.0.1";
        ncConfig1.nodeId = NC1_ID;
        nc1 = new NodeControllerService(ncConfig1);
        nc1.start();
    }

    public static void startNC2(String NC2_ID, String host, int clusterNetPort)
            throws Exception {
        NCConfig ncConfig2 = new NCConfig();
        ncConfig2.ccHost = host;
        ncConfig2.clusterNetIPAddress = host;
        ncConfig2.ccPort = clusterNetPort;
        ncConfig2.dataIPAddress = "127.0.0.1";
        ncConfig2.nodeId = NC2_ID;
        nc2 = new NodeControllerService(ncConfig2);
        nc2.start();

        // ClusterConfig
        // .setClusterPropertiesPath("imru/imru-core/src/main/resources/conf/cluster.properties");
        // ClusterConfig
        // .setStorePath("imru/imru-core/src/main/resources/conf/stores.properties");
        // ClusterConfig.loadClusterConfig(CC_HOST,
        // TEST_HYRACKS_CC_CLIENT_PORT);
    }

    public static void disableLogging() throws Exception {
        Logger globalLogger = Logger.getLogger("");
        Handler[] handlers = globalLogger.getHandlers();
        for (Handler handler : handlers)
            globalLogger.removeHandler(handler);
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
