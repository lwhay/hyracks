package edu.uci.ics.hyracks.imru.example.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.logging.Handler;
import java.util.logging.Logger;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.imru.api.IModel;
import edu.uci.ics.hyracks.imru.api2.IMRUJob;
import edu.uci.ics.hyracks.imru.api2.IMRUJobControl;
import edu.uci.ics.hyracks.imru.example.helloworld.HelloWorldIncrementalResult;
import edu.uci.ics.hyracks.imru.example.helloworld.HelloWorldModel;

public class Client<Model extends IModel, T extends Serializable> {
    public static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1099)")
        public int port = 3099;

        @Option(name = "-clusterport", usage = "Hyracks Cluster Controller Port (default: 3099)")
        public int clusterPort = 1099;

        @Option(name = "-app", usage = "Hyracks Application name", required = true)
        public String app;

        @Option(name = "-hadoop-conf", usage = "Path to Hadoop configuration", required = true)
        public String hadoopConfPath;

        @Option(name = "-cluster-conf", usage = "Path to Hyracks cluster configuration")
        public String clusterConfPath = "conf/cluster.conf";

        @Option(name = "-temp-path", usage = "HDFS path to hold temporary files", required = true)
        public String tempPath;

        @Option(name = "-example-paths", usage = "HDFS path to hold input data")
        public String examplePaths = "/input/data.txt";

        @Option(name = "-agg-tree-type", usage = "The aggregation tree type (none, rack, nary, or generic)", required = true)
        public String aggTreeType;

        @Option(name = "-agg-count", usage = "The number of aggregators to use, if using an aggregation tree")
        public int aggCount = -1;

        @Option(name = "-fan-in", usage = "The fan-in, if using an nary aggregation tree")
        public int fanIn = -1;

        @Option(name = "-model-file", usage = "Local file to write the final weights to")
        public String modelFilename;

        @Option(name = "-num-rounds", usage = "The number of iterations to perform")
        public int numRounds = 5;
    }

    public static final int FRAME_SIZE = 65536;

    private ClusterControllerService cc;
    private NodeControllerService nc1;
    private NodeControllerService nc2;
    private IHyracksClientConnection hcc;

    public IMRUJobControl<Model, T> control;
    public Options options = new Options();
    Configuration conf;

    public Client(String[] args) throws CmdLineException {
        CmdLineParser parser = new CmdLineParser(options);
        parser.parseArgument(args);
    }

    public static String getLocalHostName() throws Exception {
        return java.net.InetAddress.getLocalHost().getHostName();
    }

    public static String getLocalIp() throws Exception {
        // return same ip as
        // pregelix/pregelix-dist/target/appassembler/bin/getip.sh
        String ip = "127.0.0.1";
        NetworkInterface netint = NetworkInterface.getByName("eth0");
        Enumeration<InetAddress> inetAddresses = netint.getInetAddresses();
        for (InetAddress inetAddress : Collections.list(inetAddresses)) {
            byte[] addr = inetAddress.getAddress();
            if (addr != null && addr.length == 4)
                ip = inetAddress.getHostAddress();
        }
        return ip;
    }

    public static void generateClusterConfig(File file, String... args)
            throws IOException {
        PrintStream ps = new PrintStream(file);
        for (int i = 0; i < args.length / 2; i++)
            ps.println(args[i * 2] + " " + args[i * 2 + 1]);
        ps.close();
    }

    public void connect() throws Exception {
        this.control = new IMRUJobControl<Model, T>();
        control.connect(options.host, options.port, options.hadoopConfPath,
                options.clusterConfPath);
        conf = control.conf;
        // set aggregation type
        if (options.aggTreeType.equals("none")) {
            control.selectNoAggregation(options.examplePaths);
        } else if (options.aggTreeType.equals("generic")) {
            control.selectGenericAggregation(options.examplePaths,
                    options.aggCount);
        } else if (options.aggTreeType.equals("nary")) {
            control.selectNAryAggregation(options.examplePaths, options.fanIn);
        } else {
            throw new IllegalArgumentException("Invalid aggregation tree type");
        }
        // hyracks connection
        hcc = new HyracksConnection(options.host, options.port);
    }

    public JobStatus run(IMRUJob<Model, T> job) throws Exception {
        return control.run(job, options.tempPath, options.app);
    }

    public FileSystem getHDFS() throws IOException {
        return FileSystem.get(conf);
    }

    public void clearTempDirectory() throws Exception {
        FileSystem dfs = getHDFS();
        // remove old intermediate models
        if (dfs.listStatus(new Path(options.tempPath)) != null)
            for (FileStatus f : dfs.listStatus(new Path(options.tempPath)))
                dfs.delete(f.getPath());
        dfs.close();
    }

    public void startClusterAndNodes() throws Exception {
        startCC(options.host, options.clusterPort, options.port);
        startNC1("nc1", options.host, options.clusterPort);
        startNC2("nc2", options.host, options.clusterPort);
    }

    public void startCC(String host, int clusterNetPort, int clientNetPort)
            throws Exception {
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
    }

    public void startNC1(String NC1_ID, String host, int clusterNetPort)
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

    public void startNC2(String NC2_ID, String host, int clusterNetPort)
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

    public void destroyApp(String hyracksAppName) throws Exception {
        hcc.destroyApplication(hyracksAppName);
    }

    public void deinit() throws Exception {
        nc2.stop();
        nc1.stop();
        cc.stop();
    }

    public void runJob(JobSpecification spec, String appName) throws Exception {
        spec.setFrameSize(FRAME_SIZE);
        JobId jobId = hcc.startJob(appName, spec, EnumSet
                .of(JobFlag.PROFILE_RUNTIME));
        hcc.waitForCompletion(jobId);
    }

    public void writeLocalFile(File file, byte[] bs) throws IOException {
        FileOutputStream out = new FileOutputStream(file);
        out.write(bs);
        out.close();
    }

    public void copyFromLocalToHDFS(String localPath, String hdfsPath)
            throws IOException {
        FileSystem dfs = getHDFS();
        dfs.mkdirs(new Path(hdfsPath).getParent());
        System.out.println("copy " + localPath + " to " + hdfsPath);
        dfs.copyFromLocalFile(new Path(localPath), new Path(hdfsPath));
        dfs.close();
    }

    public void uploadApp() throws Exception {
        File harFile = File.createTempFile("imru_app", ".zip");
        FileOutputStream out = new FileOutputStream(harFile);
        CreateHar.createHar(harFile);
        out.close();
        try {
            hcc.createApplication(options.app, harFile);
        } catch (Exception e) {
            e.printStackTrace();
        }
        harFile.delete();
    }
}
