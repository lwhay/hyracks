/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.tests.integration;

import java.io.DataOutput;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.constraints.PartitionConstraintHelper;
import edu.uci.ics.hyracks.api.dataflow.IConnectorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.IOperatorDescriptor;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriter;
import edu.uci.ics.hyracks.api.dataflow.value.INullWriterFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryComparatorFactory;
import edu.uci.ics.hyracks.data.std.accessors.PointableBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.normalizers.IntegerNormalizedKeyComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.FloatParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IntegerParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.UTF8StringParserFactory;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.std.connectors.OneToOneConnectorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.ConstantFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.DelimitedDataTupleParserFactory;
import edu.uci.ics.hyracks.dataflow.std.file.FileScanOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.dataflow.std.file.PlainFileWriterOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.ExternalGroupOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.group.HashSpillableTableFactory;
import edu.uci.ics.hyracks.dataflow.std.group.IFieldAggregateDescriptorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.CountFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.IntSumFieldAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.group.aggregators.MultiFieldsAggregatorFactory;
import edu.uci.ics.hyracks.dataflow.std.join.GraceHashGroupJoinOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.join.GraceHashJoinOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.join.HybridHashGroupJoinOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.join.HybridHashJoinOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.join.InMemoryHashGroupJoinOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.join.InMemoryHashJoinOperatorDescriptor;

public class TPCHCustomerOrderGroupJoinTest {
//    private static final Logger LOGGER = Logger.getLogger(AbstractIntegrationTest.class.getName());

    public static final String NC1_ID = "nc1";

    private static ClusterControllerService cc;
    private static NodeControllerService nc1;
    private static IHyracksClientConnection hcc;

    private static List<File> outputFiles;

//    private static final boolean DEBUG = true;
    private static String scale = "s1";
    private static String tablePath = "data/tpch0.001/";
    private static String custTable = tablePath + "customer.tbl";
    private static String ordTable = tablePath + "orders.tbl";
    private static String ordTable2 = tablePath + "orders2.tbl";
    
    private static File file  = new File("/home/manish/asterix-project/results/result_" + scale + System.currentTimeMillis() + ".out");

    private static int memsize = 5;
    private static int custSize = 1000000;
    private static int recordsPerFrame = 200;
    private static double factor = 1.2;
    
    private static long runTime;

    private static FileSplit[] custSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
            custTable))) };
    private static IFileSplitProvider custSplitsProvider = new ConstantFileSplitProvider(custSplits);
    private static RecordDescriptor custDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            IntegerSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

    private static FileSplit[] ordersSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
            ordTable))) };
    private static IFileSplitProvider ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
    private static RecordDescriptor ordersDesc = new RecordDescriptor(new ISerializerDeserializer[] {
            IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE });

    private static FileSplit[] ordersSplits2 = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(
    		ordTable2))) };
    private static IFileSplitProvider ordersSplitsProvider2 = new ConstantFileSplitProvider(ordersSplits2);
    private static RecordDescriptor ordersDesc2 = new RecordDescriptor(new ISerializerDeserializer[] {
            IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
            UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

    @Rule
    public static TemporaryFolder outputFolder = new TemporaryFolder();

    public TPCHCustomerOrderGroupJoinTest() throws IOException{
        outputFiles = new ArrayList<File>();
        runTime = 0;
    }

    @Test
    public void main() throws Exception{
        PrintStream printStream;

//        for (int i=1; i<=50; i++){
/*
        	switch(i%5){
        	case 1:
        		scale = "s0.25";
        		break;
        	case 2:
        		scale = "s0.5";
        		break;
        	case 3:
        		scale = "s1";
        		break;
        	case 4:
        		scale = "s2";
        		break;
        	case 0:
        		scale = "s4";
        		break;
        	default: break;
        	}
*/        	
        scale = "s16";
        tablePath = "/media/New Volume_/tpch/" + scale + "/";
        custTable = tablePath + "customer.tbl";
        ordTable = tablePath + "orders.tbl";
        ordTable2 = tablePath + "orders2.tbl";

        custSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(custTable))) };
        custSplitsProvider = new ConstantFileSplitProvider(custSplits);
        ordersSplits = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(ordTable))) };
        ordersSplitsProvider = new ConstantFileSplitProvider(ordersSplits);
        ordersSplits2 = new FileSplit[] { new FileSplit(NC1_ID, new FileReference(new File(ordTable2))) };
        ordersSplitsProvider2 = new ConstantFileSplitProvider(ordersSplits2);

//    	custPrint();
//    	ordPrint();
//    	ord2Print();

    	for (int i=1; i<=6; i++){
    		file  = new File("/home/manish/asterix-project/results/IMGJ_" + scale + "_" + System.currentTimeMillis() + ".out");
    		printStream = new PrintStream(new FileOutputStream(file));
    		System.setErr(printStream);

//    		init();

    		customerOrderInMemoryGroupJoin();
/*    		customerOrderGraceHashGroupJoin();
    		customerOrderHybridHashGroupJoin();

    		customerOrderInMemoryJoinAndGroup();
    		customerOrderGraceHashJoinAndGroup();
    		customerOrderHybridHashJoinAndGroup();
*/
//    		deinit();
    		printStream.close();
    	}
    }
    
    @BeforeClass
    public static void init() throws Exception {
        CCConfig ccConfig = new CCConfig();
        ccConfig.clientNetIpAddress = "127.0.0.1";
        ccConfig.clientNetPort = 39000;
        ccConfig.clusterNetIpAddress = "127.0.0.1";
        ccConfig.clusterNetPort = 39001;
        ccConfig.profileDumpPeriod = 10000;
        File outDir = new File("target/ClusterController");
        outDir.mkdirs();
        File ccRoot = File.createTempFile(TPCHCustomerOrderGroupJoinTest.class.getName(), ".data", outDir);
        ccRoot.delete();
        ccRoot.mkdir();
        ccConfig.ccRoot = ccRoot.getAbsolutePath();
        cc = new ClusterControllerService(ccConfig);
        cc.start();

        NCConfig ncConfig1 = new NCConfig();
        ncConfig1.ccHost = "localhost";
        ncConfig1.ccPort = 39001;
        ncConfig1.clusterNetIPAddress = "127.0.0.1";
        ncConfig1.dataIPAddress = "127.0.0.1";
        ncConfig1.nodeId = NC1_ID;
        nc1 = new NodeControllerService(ncConfig1);
        nc1.start();

/*        NCConfig ncConfig2 = new NCConfig();
        ncConfig2.ccHost = "localhost";
        ncConfig2.ccPort = 39001;
        ncConfig2.clusterNetIPAddress = "127.0.0.1";
        ncConfig2.dataIPAddress = "127.0.0.1";
        ncConfig2.nodeId = NC1_ID;
        nc2 = new NodeControllerService(ncConfig2);
        nc2.start();
*/
        hcc = new HyracksConnection(ccConfig.clientNetIpAddress, ccConfig.clientNetPort);
        hcc.createApplication("test", null);
    }
    
    @AfterClass
    public static void deinit() throws Exception {
//        nc2.stop();
        nc1.stop();
        cc.stop();
    }

    protected static void runTest(JobSpecification spec) throws Exception {
        JobId jobId = hcc.createJob("test", spec, EnumSet.of(JobFlag.PROFILE_RUNTIME));
        hcc.start(jobId);
        hcc.waitForCompletion(jobId);
//        dumpOutputFiles();
    }
    
    private static void dumpOutputFiles() {
            for (File f : outputFiles) {
                if (f.exists() && f.isFile()) {
                    try {
                        System.err.println("Reading file: " + f.getAbsolutePath());
                        		//+ " in test: " + getClass().getName());
                        String data = FileUtils.readFileToString(f);
                        System.err.println(data);
                    } catch (IOException e) {
                    	System.err.println("Error reading file: " + f.getAbsolutePath());
                    	System.err.println(e.getMessage());
                    }
                }
            }
    }
    
    protected File createTempFile() throws IOException {
        File tempFile = File.createTempFile(getClass().getName(), ".tmp", outputFolder.getRoot());
        System.err.println("Output file: " + tempFile.getAbsolutePath());
        outputFiles.add(tempFile);
        return tempFile;
    }
    
    private static void clearCache(){
    	try{
    		String cmd = "sync";
    		Runtime run = Runtime.getRuntime();
    		Process pr1 = run.exec(cmd);
    		pr1.waitFor();
    		cmd = "echo 3 > /proc/sys/vm/drop_caches";
    		Process pr2 = run.exec(cmd);
    		pr2.waitFor();
    		System.err.println("~Cache dropped");
    	}
    	catch (Exception e){
    		System.err.println("~Cache drop fail");
    	}
    }
    
    private static class NoopNullWriterFactory implements INullWriterFactory {

        private static final long serialVersionUID = 1L;
        public static final NoopNullWriterFactory INSTANCE = new NoopNullWriterFactory();

        private NoopNullWriterFactory() {
        }

        @Override
        public INullWriter createNullWriter() {
            return new INullWriter() {
                @Override
                public void writeNull(DataOutput out) throws HyracksDataException {
                    try {
                        out.writeInt(0);
                    } catch (IOException e) {
                        throw new HyracksDataException(e);
                    }
                }
            };
        }
    }
    
    public void dispIO(String s) throws IOException {
    	Scanner sc = new Scanner(new File("/proc/self/io"));
    	while(sc.hasNext()){
    		System.err.println("~\t" + s + " " + sc.nextLine());
    	}
    }

//    @Test
    public void custPrint() throws Exception {
    	System.err.println("~Starting Tests");
    	JobSpecification spec = new JobSpecification();

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
                		IntegerParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);

        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] {
                new FileSplit(NC1_ID, createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, "|");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn, custScanner, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
        System.err.println();
//        dummyWait();
    }
    
//    @Test
    public void ordPrint() throws Exception {
    	System.err.println("~" + this.getClass().getSimpleName());
    	System.err.println("~Starting Tests");
    	JobSpecification spec = new JobSpecification();

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { 
                		IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] {
                new FileSplit(NC1_ID, createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, "|");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn, ordScanner, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
        System.err.println();
//        dummyWait();
    }

//    @Test
    public void ord2Print() throws Exception {
    	System.err.println("~" + this.getClass().getSimpleName());
    	System.err.println("~Starting Tests");
    	JobSpecification spec = new JobSpecification();

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider2,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
                		IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE }, '|'), ordersDesc2);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] {
                new FileSplit(NC1_ID, createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, "|");
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor conn = new OneToOneConnectorDescriptor(spec);
        spec.connect(conn, ordScanner, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
        System.err.println();
//        dummyWait();
    }

    /*
     * TPCH Customer table: CREATE TABLE CUSTOMER ( C_CUSTKEY INTEGER NOT NULL,
     * C_NAME VARCHAR(25) NOT NULL, C_ADDRESS VARCHAR(40) NOT NULL, C_NATIONKEY
     * INTEGER NOT NULL, C_PHONE CHAR(15) NOT NULL, C_ACCTBAL DECIMAL(15,2) NOT
     * NULL, C_MKTSEGMENT CHAR(10) NOT NULL, C_COMMENT VARCHAR(117) NOT NULL );
     * 
     * TPCH Orders table: CREATE TABLE ORDERS ( O_ORDERKEY INTEGER NOT NULL,
     * O_CUSTKEY INTEGER NOT NULL, O_ORDERSTATUS CHAR(1) NOT NULL, O_TOTALPRICE
     * DECIMAL(15,2) NOT NULL, O_ORDERDATE DATE NOT NULL, O_ORDERPRIORITY
     * CHAR(15) NOT NULL, O_CLERK CHAR(15) NOT NULL, O_SHIPPRIORITY INTEGER NOT
     * NULL, O_COMMENT VARCHAR(79) NOT NULL );
     */

//    @Test
    public void customerOrderInMemoryGroupJoin() throws Exception {
    	clearCache();
    	System.err.println("~\n~customerOrderInMemoryGroupJoin start");
    	dispIO("pre");
    	runTime = System.currentTimeMillis();
        JobSpecification spec = new JobSpecification();

        RecordDescriptor custOrderGroupJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
        		IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        
        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { 
                		IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
                		IntegerParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);
        
        INullWriterFactory[] nullWriterFactories = new INullWriterFactory[1];
        for (int j = 0; j < nullWriterFactories.length; j++) {
            nullWriterFactories[j] = NoopNullWriterFactory.INSTANCE;
        }
        
        InMemoryHashGroupJoinOperatorDescriptor groupJoin = new InMemoryHashGroupJoinOperatorDescriptor(
                spec,
                new int[] { 0 },
                new int[] { 1 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(IntegerPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(false) }),
                custOrderGroupJoinDesc, nullWriterFactories, custSize);
        
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, groupJoin, NC1_ID);

        ExternalGroupOperatorDescriptor finalGroup = new ExternalGroupOperatorDescriptor(
                spec,
                new int[] { 1 },
                4,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
                        .of(IntegerPointable.FACTORY) },
                new IntegerNormalizedKeyComputerFactory(),
		        new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(false) }),
		        new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false) }),
                custOrderGroupJoinDesc,
                new HashSpillableTableFactory(
                        new FieldHashPartitionComputerFactory(
                        		new int[] { 1 },
                                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                        .of(IntegerPointable.FACTORY) }),
                        128), true);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, finalGroup, NC1_ID);

        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, "|");
        
        
//        IOperatorDescriptor printer = DEBUG ? new PrinterOperatorDescriptor(spec) : new NullSinkOperatorDescriptor(spec);
        
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, groupJoin, 0);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, groupJoin, 1);

//        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
//        spec.connect(joinPrinterConn, groupJoin, 0, printer, 0);

        IConnectorDescriptor finalGroupConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(finalGroupConn, groupJoin, 0, finalGroup, 0);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, finalGroup, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
        
    	runTime = System.currentTimeMillis() - runTime;
    	dispIO("post");
    	System.err.println("~customerOrderInMemoryGroupJoin stop. run time: " + runTime);
    }

//    @Test
    public void customerOrderGraceHashGroupJoin() throws Exception {
    	clearCache();
    	System.err.println("~\n~customerOrderGraceHashGroupJoin start");
    	dispIO("pre");
    	runTime = System.currentTimeMillis();
        JobSpecification spec = new JobSpecification();

        RecordDescriptor custOrderGroupJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { 
                		IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
                		IntegerParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);
        
        INullWriterFactory[] nullWriterFactories = new INullWriterFactory[1];
        for (int j = 0; j < nullWriterFactories.length; j++) {
            nullWriterFactories[j] = NoopNullWriterFactory.INSTANCE;
        }        
        
        GraceHashGroupJoinOperatorDescriptor groupJoin = new GraceHashGroupJoinOperatorDescriptor(
                spec,
                memsize,
                custSize,
                recordsPerFrame,
                factor,
                new int[] { 0 },
                new int[] { 1 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(IntegerPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(false) }),
                custOrderGroupJoinDesc, nullWriterFactories);
        
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, groupJoin, NC1_ID);

        ExternalGroupOperatorDescriptor finalGroup = new ExternalGroupOperatorDescriptor(
                spec,
                new int[] { 1 },
                4,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
                        .of(IntegerPointable.FACTORY) },
                new IntegerNormalizedKeyComputerFactory(),
		        new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(false) }),
		        new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false) }),
                custOrderGroupJoinDesc,
                new HashSpillableTableFactory(
                        new FieldHashPartitionComputerFactory(
                        		new int[] { 1 },
                                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                        .of(IntegerPointable.FACTORY) }),
                        128), true);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, finalGroup, NC1_ID);
        
        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, "|");
        
        
//        IOperatorDescriptor printer = DEBUG ? new PrinterOperatorDescriptor(spec) : new NullSinkOperatorDescriptor(spec);
        
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, groupJoin, 0);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, groupJoin, 1);

//        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
//        spec.connect(joinPrinterConn, groupJoin, 0, printer, 0);

        IConnectorDescriptor finalGroupConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(finalGroupConn, groupJoin, 0, finalGroup, 0);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, finalGroup, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);

    	runTime = System.currentTimeMillis() - runTime;
    	dispIO("post");
    	System.err.println("~customerOrderGraceHashGroupJoin stop. run time: " + runTime);
    }

//    @Test
    public void customerOrderHybridHashGroupJoin() throws Exception {
    	clearCache();
    	System.err.println("~\n~customerOrderHybridHashGroupJoin start");
    	dispIO("pre");
    	runTime = System.currentTimeMillis();
        JobSpecification spec = new JobSpecification();

        RecordDescriptor custOrderGroupJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] { 
                		IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE }, '|'), ordersDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
                		IntegerParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);
        
        INullWriterFactory[] nullWriterFactories = new INullWriterFactory[1];
        for (int j = 0; j < nullWriterFactories.length; j++) {
            nullWriterFactories[j] = NoopNullWriterFactory.INSTANCE;
        }        

        HybridHashGroupJoinOperatorDescriptor groupJoin = new HybridHashGroupJoinOperatorDescriptor(
                spec,
                memsize,
                custSize,
                recordsPerFrame,
                factor,
                new int[] { 0 },
                new int[] { 1 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(IntegerPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(false) }),
                custOrderGroupJoinDesc,
                nullWriterFactories);
        
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, groupJoin, NC1_ID);

        ExternalGroupOperatorDescriptor finalGroup = new ExternalGroupOperatorDescriptor(
                spec,
                new int[] { 1 },
                4,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
                        .of(IntegerPointable.FACTORY) },
                new IntegerNormalizedKeyComputerFactory(),
		        new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(false) }),
		        new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false) }),
                custOrderGroupJoinDesc,
                new HashSpillableTableFactory(
                        new FieldHashPartitionComputerFactory(
                        		new int[] { 1 },
                                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                        .of(IntegerPointable.FACTORY) }),
                        128), true);

        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, finalGroup, NC1_ID);
        
        IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
                createTempFile().getAbsolutePath()) });
        IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, "|");
        
        
//        IOperatorDescriptor printer = DEBUG ? new PrinterOperatorDescriptor(spec) : new NullSinkOperatorDescriptor(spec);
        
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

        IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(custJoinConn, custScanner, 0, groupJoin, 0);

        IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(ordJoinConn, ordScanner, 0, groupJoin, 1);

//        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
//        spec.connect(joinPrinterConn, groupJoin, 0, printer, 0);

        IConnectorDescriptor finalGroupConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(finalGroupConn, groupJoin, 0, finalGroup, 0);

        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
        spec.connect(joinPrinterConn, finalGroup, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);

    	runTime = System.currentTimeMillis() - runTime;
    	dispIO("post");
    	System.err.println("~customerOrderHybridHashGroupJoin stop. run time: " + runTime);
    }

//    @Test
    public void customerOrderInMemoryJoinAndGroup() throws Exception {
    	clearCache();
    	System.err.println("~\n~customerOrderInMemoryJoinAndGroup start");
    	dispIO("pre");
    	runTime = System.currentTimeMillis();
        JobSpecification spec = new JobSpecification();

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider2,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
                		IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE }, '|'), ordersDesc2);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
                		IntegerParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);

        INullWriterFactory[] nullWriterFactories = new INullWriterFactory[ordersDesc2.getFieldCount()];
        for (int j = 0; j < nullWriterFactories.length; j++) {
            nullWriterFactories[j] = NoopNullWriterFactory.INSTANCE;
        }

        InMemoryHashJoinOperatorDescriptor join = new InMemoryHashJoinOperatorDescriptor(
                spec,
                new int[] { 0 },
                new int[] { 1 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(IntegerPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                custOrderJoinDesc, true, false, nullWriterFactories, null, custSize*10);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        RecordDescriptor groupDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        
        ExternalGroupOperatorDescriptor group = new ExternalGroupOperatorDescriptor(
                spec,
                new int[] { 0 },
                4,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
                        .of(IntegerPointable.FACTORY) },
                new IntegerNormalizedKeyComputerFactory(),
		        new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(custOrderJoinDesc.getFieldCount()-1, false) }),
		        new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false) }),
                groupDesc,
                new HashSpillableTableFactory(
                        new FieldHashPartitionComputerFactory(
                        		new int[] { 0 },
                                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                        .of(IntegerPointable.FACTORY) }),
                        128), true);
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, group, NC1_ID);
		
        ExternalGroupOperatorDescriptor finalGroup = new ExternalGroupOperatorDescriptor(
                spec,
                new int[] { 1 },
                4,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
                        .of(IntegerPointable.FACTORY) },
                new IntegerNormalizedKeyComputerFactory(),
		        new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(false) }),
		        new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false) }),
                groupDesc,
                new HashSpillableTableFactory(
                        new FieldHashPartitionComputerFactory(
                        		new int[] { 1 },
                                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                        .of(IntegerPointable.FACTORY) }),
                        128), true);
        
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, finalGroup, NC1_ID);

		IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
		        createTempFile().getAbsolutePath()) });
		IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, "|");
		        
//		IOperatorDescriptor printer = new LineFileWriteOperatorDescriptor(spec, outSplits.getFileSplits(), new int[] { 0, custOrderJoinDesc.getFieldCount()-1 });
		
		//IOperatorDescriptor printer = DEBUG ? new PrinterOperatorDescriptor(spec) : new NullSinkOperatorDescriptor(spec);
		
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

		IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
		spec.connect(ordJoinConn, ordScanner, 0, join, 1);
		
		IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
		spec.connect(custJoinConn, custScanner, 0, join, 0);
		
		IConnectorDescriptor joinGroupConn = new OneToOneConnectorDescriptor(spec);
		spec.connect(joinGroupConn, join, 0, group, 0);
		
		IConnectorDescriptor groupFinalGroupConn = new OneToOneConnectorDescriptor(spec);
		spec.connect(groupFinalGroupConn, group, 0, finalGroup, 0);

		IConnectorDescriptor finalGroupConn = new OneToOneConnectorDescriptor(spec);
		spec.connect(finalGroupConn, finalGroup, 0, printer, 0);

//        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
//        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
        
    	runTime = System.currentTimeMillis() - runTime;
    	dispIO("post");
    	System.err.println("~customerOrderInMemoryJoinAndGroup stop. run time: " + runTime);
    }

//    @Test
    public void customerOrderGraceHashJoinAndGroup() throws Exception {
    	clearCache();
    	System.err.println("~\n~customerOrderGraceHashJoinAndGroup start");
    	dispIO("pre");
    	runTime = System.currentTimeMillis();
        JobSpecification spec = new JobSpecification();

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider2,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
                		IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE }, '|'), ordersDesc2);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
                		IntegerParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);

        INullWriterFactory[] nullWriterFactories = new INullWriterFactory[ordersDesc2.getFieldCount()];
        for (int j = 0; j < nullWriterFactories.length; j++) {
            nullWriterFactories[j] = NoopNullWriterFactory.INSTANCE;
        }

        GraceHashJoinOperatorDescriptor join = new GraceHashJoinOperatorDescriptor(
                spec,
                memsize,
                custSize,
                recordsPerFrame,
                factor,
                new int[] { 0 },
                new int[] { 1 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(IntegerPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                custOrderJoinDesc, true, false, nullWriterFactories, null);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        RecordDescriptor groupDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        
        ExternalGroupOperatorDescriptor group = new ExternalGroupOperatorDescriptor(
                spec,
                new int[] { 0 },
                4,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
                        .of(IntegerPointable.FACTORY) },
                new IntegerNormalizedKeyComputerFactory(),
		        new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(custOrderJoinDesc.getFieldCount()-1, false) }),
		        new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false) }),
                groupDesc,
                new HashSpillableTableFactory(
                        new FieldHashPartitionComputerFactory(
                        		new int[] { 0 },
                                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                        .of(IntegerPointable.FACTORY) }),
                        128), true);
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, group, NC1_ID);
		
        ExternalGroupOperatorDescriptor finalGroup = new ExternalGroupOperatorDescriptor(
                spec,
                new int[] { 1 },
                4,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
                        .of(IntegerPointable.FACTORY) },
                new IntegerNormalizedKeyComputerFactory(),
		        new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(false) }),
		        new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false) }),
                groupDesc,
                new HashSpillableTableFactory(
                        new FieldHashPartitionComputerFactory(
                        		new int[] { 1 },
                                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                        .of(IntegerPointable.FACTORY) }),
                        128), true);
        
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, finalGroup, NC1_ID);

		IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
		        createTempFile().getAbsolutePath()) });
		IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, "|");
		        
//		IOperatorDescriptor printer = new LineFileWriteOperatorDescriptor(spec, outSplits.getFileSplits(), new int[] { 0, custOrderJoinDesc.getFieldCount()-1 });
		
		//IOperatorDescriptor printer = DEBUG ? new PrinterOperatorDescriptor(spec) : new NullSinkOperatorDescriptor(spec);
		
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

		IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
		spec.connect(ordJoinConn, ordScanner, 0, join, 1);
		
		IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
		spec.connect(custJoinConn, custScanner, 0, join, 0);
		
		IConnectorDescriptor joinGroupConn = new OneToOneConnectorDescriptor(spec);
		spec.connect(joinGroupConn, join, 0, group, 0);
		
		IConnectorDescriptor groupFinalGroupConn = new OneToOneConnectorDescriptor(spec);
		spec.connect(groupFinalGroupConn, group, 0, finalGroup, 0);

		IConnectorDescriptor finalGroupConn = new OneToOneConnectorDescriptor(spec);
		spec.connect(finalGroupConn, finalGroup, 0, printer, 0);

//        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
//        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
        
    	runTime = System.currentTimeMillis() - runTime;
    	dispIO("post");
    	System.err.println("~customerOrderGraceHashJoinAndGroup stop. run time: " + runTime);
    }

//    @Test
    public void customerOrderHybridHashJoinAndGroup() throws Exception {
    	clearCache();
    	System.err.println("~\n~customerOrderHybridHashJoinAndGroup start");
    	dispIO("pre");
    	runTime = System.currentTimeMillis();
        JobSpecification spec = new JobSpecification();

        RecordDescriptor custOrderJoinDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, FloatSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });

        FileScanOperatorDescriptor ordScanner = new FileScanOperatorDescriptor(spec, ordersSplitsProvider2,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
                		IntegerParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE }, '|'), ordersDesc2);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, ordScanner, NC1_ID);

        FileScanOperatorDescriptor custScanner = new FileScanOperatorDescriptor(spec, custSplitsProvider,
                new DelimitedDataTupleParserFactory(new IValueParserFactory[] {
                		IntegerParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, IntegerParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, FloatParserFactory.INSTANCE,
                        UTF8StringParserFactory.INSTANCE, UTF8StringParserFactory.INSTANCE,
                        }, '|'), custDesc);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, custScanner, NC1_ID);

        INullWriterFactory[] nullWriterFactories = new INullWriterFactory[ordersDesc2.getFieldCount()];
        for (int j = 0; j < nullWriterFactories.length; j++) {
            nullWriterFactories[j] = NoopNullWriterFactory.INSTANCE;
        }

        HybridHashJoinOperatorDescriptor join = new HybridHashJoinOperatorDescriptor(
                spec,
                memsize,
                custSize,
                recordsPerFrame,
                factor,
                new int[] { 0 },
                new int[] { 1 },
                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory.of(IntegerPointable.FACTORY) },
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory.of(IntegerPointable.FACTORY) },
                custOrderJoinDesc, true, false, nullWriterFactories, null);
        PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, join, NC1_ID);

        RecordDescriptor groupDesc = new RecordDescriptor(new ISerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE });
        
        ExternalGroupOperatorDescriptor group = new ExternalGroupOperatorDescriptor(
                spec,
                new int[] { 0 },
                4,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
                        .of(IntegerPointable.FACTORY) },
                new IntegerNormalizedKeyComputerFactory(),
		        new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(custOrderJoinDesc.getFieldCount()-1, false) }),
		        new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false) }),
                groupDesc,
                new HashSpillableTableFactory(
                        new FieldHashPartitionComputerFactory(
                        		new int[] { 0 },
                                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                        .of(IntegerPointable.FACTORY) }),
                        128), true);
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, group, NC1_ID);
		
        ExternalGroupOperatorDescriptor finalGroup = new ExternalGroupOperatorDescriptor(
                spec,
                new int[] { 1 },
                4,
                new IBinaryComparatorFactory[] { PointableBinaryComparatorFactory
                        .of(IntegerPointable.FACTORY) },
                new IntegerNormalizedKeyComputerFactory(),
		        new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new CountFieldAggregatorFactory(false) }),
		        new MultiFieldsAggregatorFactory( new IFieldAggregateDescriptorFactory[] { new IntSumFieldAggregatorFactory(1, false) }),
                groupDesc,
                new HashSpillableTableFactory(
                        new FieldHashPartitionComputerFactory(
                        		new int[] { 1 },
                                new IBinaryHashFunctionFactory[] { PointableBinaryHashFunctionFactory
                                        .of(IntegerPointable.FACTORY) }),
                        128), true);
        
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, finalGroup, NC1_ID);

		IFileSplitProvider outSplits = new ConstantFileSplitProvider(new FileSplit[] { new FileSplit(NC1_ID,
		        createTempFile().getAbsolutePath()) });
		IOperatorDescriptor printer = new PlainFileWriterOperatorDescriptor(spec, outSplits, "|");
		        
//		IOperatorDescriptor printer = new LineFileWriteOperatorDescriptor(spec, outSplits.getFileSplits(), new int[] { 0, custOrderJoinDesc.getFieldCount()-1 });
		
		//IOperatorDescriptor printer = DEBUG ? new PrinterOperatorDescriptor(spec) : new NullSinkOperatorDescriptor(spec);
		
		PartitionConstraintHelper.addAbsoluteLocationConstraint(spec, printer, NC1_ID);

		IConnectorDescriptor ordJoinConn = new OneToOneConnectorDescriptor(spec);
		spec.connect(ordJoinConn, ordScanner, 0, join, 1);
		
		IConnectorDescriptor custJoinConn = new OneToOneConnectorDescriptor(spec);
		spec.connect(custJoinConn, custScanner, 0, join, 0);
		
		IConnectorDescriptor joinGroupConn = new OneToOneConnectorDescriptor(spec);
		spec.connect(joinGroupConn, join, 0, group, 0);
		
		IConnectorDescriptor groupFinalGroupConn = new OneToOneConnectorDescriptor(spec);
		spec.connect(groupFinalGroupConn, group, 0, finalGroup, 0);

		IConnectorDescriptor finalGroupConn = new OneToOneConnectorDescriptor(spec);
		spec.connect(finalGroupConn, finalGroup, 0, printer, 0);

//        IConnectorDescriptor joinPrinterConn = new OneToOneConnectorDescriptor(spec);
//        spec.connect(joinPrinterConn, join, 0, printer, 0);

        spec.addRoot(printer);
        runTest(spec);
        
    	runTime = System.currentTimeMillis() - runTime;
    	dispIO("post");
    	System.err.println("~customerOrderHybridHashJoinAndGroup stop. run time: " + runTime);
    }
}