/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.tests.integration.globalagg;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import edu.uci.ics.hyracks.api.client.HyracksConnection;
import edu.uci.ics.hyracks.api.client.IHyracksClientConnection;
import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.dataset.IHyracksDataset;
import edu.uci.ics.hyracks.api.dataset.IHyracksDatasetReader;
import edu.uci.ics.hyracks.api.dataset.ResultSetId;
import edu.uci.ics.hyracks.api.job.JobFlag;
import edu.uci.ics.hyracks.api.job.JobId;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.client.dataset.HyracksDataset;
import edu.uci.ics.hyracks.control.cc.ClusterControllerService;
import edu.uci.ics.hyracks.control.common.controllers.CCConfig;
import edu.uci.ics.hyracks.control.common.controllers.NCConfig;
import edu.uci.ics.hyracks.control.nc.NodeControllerService;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ResultFrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.FloatSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.Integer64SerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;
import edu.uci.ics.hyracks.tests.integration.AbstractIntegrationTest;

public abstract class AbstractGlobalAggIntegrationTest {
    private static final Logger LOGGER = Logger.getLogger(AbstractIntegrationTest.class.getName());

    public static final String[] NC_IDS = new String[] { "nc1", "nc2", "nc3", "nc4", "nc5", "nc6", "nc7", "nc8" };

    private static ClusterControllerService cc;
    private static NodeControllerService[] ncs;
    private static IHyracksClientConnection hcc;

    private final List<File> outputFiles;

    protected static int DEFAULT_MEM_PAGE_SIZE = 32768;
    protected static int DEFAULT_MEM_NUM_PAGES = 1000;
    protected static double DEFAULT_BLOOM_FILTER_FALSE_POSITIVE_RATE = 0.01;

    @Rule
    public TemporaryFolder outputFolder = new TemporaryFolder();

    public AbstractGlobalAggIntegrationTest() {
        outputFiles = new ArrayList<File>();
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
        File ccRoot = File.createTempFile(AbstractIntegrationTest.class.getName(), ".data", outDir);
        ccRoot.delete();
        ccRoot.mkdir();
        ccConfig.ccRoot = ccRoot.getAbsolutePath();
        cc = new ClusterControllerService(ccConfig);
        cc.start();
        
        ncs = new NodeControllerService[NC_IDS.length];

        for (int i = 0; i < NC_IDS.length; i++) {

            NCConfig ncConfig = new NCConfig();
            ncConfig.ccHost = "localhost";
            ncConfig.ccPort = 39001;
            ncConfig.clusterNetIPAddress = "127.0.0.1";
            ncConfig.dataIPAddress = "127.0.0.1";
            ncConfig.datasetIPAddress = "127.0.0.1";
            ncConfig.nodeId = NC_IDS[i];
            ncs[i] = new NodeControllerService(ncConfig);
            ncs[i].start();
        }

        hcc = new HyracksConnection(ccConfig.clientNetIpAddress, ccConfig.clientNetPort);
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Starting CC in " + ccRoot.getAbsolutePath());
        }
    }

    @AfterClass
    public static void deinit() throws Exception {
        for(NodeControllerService nc : ncs){
            nc.stop();
        }
        cc.stop();
    }

    protected JobId executeTest(JobSpecification spec) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(spec.toJSON().toString(2));
        }
        JobId jobId = hcc.startJob(spec, EnumSet.of(JobFlag.PROFILE_RUNTIME));
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(jobId.toString());
        }
        return jobId;
    }

    protected void runTest(JobSpecification spec) throws Exception {
        JobId jobId = executeTest(spec);
        hcc.waitForCompletion(jobId);
    }

    /**
     * Run the test specified in the job specification, and check the result by comparing the output files with the list
     * of expected files.
     * 
     * @param spec
     * @param expectedResults
     * @throws Exception
     */
    protected void runTestAndCheckCorrectness(JobSpecification spec, File[] expectedResults,
            RecordDescriptor fieldDescriptor) throws Exception {
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(spec.toJSON().toString(2));
        }
        JobId jobId = hcc.startJob(spec, EnumSet.of(JobFlag.PROFILE_RUNTIME));
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info(jobId.toString());
        }
        hcc.waitForCompletion(jobId);
        String testMethod = Thread.currentThread().getStackTrace()[1].getMethodName();
        if (outputFiles.size() != expectedResults.length) {
            throw new Exception("Test \"" + testMethod + "\" Failed: the count of output files are not same: expect "
                    + expectedResults.length + " but got " + outputFiles.size());
        }
        BufferedReader readerExpected, readerActual;
        String expectedLine = null, actualLine = null;

        for (int i = 0; i < expectedResults.length; i++) {
            int lineNumber = 1;
            readerExpected = new BufferedReader(new InputStreamReader(new FileInputStream(expectedResults[i])));
            readerActual = new BufferedReader(new InputStreamReader(new FileInputStream(outputFiles.get(i))));
            try {
                expectedLine = readerExpected.readLine();
                actualLine = readerActual.readLine();
                while (expectedLine != null) {

                    if (actualLine == null) {
                        break;
                    }
                    if (!compareString(expectedLine, actualLine, fieldDescriptor)) {
                        throw new Exception("Result for " + testMethod + " failed at line " + lineNumber + ":\n< "
                                + expectedLine + "\n> " + actualLine + "\n");
                    }
                    lineNumber++;
                    expectedLine = readerExpected.readLine();
                    actualLine = readerActual.readLine();
                }
                if (expectedLine != null || actualLine != null) {
                    throw new Exception("Result for " + testMethod + " failed at line " + lineNumber + ":\n< "
                            + expectedLine + "\n> " + actualLine + "\n");
                }
            } finally {
                readerExpected.close();
                readerActual.close();
            }
        }
    }

    private static Pattern SPLIT_PATTERN = Pattern.compile("\t");

    private static boolean compareString(String str1, String str2, RecordDescriptor fieldDecriptor) {
        if (str1.equalsIgnoreCase(str2)) {
            return true;
        }

        String[] fields1 = SPLIT_PATTERN.split(str1);
        String[] fields2 = SPLIT_PATTERN.split(str2);
        if (fields1.length != fields2.length) {
            return false;
        }

        for (int i = 0; i < fields1.length; i++) {
            if (fieldDecriptor.getFields()[i] instanceof IntegerSerializerDeserializer) {
                if (Integer.valueOf(fields1[i]).equals(Integer.valueOf(fields2[i]))) {
                    continue;
                }
            }
            if (fieldDecriptor.getFields()[i] instanceof Integer64SerializerDeserializer) {
                if (Long.valueOf(fields1[i]).equals(Long.valueOf(fields2[i]))) {
                    continue;
                }
            }
            if (fieldDecriptor.getFields()[i] instanceof FloatSerializerDeserializer) {
                if (Float.valueOf(fields1[i]).equals(Float.valueOf(fields2[i]))) {
                    continue;
                }
            }
            if (fieldDecriptor.getFields()[i] instanceof DoubleSerializerDeserializer) {
                if (Double.valueOf(fields1[i]).equals(Double.valueOf(fields2[i]))) {
                    continue;
                }
            }
            if (fields1[i].equalsIgnoreCase(fields2[i])) {
                continue;
            }
            // special case: for min/max string length
            if (fieldDecriptor.getFields()[i] instanceof UTF8StringSerializerDeserializer
                    && fields1[i].length() == fields2[i].length()) {
                continue;
            }
            return false;
        }
        return true;
    }

    protected List<String> readResults(JobSpecification spec, JobId jobId, ResultSetId resultSetId) throws Exception {
        int nReaders = 1;
        ByteBuffer resultBuffer = ByteBuffer.allocate(spec.getFrameSize());
        resultBuffer.clear();

        IFrameTupleAccessor frameTupleAccessor = new ResultFrameTupleAccessor(spec.getFrameSize());

        IHyracksDataset hyracksDataset = new HyracksDataset(hcc, spec.getFrameSize(), nReaders);
        IHyracksDatasetReader reader = hyracksDataset.createReader(jobId, resultSetId);

        List<String> resultRecords = new ArrayList<String>();
        ByteBufferInputStream bbis = new ByteBufferInputStream();

        int readSize = reader.read(resultBuffer);

        while (readSize > 0) {

            try {
                frameTupleAccessor.reset(resultBuffer);
                for (int tIndex = 0; tIndex < frameTupleAccessor.getTupleCount(); tIndex++) {
                    int start = frameTupleAccessor.getTupleStartOffset(tIndex);
                    int length = frameTupleAccessor.getTupleEndOffset(tIndex) - start;
                    bbis.setByteBuffer(resultBuffer, start);
                    byte[] recordBytes = new byte[length];
                    bbis.read(recordBytes, 0, length);
                    resultRecords.add(new String(recordBytes, 0, length));
                }
            } finally {
                bbis.close();
            }

            resultBuffer.clear();
            readSize = reader.read(resultBuffer);
        }
        return resultRecords;
    }

    protected boolean runTestAndCompareResults(JobSpecification spec, String[] expectedFileNames) throws Exception {
        JobId jobId = executeTest(spec);

        List<String> results;
        for (int i = 0; i < expectedFileNames.length; i++) {
            results = readResults(spec, jobId, spec.getResultSetIds().get(i));
            BufferedReader expectedFile = new BufferedReader(new FileReader(expectedFileNames[i]));

            String expectedLine, actualLine;
            int j = 0;
            while ((expectedLine = expectedFile.readLine()) != null) {
                actualLine = results.get(j).trim();
                Assert.assertEquals(expectedLine, actualLine);
                j++;
            }
            Assert.assertEquals(j, results.size());
            expectedFile.close();
        }

        hcc.waitForCompletion(jobId);
        return true;
    }

    protected File createTempFile() throws IOException {
        File tempFile = File.createTempFile(getClass().getName(), ".tmp", outputFolder.getRoot());
        if (LOGGER.isLoggable(Level.INFO)) {
            LOGGER.info("Output file: " + tempFile.getAbsolutePath());
        }
        outputFiles.add(tempFile);
        return tempFile;
    }

    protected void runTestAndDumpResults(JobSpecification spec, FileSplit[] splits) throws Exception {
        JobId jobId = executeTest(spec);

        hcc.waitForCompletion(jobId);
        for (FileSplit fs : splits) {
            LOGGER.severe(fs.getLocalFile().getFile().getAbsolutePath());
            BufferedReader freader = new BufferedReader(new FileReader(fs.getLocalFile().getFile().getAbsolutePath()));

            String l;
            boolean isFirst = true;
            while ((l = freader.readLine()) != null && isFirst) {
                LOGGER.severe(l);
                isFirst = false;
            }
            freader.close();
        }
    }

}
