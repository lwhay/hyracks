package edu.uci.ics.hyracks.imru.example.bgd2;

import java.io.BufferedReader;
import java.io.DataOutput;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.util.Scanner;
import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;
import edu.uci.ics.hyracks.imru.api2.IMRUJobTmp;
import edu.uci.ics.hyracks.imru.api2.IMRUJobControl;
import edu.uci.ics.hyracks.imru.base.IJobFactory;
import edu.uci.ics.hyracks.imru.example.bgd.R;
import edu.uci.ics.hyracks.imru.jobgen.GenericAggregationIMRUJobFactory;
import edu.uci.ics.hyracks.imru.jobgen.NAryAggregationIMRUJobFactory;
import edu.uci.ics.hyracks.imru.jobgen.NoAggregationIMRUJobFactory;
import edu.uci.ics.hyracks.imru.runtime.IMRUDriver;
import edu.uci.ics.hyracks.imru.test.ImruTest;

/**
 * Generic main class for running Hyracks IMRU jobs.
 * 
 * @author Josh Rosen
 */
public class BGDMain2 implements IMRUJobTmp<LinearModel, LossGradient> {
    private final int numFeatures;

    public BGDMain2(int numFeatures) {
        this.numFeatures = numFeatures;
    }

    private LossGradient lossGradientMap;
    private LossGradient lossGradientReduce;
    private LossGradient lossGradientUpdate;
    private IFrameTupleAccessor accessor;
    private LinearExample example;

    @Override
    public void openMap(LinearModel model, int cachedDataFrameSize)
            throws HyracksDataException {
        R.p("openMap " + this);
        lossGradientMap = new LossGradient();
        lossGradientMap.loss = 0.0f;
        lossGradientMap.gradient = new float[model.numFeatures];
        accessor = new FrameTupleAccessor(cachedDataFrameSize,
                RecordDescriptorUtils.getDummyRecordDescriptor(2));
        example = new LinearExample();
    }

    @Override
    public void map(ByteBuffer input, LinearModel model, int cachedDataFrameSize)
            throws HyracksDataException {
        accessor.reset(input);
        int tupleCount = accessor.getTupleCount();
        R.p("map " + tupleCount + " " + this);
        for (int i = 0; i < tupleCount; i++) {
            example.reset(accessor, i);
            float innerProduct = example.dot(model.weights);
            float diff = (example.getLabel() - innerProduct);
            lossGradientMap.loss += diff * diff; // Use L2 loss
            // function.
            example.computeGradient(model.weights, innerProduct,
                    lossGradientMap.gradient);
        }
    }

    @Override
    public LossGradient closeMap(LinearModel model, int cachedDataFrameSize)
            throws HyracksDataException {
        R.p("closeMap " + this);
        return lossGradientMap;
    }

    @Override
    public void openReduce() throws HyracksDataException {
        R.p("openReduce " + this);
        new Error().printStackTrace();
    }

    @Override
    public void reduce(LossGradient input) throws HyracksDataException {
        R.p("reduce " + this);
        if (lossGradientReduce == null) {
            lossGradientReduce = input;
        } else {
            lossGradientReduce.loss += input.loss;
            for (int i = 0; i < lossGradientReduce.gradient.length; i++) {
                lossGradientReduce.gradient[i] += input.gradient[i];
            }
        }
    }

    @Override
    public LossGradient closeReduce() throws HyracksDataException {
        R.p("closeReduce " + this);
        return lossGradientReduce;
    }

    @Override
    public void openUpdate(LinearModel model) throws HyracksDataException {
        R.p("openUpdate " + this);
    }

    @Override
    public void update(LossGradient input, LinearModel model)
            throws HyracksDataException {
        R.p("update " + this);
        if (lossGradientUpdate == null) {
            lossGradientUpdate = input;
        } else {
            lossGradientUpdate.loss += input.loss;
            for (int i = 0; i < lossGradientUpdate.gradient.length; i++) {
                lossGradientUpdate.gradient[i] += input.gradient[i];
            }
        }
    }

    @Override
    public void closeUpdate(LinearModel model) throws HyracksDataException {
        R.p("closeUpdate " + this);
        // Update loss
        model.loss = lossGradientUpdate.loss;
        model.loss += model.regularizationConstant * norm(model.weights.array);
        // Update weights
        for (int i = 0; i < model.weights.length; i++) {
            model.weights.array[i] = (model.weights.array[i] - lossGradientUpdate.gradient[i]
                    * model.stepSize)
                    * (1.0f - model.stepSize * model.regularizationConstant);
        }
        model.stepSize *= 0.9;
        model.roundsRemaining--;
    }

    /**
     * @return The Euclidean norm of the vector.
     */
    public static double norm(float[] vec) {
        double norm = 0.0;
        for (double comp : vec) {
            norm += comp * comp;
        }
        return Math.sqrt(norm);
    }

    @Override
    public int getCachedDataFrameSize() {
        return 4 * 1024;
    }

    @Override
    public boolean shouldTerminate(LinearModel model) {
        return model.roundsRemaining == 0;
    }

    @Override
    public void parse(IHyracksTaskContext ctx, InputStream in,
            IFrameWriter writer) throws HyracksDataException {
        ByteBuffer frame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        appender.reset(frame, true);
        ArrayTupleBuilder tb = new ArrayTupleBuilder(2);
        DataOutput dos = tb.getDataOutput();
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        int activeFeatures = 0;
        try {
            Pattern whitespacePattern = Pattern.compile("\\s+");
            Pattern labelFeaturePattern = Pattern.compile("[:=]");
            String line;
            boolean firstLine = true;
            while (true) {
                tb.reset();
                if (firstLine) {
                    long start = System.currentTimeMillis();
                    line = reader.readLine();
                    long end = System.currentTimeMillis();
                    // LOG.info("First call to reader.readLine() took " + (end -
                    // start) + " milliseconds");
                    firstLine = false;
                } else {
                    line = reader.readLine();
                }
                if (line == null) {
                    break;
                }
                String[] comps = whitespacePattern.split(line, 2);

                // Label
                // Ignore leading plus sign
                if (comps[0].charAt(0) == '+') {
                    comps[0] = comps[0].substring(1);
                }

                int label = Integer.parseInt(comps[0]);
                dos.writeInt(label);
                tb.addFieldEndOffset();
                Scanner scan = new Scanner(comps[1]);
                scan.useDelimiter(",|\\s+");
                while (scan.hasNext()) {
                    String[] parts = labelFeaturePattern.split(scan.next());
                    int index = Integer.parseInt(parts[0]);
                    if (index > numFeatures) {
                        throw new IndexOutOfBoundsException("Feature index "
                                + index
                                + " exceed the declared number of features ("
                                + numFeatures + ")");
                    }
                    // Ignore leading plus sign.
                    if (parts[1].charAt(0) == '+') {
                        parts[1] = parts[1].substring(1);
                    }
                    float value = Float.parseFloat(parts[1]);
                    dos.writeInt(index);
                    dos.writeFloat(value);
                    activeFeatures++;
                }
                dos.writeInt(-1); // Marks the end of the sparse array.
                tb.addFieldEndOffset();
                if (!appender.append(tb.getFieldEndOffsets(),
                        tb.getByteArray(), 0, tb.getSize())) {
                    FrameUtils.flushFrame(frame, writer);
                    appender.reset(frame, true);
                    if (!appender.append(tb.getFieldEndOffsets(), tb
                            .getByteArray(), 0, tb.getSize())) {
                        // LOG.severe("Example too large to fit in frame: " +
                        // line);
                        throw new IllegalStateException();
                    }
                }
            }
            if (appender.getTupleCount() > 0) {
                FrameUtils.flushFrame(frame, writer);
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        // LOG.info("Parsed input partition containing " + activeFeatures +
        // " active features");
    }

    private static class Options {
        @Option(name = "-host", usage = "Hyracks Cluster Controller Host name", required = true)
        public String host;

        @Option(name = "-port", usage = "Hyracks Cluster Controller Port (default: 1099)")
        public int port = 1099;

        @Option(name = "-app", usage = "Hyracks Application name", required = true)
        public String app;

        @Option(name = "-example-paths", usage = "Comma separated list of input paths containing training examples.", required = true)
        public String examplePaths;

        @Option(name = "-model-file", usage = "Local file to write the final weights to", required = true)
        public String modelFilename;

        @Option(name = "-temp-path", usage = "HDFS path to hold temporary files", required = true)
        public String tempPath;

        @Option(name = "-hadoop-conf", usage = "Path to Hadoop configuration", required = true)
        public String hadoopConfPath;

        @Option(name = "-cluster-conf", usage = "Path to Hyracks cluster configuration")
        public String clusterConfPath = "cluster.conf";

        @Option(name = "-num-rounds", usage = "The number of iterations to perform", required = true)
        public int numRounds;

        @Option(name = "-agg-tree-type", usage = "The aggregation tree type (none, rack, nary, or generic)", required = true)
        public String aggTreeType;

        @Option(name = "-agg-count", usage = "The number of aggregators to use, if using an aggregation tree")
        public int aggCount = -1;

        @Option(name = "-fan-in", usage = "The fan-in, if using an nary aggregation tree")
        public int fanIn = -1;

    }

    public static void main(String[] args) throws Exception {
        try {
            if (args.length == 0) {
                args = ("-host localhost"//
                        + " -app bgd"//
                        + " -port 3099"//
                        + " -hadoop-conf /data/imru/hadoop-0.20.2/conf"//
                        + " -agg-tree-type generic"//
                        + " -agg-count 1"//
                        + " -num-rounds 2"//
                        + " -temp-path /tmp"//
                        + " -model-file /tmp/__imru.txt"//
                        + " -cluster-conf imru/imru-core/src/main/resources/conf/cluster.conf"//
                        + " -example-paths /input/data.txt").split(" ");
                ImruTest.startControllers();
                ImruTest.createApp("bgd", new File(
                        "imru/imru-example/src/main/resources/bootstrap.zip"));
                ImruTest.disableLogging();
            }
            Options options = new Options();
            CmdLineParser parser = new CmdLineParser(options);
            parser.parseArgument(args);

            BGDMain2 job = new BGDMain2(8000);
            IMRUJobControl<LinearModel, LossGradient> control = new IMRUJobControl<LinearModel, LossGradient>();
            control.connect(options.host, options.port, options.hadoopConfPath,
                    options.clusterConfPath);

            // copy input files to HDFS
            FileSystem dfs = FileSystem.get(control.conf);
            if (dfs.listStatus(new Path("/tmp")) != null)
                for (FileStatus f : dfs.listStatus(new Path("/tmp")))
                    dfs.delete(f.getPath());
            dfs.copyFromLocalFile(new Path("/data/imru/test/data.txt"),
                    new Path("/input/data.txt"));

            if (options.aggTreeType.equals("none")) {
                control.selectNoAggregation(options.examplePaths);
            } else if (options.aggTreeType.equals("generic")) {
                control.selectGenericAggregation(options.examplePaths,
                        options.aggCount);
            } else if (options.aggTreeType.equals("nary")) {
                control.selectNAryAggregation(options.examplePaths,
                        options.fanIn);
            } else {
                throw new IllegalArgumentException(
                        "Invalid aggregation tree type");
            }

            LinearModel initalModel = new LinearModel(8000, options.numRounds);

            JobStatus status = control.run(job, initalModel, options.tempPath,
                    options.app);
            if (status == JobStatus.FAILURE) {
                System.err.println("Job failed; see CC and NC logs");
                System.exit(-1);
            }
            int iterationCount = control.getIterationCount();
            LinearModel finalModel = control.getModel();
            System.out.println("Terminated after " + iterationCount
                    + " iterations");
            System.out
                    .println("Final model [0] " + finalModel.weights.array[0]);
            System.out.println("Final loss was " + finalModel.loss);
            PrintWriter writer = new PrintWriter(new FileOutputStream(
                    options.modelFilename));
            for (float x : finalModel.weights.array) {
                writer.println("" + x);
            }
            writer.close();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

}
