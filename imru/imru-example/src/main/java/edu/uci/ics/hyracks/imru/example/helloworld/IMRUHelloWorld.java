package edu.uci.ics.hyracks.imru.example.helloworld;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.job.JobStatus;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.ByteBufferInputStream;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.imru.api2.IMRUJob;
import edu.uci.ics.hyracks.imru.api2.IMRUJobTmp;
import edu.uci.ics.hyracks.imru.api2.IMRUJobControl;
import edu.uci.ics.hyracks.imru.example.bgd.R;
import edu.uci.ics.hyracks.imru.test.ImruTest;

public class IMRUHelloWorld extends
        IMRUJob<HelloWorldModel, HelloWorldIncrementalResult> {
    static AtomicInteger nextId = new AtomicInteger();
    int id;

    public IMRUHelloWorld() {
        this.id = nextId.getAndIncrement();
    }

    @Override
    public HelloWorldModel initModel() {
        return new HelloWorldModel();
    }

    @Override
    public int getCachedDataFrameSize() {
        return 256;
    }

    /**
     * Parse input data and create frames
     */
    @Override
    public void parse(IHyracksTaskContext ctx, InputStream in,
            IFrameWriter writer) throws HyracksDataException {
        ByteBuffer frame = ctx.allocateFrame();
        FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
        appender.reset(frame, true);
        ArrayTupleBuilder tb = new ArrayTupleBuilder(2);
        DataOutput dos = tb.getDataOutput();
        try {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(in));
            String line = reader.readLine();
            reader.close();
            for (String s : line.split(" ")) {
                // create a new frame
                appender.reset(frame, true);

                tb.reset();
                // add one field
                dos.writeUTF(s);
                tb.addFieldEndOffset();
                // add another field
                dos.writeUTF(s + "_copy");
                tb.addFieldEndOffset();
                if (!appender.append(tb.getFieldEndOffsets(),
                        tb.getByteArray(), 0, tb.getSize())) {
                    // if frame can't hold this tuple
                    throw new IllegalStateException(
                            "Example too large to fit in frame: " + line);
                }
                FrameUtils.flushFrame(frame, writer);
            }
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
    }

    @Override
    public HelloWorldIncrementalResult map(Iterator<ByteBuffer> input,
            HelloWorldModel model, int cachedDataFrameSize)
            throws HyracksDataException {
        HelloWorldIncrementalResult mapResult;
        IFrameTupleAccessor accessor;
        R.p("openMap" + id+" model="+ model.totalLength);
        mapResult = new HelloWorldIncrementalResult();
        accessor = new FrameTupleAccessor(cachedDataFrameSize,
                new RecordDescriptor(new ISerializerDeserializer[2]));
        while (input.hasNext()) {
            ByteBuffer buf = input.next();
            try {
                accessor.reset(buf);
                int tupleCount = accessor.getTupleCount();
                ByteBufferInputStream bbis = new ByteBufferInputStream();
                DataInputStream di = new DataInputStream(bbis);
                for (int i = 0; i < tupleCount; i++) {
                    int fieldId = 0;
                    int startOffset = accessor.getFieldSlotsLength()
                            + accessor.getTupleStartOffset(i)
                            + accessor.getFieldStartOffset(i, fieldId);
                    bbis.setByteBuffer(accessor.getBuffer(), startOffset);
                    String word = di.readUTF();
                    R.p("map%d read frame: %s", id, word);
                    mapResult.length += word.length();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        R.p("closeMap" + id + " output=" + mapResult.length);
        return mapResult;
    }

    @Override
    public HelloWorldIncrementalResult reduce(
            Iterator<HelloWorldIncrementalResult> input)
            throws HyracksDataException {
        HelloWorldIncrementalResult reduceResult;
        R.p("openReduce" + id);
        reduceResult = new HelloWorldIncrementalResult();
        while (input.hasNext()) {
            HelloWorldIncrementalResult result = input.next();
            R.p("reduce" + id + " input=" + result.length);
            reduceResult.length += result.length;
        }
        R.p("closeReduce" + id + " output=" + reduceResult.length);
        return reduceResult;
    }

    @Override
    public void update(Iterator<HelloWorldIncrementalResult> input,
            HelloWorldModel model) throws HyracksDataException {
        R.p("openUpdate" + id + " input=" + model.totalLength);
        while (input.hasNext()) {
            HelloWorldIncrementalResult result = input.next();
            R.p("update" + id);
            model.totalLength += result.length;
        }
        R.p("closeUpdate" + id + " output=" + model.totalLength);
        model.roundsRemaining--;
    }

    /**
     * Return true to exit loop
     */
    @Override
    public boolean shouldTerminate(HelloWorldModel model) {
        return model.roundsRemaining == 0;
    }

    public static void main(String[] args) throws Exception {
        try {
            boolean debugging = true; // start everything in one process
            ImruTest.disableLogging();
            String host = "localhost";
            int port = 3099;
            String app = "imru_helloworld";

            // hadoop 0.20.2 need to be started
            String hadoopConfPath = "/data/imru/hadoop-0.20.2/conf";

            // directory in hadoop HDFS which contains intermediate models
            String tempPath = "/helloworld";

            // config files which contains node names
            String clusterConfPath = "imru/imru-core/src/main/resources/conf/cluster.conf";

            // directory in hadoop HDFS which contains input data
            String examplePaths = "/helloworld/input.txt";
            if (debugging) {
                ImruTest.startControllers();
                // Minimum config file to invoke
                // edu.uci.ics.hyracks.imru.runtime.bootstrap.IMRUNCBootstrapImpl
                ImruTest.createApp(app, new File(
                        "imru/imru-example/src/main/resources/bootstrap.zip"));
            }
            IMRUHelloWorld job = new IMRUHelloWorld();
            IMRUJobControl<HelloWorldModel, HelloWorldIncrementalResult> control = new IMRUJobControl<HelloWorldModel, HelloWorldIncrementalResult>();
            control.connect(host, port, hadoopConfPath, clusterConfPath);

            // remove old intermediate models
            FileSystem dfs = FileSystem.get(control.conf);
            if (dfs.listStatus(new Path(tempPath)) != null)
                for (FileStatus f : dfs.listStatus(new Path(tempPath)))
                    dfs.delete(f.getPath());

            // create input file
            FSDataOutputStream out = dfs.create(new Path(examplePaths), true);
            out.write("hello world".getBytes());
            out.close();

            // set aggregation type
            // control.selectNAryAggregation(examplePaths, 2);
            control.selectGenericAggregation(examplePaths, 1);

            JobStatus status = control.run(job, tempPath, app);
            if (status == JobStatus.FAILURE) {
                System.err.println("Job failed; see CC and NC logs");
                System.exit(-1);
            }
            int iterationCount = control.getIterationCount();
            HelloWorldModel finalModel = control.getModel();
            System.out.println("Terminated after " + iterationCount
                    + " iterations");
            R.p("FinalModel: " + finalModel.totalLength);
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.exit(0);
    }

}
