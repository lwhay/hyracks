package edu.uci.ics.hyracks.dataflow.std.group.global;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.group.AggregateState;
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptor;

public class MergeGrouper {

    private final IHyracksTaskContext ctx;

    private final int[] keyFields, decorFields;
    private final RecordDescriptor inRecDesc, outRecDesc;

    private final IBinaryComparator[] comparators;

    private final int framesLimit;

    private final IAggregatorDescriptor merger;
    private AggregateState mergeState;

    LinkedList<RunFileReader> runs;

    List<ByteBuffer> inFrames;
    ByteBuffer outFrame, writerFrame;
    FrameTupleAppender outFrameAppender, writerAppender;
    ArrayTupleBuilder flushTupleBuilder;
    FrameTupleAccessor outFrameAccessor;
    int[] currentFrameIndexInRun, currentRunFrames, currentBucketInRun;
    int runFrameLimit = 1;

    public MergeGrouper(IHyracksTaskContext ctx, int[] keyFields, int[] decorFields, int framesLimit,
            IBinaryComparator[] comparators, IAggregatorDescriptor merger, RecordDescriptor inRecDesc,
            RecordDescriptor outRecDesc) throws HyracksDataException {
        this.ctx = ctx;
        this.keyFields = keyFields;
        this.decorFields = decorFields;
        this.framesLimit = framesLimit;
        this.comparators = comparators;
        this.merger = merger;
        this.inRecDesc = inRecDesc;
        this.outRecDesc = outRecDesc;

        this.mergeState = merger.createAggregateStates();
    }

    public void process(LinkedList<RunFileReader> runFiles, IFrameWriter writer) throws HyracksDataException {
        runs = runFiles;

        writer.open();
        try {
            if (runs.size() > 0) {
                inFrames = new ArrayList<ByteBuffer>();
                outFrame = ctx.allocateFrame();
                outFrameAppender = new FrameTupleAppender(ctx.getFrameSize());
                outFrameAppender.reset(outFrame, true);
                for (int i = 0; i < framesLimit - 1; ++i) {
                    inFrames.add(ctx.allocateFrame());
                }
                int maxMergeWidth = framesLimit - 1;
                while (runs.size() > maxMergeWidth) {
                    int generationSeparator = 0;
                    while (generationSeparator < runs.size() && runs.size() > maxMergeWidth) {
                        int mergeWidth = Math.min(Math.min(runs.size() - generationSeparator, maxMergeWidth),
                                runs.size() - maxMergeWidth + 1);
                        FileReference newRun = ctx.createManagedWorkspaceFile(MergeGrouper.class.getSimpleName());
                        IFrameWriter mergeResultWriter = new RunFileWriter(newRun, ctx.getIOManager());
                        mergeResultWriter.open();
                        IFrameReader[] runCursors = new RunFileReader[mergeWidth];
                        for (int i = 0; i < mergeWidth; i++) {
                            runCursors[i] = runs.get(generationSeparator + i);
                        }
                        merge(mergeResultWriter, runCursors, false);
                        runs.subList(generationSeparator, mergeWidth + generationSeparator).clear();
                        runs.add(generationSeparator++, ((RunFileWriter) mergeResultWriter).createReader());
                    }
                }
                if (!runs.isEmpty()) {
                    IFrameReader[] runCursors = new RunFileReader[runs.size()];
                    for (int i = 0; i < runCursors.length; i++) {
                        runCursors[i] = runs.get(i);
                    }
                    merge(writer, runCursors, true);
                }
            }
        } catch (Exception e) {
            writer.fail();
            throw new HyracksDataException(e);
        } finally {
            writer.close();
        }
    }

    protected void merge(IFrameWriter mergeResultWriter, IFrameReader[] runCursors, boolean isFinalRound)
            throws HyracksDataException {
        RumMergingGroupingFrameReader mergeFrameReader = new RumMergingGroupingFrameReader(ctx, runCursors, inFrames,
                keyFields, decorFields, comparators, merger, mergeState, inRecDesc, outRecDesc);
        mergeFrameReader.open();
        try {
            while (mergeFrameReader.nextFrame(outFrame)) {
                flushOutFrame(mergeResultWriter, isFinalRound);
            }
        } finally {
            mergeFrameReader.close();
        }
    }

    private void flushOutFrame(IFrameWriter writer, boolean isFinal) throws HyracksDataException {

        if (flushTupleBuilder == null) {
            flushTupleBuilder = new ArrayTupleBuilder(outRecDesc.getFields().length);
        }

        if (writerFrame == null) {
            writerFrame = ctx.allocateFrame();
        }

        if (writerAppender == null) {
            writerAppender = new FrameTupleAppender(ctx.getFrameSize());
        }
        writerAppender.reset(writerFrame, true);

        outFrameAccessor.reset(outFrame);

        for (int i = 0; i < outFrameAccessor.getTupleCount(); i++) {

            flushTupleBuilder.reset();

            for (int k = 0; k < keyFields.length + decorFields.length; k++) {
                flushTupleBuilder.addField(outFrameAccessor, i, k);
            }

            if (isFinal) {

                merger.outputFinalResult(flushTupleBuilder, outFrameAccessor, i, mergeState);

            } else {

                merger.outputPartialResult(flushTupleBuilder, outFrameAccessor, i, mergeState);
            }

            if (!writerAppender.append(flushTupleBuilder.getFieldEndOffsets(), flushTupleBuilder.getByteArray(), 0,
                    flushTupleBuilder.getSize())) {
                FrameUtils.flushFrame(writerFrame, writer);
                writerAppender.reset(writerFrame, true);
                if (!writerAppender.append(flushTupleBuilder.getFieldEndOffsets(), flushTupleBuilder.getByteArray(), 0,
                        flushTupleBuilder.getSize())) {
                    throw new HyracksDataException("Aggregation output is too large to be fit into a frame.");
                }
            }
        }
        if (writerAppender.getTupleCount() > 0) {
            FrameUtils.flushFrame(writerFrame, writer);
            writerAppender.reset(writerFrame, true);
        }
    }

}
