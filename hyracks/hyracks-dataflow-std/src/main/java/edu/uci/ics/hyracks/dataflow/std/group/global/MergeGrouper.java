package edu.uci.ics.hyracks.dataflow.std.group.global;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
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
import edu.uci.ics.hyracks.dataflow.std.group.IAggregatorDescriptorFactory;

public class MergeGrouper {

    private final IHyracksTaskContext ctx;

    private final int[] keyFields, decorFields;
    private final RecordDescriptor inRecDesc, outRecDesc;

    private final IBinaryComparator[] comparators;

    private final int framesLimit;

    private final IAggregatorDescriptor partialMerger, finalMerger;
    private AggregateState mergeState;

    List<RunFileReader> runs;

    List<ByteBuffer> inFrames;
    ByteBuffer outFrame, writerFrame;
    FrameTupleAppender outFrameAppender, writerAppender;
    ArrayTupleBuilder flushTupleBuilder;
    FrameTupleAccessor outFrameAccessor;
    int[] currentFrameIndexInRun, currentRunFrames, currentBucketInRun;
    int runFrameLimit = 1;

    public MergeGrouper(IHyracksTaskContext ctx, int[] keyFields, int[] decorFields, int framesLimit,
            IBinaryComparatorFactory[] comparatorFactories, IAggregatorDescriptorFactory partialMergerFactory,
            IAggregatorDescriptorFactory finalMergerFactory, RecordDescriptor inRecDesc, RecordDescriptor outRecDesc)
            throws HyracksDataException {
        this.ctx = ctx;
        this.keyFields = keyFields;
        this.decorFields = decorFields;
        this.framesLimit = framesLimit;
        this.comparators = new IBinaryComparator[comparatorFactories.length];
        for (int i = 0; i < this.comparators.length; i++) {
            this.comparators[i] = comparatorFactories[i].createBinaryComparator();
        }
        this.partialMerger = partialMergerFactory.createAggregator(ctx, inRecDesc, outRecDesc, keyFields, keyFields);
        this.finalMerger = finalMergerFactory.createAggregator(ctx, outRecDesc, outRecDesc, keyFields, keyFields);
        this.inRecDesc = inRecDesc;
        this.outRecDesc = outRecDesc;

        this.mergeState = partialMerger.createAggregateStates();
    }

    public void process(List<RunFileReader> runFiles, IFrameWriter writer) throws HyracksDataException {
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
                        merge(mergeResultWriter, runCursors, partialMerger);
                        runs.subList(generationSeparator, mergeWidth + generationSeparator).clear();
                        runs.add(generationSeparator++, ((RunFileWriter) mergeResultWriter).createReader());
                    }
                }
                if (!runs.isEmpty()) {
                    IFrameReader[] runCursors = new RunFileReader[runs.size()];
                    for (int i = 0; i < runCursors.length; i++) {
                        runCursors[i] = runs.get(i);
                    }
                    merge(writer, runCursors, finalMerger);
                }
            }
        } catch (Exception e) {
            writer.fail();
            throw new HyracksDataException(e);
        } finally {
            writer.close();
        }
    }

    protected void merge(IFrameWriter mergeResultWriter, IFrameReader[] runCursors, IAggregatorDescriptor merger)
            throws HyracksDataException {
        RumMergingGroupingFrameReader mergeFrameReader = new RumMergingGroupingFrameReader(ctx, runCursors, inFrames,
                keyFields, decorFields, comparators, merger, mergeState, inRecDesc, outRecDesc);
        mergeFrameReader.open();
        try {
            while (mergeFrameReader.nextFrame(outFrame)) {
                flushOutFrame(mergeResultWriter, merger);
            }
        } finally {
            if (writerAppender.getTupleCount() > 0) {
                FrameUtils.flushFrame(writerFrame, mergeResultWriter);
                writerAppender.reset(writerFrame, true);
            }
            mergeFrameReader.close();
        }
    }

    private void flushOutFrame(IFrameWriter writer, IAggregatorDescriptor merger) throws HyracksDataException {

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

        if (outFrameAccessor == null) {
            outFrameAccessor = new FrameTupleAccessor(ctx.getFrameSize(), outRecDesc);
        }

        outFrameAccessor.reset(outFrame);

        for (int i = 0; i < outFrameAccessor.getTupleCount(); i++) {

            flushTupleBuilder.reset();

            for (int k = 0; k < keyFields.length + decorFields.length; k++) {
                flushTupleBuilder.addField(outFrameAccessor, i, k);
            }

            merger.outputFinalResult(flushTupleBuilder, outFrameAccessor, i, mergeState);

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
    }

}
