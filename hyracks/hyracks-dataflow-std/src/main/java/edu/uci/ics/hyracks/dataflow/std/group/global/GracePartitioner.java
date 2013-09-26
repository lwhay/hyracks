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
package edu.uci.ics.hyracks.dataflow.std.group.global;

import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunctionFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.partition.FieldHashPartitionComputerFactory;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileReader;
import edu.uci.ics.hyracks.dataflow.common.io.RunFileWriter;
import edu.uci.ics.hyracks.dataflow.std.group.global.base.IFrameWriterRunGenerator;

public class GracePartitioner implements IFrameWriterRunGenerator {

    private int processedTuple = 0;

    private FrameTupleAccessor inputFrameTupleAccessor;

    private final int partitions, framesLimit;

    private ByteBuffer[] bufs;

    private FrameTupleAppender outputFrameAppender;

    private final ITuplePartitionComputer tuplePartitionComputer;

    private List<RunFileReader> partitionRuns;

    private RunFileWriter[] runsForBufs;

    private final IHyracksTaskContext ctx;
    private final int frameSize;
    private final RecordDescriptor inRecordDesc;

    private final String debugID;
    private long writeFrameCount = 0, readFrameCount = 0, runFileCount = 0;

    public GracePartitioner(IHyracksTaskContext ctx, int framesLimit, int partitions, int[] keys,
            IBinaryHashFunctionFactory[] hashFunctionFactories, RecordDescriptor inRecDesc) {
        this.ctx = ctx;
        this.frameSize = ctx.getFrameSize();
        this.partitions = partitions;
        this.framesLimit = framesLimit;
        this.inRecordDesc = inRecDesc;
        this.bufs = new ByteBuffer[partitions];
        this.tuplePartitionComputer = new FieldHashPartitionComputerFactory(keys, hashFunctionFactories)
                .createPartitioner();

        this.debugID = this.getClass().getSimpleName() + "." + String.valueOf(Thread.currentThread().getId());
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#init()
     */
    @Override
    public void open() throws HyracksDataException {
        this.inputFrameTupleAccessor = new FrameTupleAccessor(frameSize, inRecordDesc);
        this.outputFrameAppender = new FrameTupleAppender(frameSize);
        this.partitionRuns = new LinkedList<RunFileReader>();
        this.runsForBufs = new RunFileWriter[bufs.length];
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#nextFrame(java.nio.ByteBuffer, int)
     */
    @Override
    public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
        inputFrameTupleAccessor.reset(buffer);
        int tupleCount = inputFrameTupleAccessor.getTupleCount();
        processedTuple = 0;
        while (processedTuple < tupleCount) {
            int h = tuplePartitionComputer.partition(inputFrameTupleAccessor, processedTuple, partitions);
            int partitionIndex = ((int) ((double) h / partitions) * framesLimit);
            if (bufs[partitionIndex] == null) {
                bufs[partitionIndex] = ctx.allocateFrame();
                this.outputFrameAppender.reset(bufs[partitionIndex], true);
            }
            this.outputFrameAppender.reset(bufs[partitionIndex], false);
            if (!outputFrameAppender.append(inputFrameTupleAccessor, processedTuple)) {
                // the output buffer for this partition is full
                if (runsForBufs[partitionIndex] == null) {
                    runsForBufs[partitionIndex] = new RunFileWriter(
                            ctx.createManagedWorkspaceFile(GracePartitioner.class.getSimpleName()), ctx.getIOManager());
                    runFileCount++;
                    runsForBufs[partitionIndex].open();
                }
                FrameUtils.flushFrame(bufs[partitionIndex], runsForBufs[partitionIndex]);
                writeFrameCount++;
                if (!outputFrameAppender.append(inputFrameTupleAccessor, processedTuple)) {
                    throw new HyracksDataException("Failed to insert a tuple into a frame");
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see edu.uci.ics.hyracks.dataflow.std.group.global.base.IPushBasedGrouper#close()
     */
    @Override
    public void close() throws HyracksDataException {
        for (int i = 0; i < runsForBufs.length; i++) {
            // flush the buffer, if there is anything
            outputFrameAppender.reset(bufs[i], false);
            if (outputFrameAppender.getTupleCount() > 0) {
                if (runsForBufs[i] == null) {
                    runsForBufs[i] = new RunFileWriter(ctx.createManagedWorkspaceFile(GracePartitioner.class
                            .getSimpleName()), ctx.getIOManager());
                    runsForBufs[i].open();
                    runFileCount++;
                }
                FrameUtils.flushFrame(bufs[i], runsForBufs[i]);
            }
            if (runsForBufs[i] != null) {
                partitionRuns.add(runsForBufs[i].createReader());
                runsForBufs[i].close();
                runsForBufs[i] = null;
            }
        }
        double partitionsPerRun = ((double) partitions) / framesLimit;
        int recursionLevel = 1;
        while (partitionsPerRun > 1) {
            int runsToPartition = partitionRuns.size();
            while (runsToPartition > 0) {
                recursivePartition(partitionRuns.remove(0), recursionLevel);
            }
            partitionsPerRun /= framesLimit;
        }

        ctx.getCounterContext().getCounter(debugID + ".readFrames", true).update(readFrameCount);
        ctx.getCounterContext().getCounter(debugID + ".writeFrames", true).update(writeFrameCount);
        ctx.getCounterContext().getCounter(debugID + ".runsCreated", true).update(runFileCount);
        ctx.getCounterContext().getCounter("costmodel.io", true).update(readFrameCount + writeFrameCount);
    }

    private void recursivePartition(RunFileReader runReader, int recursionLevel) throws HyracksDataException {
        ByteBuffer inputBuf = ctx.allocateFrame();
        runReader.open();
        while (runReader.nextFrame(inputBuf)) {
            readFrameCount++;
            inputFrameTupleAccessor.reset(inputBuf);
            int tupleCount = inputFrameTupleAccessor.getTupleCount();
            for (int i = 0; i < tupleCount; i++) {
                int h = tuplePartitionComputer.partition(inputFrameTupleAccessor, i, partitions);
                int partitionIndex = (int) (((double) h / partitions) * framesLimit * Math.pow(framesLimit,
                        recursionLevel));
                if (bufs[partitionIndex] == null) {
                    bufs[partitionIndex] = ctx.allocateFrame();
                    this.outputFrameAppender.reset(bufs[partitionIndex], true);
                }
                this.outputFrameAppender.reset(bufs[partitionIndex], false);
                if (!outputFrameAppender.append(inputFrameTupleAccessor, i)) {
                    // the output buffer for this partition is full
                    if (runsForBufs[partitionIndex] == null) {
                        runsForBufs[partitionIndex] = new RunFileWriter(
                                ctx.createManagedWorkspaceFile(GracePartitioner.class.getSimpleName()),
                                ctx.getIOManager());
                        runsForBufs[partitionIndex].open();
                        runFileCount++;
                    }
                    FrameUtils.flushFrame(bufs[partitionIndex], runsForBufs[partitionIndex]);
                    writeFrameCount++;
                    if (!outputFrameAppender.append(inputFrameTupleAccessor, i)) {
                        throw new HyracksDataException("Failed to insert a tuple into a frame");
                    }
                }
            }
        }

        for (int i = 0; i < runsForBufs.length; i++) {
            // flush the buffer, if there is anything
            outputFrameAppender.reset(bufs[i], false);
            if (outputFrameAppender.getTupleCount() > 0) {
                if (runsForBufs[i] == null) {
                    runsForBufs[i] = new RunFileWriter(ctx.createManagedWorkspaceFile(GracePartitioner.class
                            .getSimpleName()), ctx.getIOManager());
                    runsForBufs[i].open();
                    runFileCount++;
                }
                FrameUtils.flushFrame(bufs[i], runsForBufs[i]);
                writeFrameCount++;
            }
            if (runsForBufs[i] != null) {
                partitionRuns.add(runsForBufs[i].createReader());
                runsForBufs[i].close();
                runsForBufs[i] = null;
            }
        }
    }

    public void fail() throws HyracksDataException {

    }

    public List<RunFileReader> getOutputRunReaders() {
        return partitionRuns;
    }

    @Override
    public int getRunsCount() {
        return partitionRuns.size();
    }

    @Override
    public void wrapup() throws HyracksDataException {

    }

}
