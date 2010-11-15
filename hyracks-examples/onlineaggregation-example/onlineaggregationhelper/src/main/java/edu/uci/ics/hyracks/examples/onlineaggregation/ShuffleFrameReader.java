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
package edu.uci.ics.hyracks.examples.onlineaggregation;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import edu.uci.ics.hyracks.api.comm.IConnectionDemultiplexer;
import edu.uci.ics.hyracks.api.comm.IConnectionEntry;
import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.context.IHyracksContext;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.std.sort.ExternalSortRunMerger;
import edu.uci.ics.hyracks.dataflow.std.sort.RunFileReader;
import edu.uci.ics.hyracks.dataflow.std.sort.RunFileWriter;

public class ShuffleFrameReader implements IFrameReader {
    private final IHyracksContext ctx;
    private final IConnectionDemultiplexer demux;
    private final HadoopHelper helper;
    private final RecordDescriptor recordDescriptor;
    private List<File> runs;
    private Map<File, Integer> file2BlockIdMap;
    private Map<Integer, File> blockId2FileMap;
    private int lastReadSender;
    private RunFileReader reader;

    public ShuffleFrameReader(IHyracksContext ctx, IConnectionDemultiplexer demux,
            MarshalledWritable<Configuration> mConfig) throws HyracksDataException {
        this.ctx = ctx;
        this.demux = demux;
        helper = new HadoopHelper(mConfig);
        this.recordDescriptor = helper.getMapOutputRecordDescriptor();
    }

    @Override
    public void open() throws HyracksDataException {
        runs = new LinkedList<File>();
        file2BlockIdMap = new HashMap<File, Integer>();
        blockId2FileMap = new HashMap<Integer, File>();
        int nSenders = demux.getSenderCount();
        RunInfo[] infos = new RunInfo[nSenders];
        FrameTupleAccessor accessor = new FrameTupleAccessor(ctx, recordDescriptor);
        while (true) {
            IConnectionEntry entry = demux.findNextReadyEntry(lastReadSender);
            lastReadSender = (Integer) entry.getAttachment();
            ByteBuffer netBuffer = entry.getReadBuffer();
            accessor.reset(netBuffer);
            int tupleCount = accessor.getTupleCount();
            if (tupleCount == 0) {
                int openEntries = demux.closeEntry(lastReadSender);
                netBuffer.clear();
                demux.unreadyEntry(lastReadSender);
                if (openEntries == 0) {
                    break;
                }
            } else {
                RunInfo info = infos[lastReadSender];
                int nTuples = accessor.getTupleCount();
                for (int i = 0; i < nTuples; ++i) {
                    int tBlockId = IntegerSerializerDeserializer.getInt(accessor.getBuffer().array(),
                            FrameUtils.getAbsoluteFieldStartOffset(accessor, i, HadoopHelper.BLOCKID_FIELD_INDEX));
                    if (info == null) {
                        info = new RunInfo();
                        info.reset(tBlockId);
                        infos[lastReadSender] = info;
                    } else if (info.blockId != tBlockId) {
                        info.close();
                        info.reset(tBlockId);
                    }
                    info.write(accessor, i);
                }
                netBuffer.clear();
                demux.unreadyEntry(lastReadSender);
            }
        }
        for (int i = 0; i < infos.length; ++i) {
            RunInfo info = infos[i];
            if (info != null) {
                info.close();
            }
        }
        infos = null;

        File outFile;
        try {
            outFile = ctx.getResourceManager().createFile(ShuffleFrameReader.class.getName(), ".run");
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        int framesLimit = helper.getSortFrameLimit(ctx);
        ExternalSortRunMerger merger = new ExternalSortRunMerger(ctx, null, runs, new int[] { 0 },
                helper.getSortComparatorFactories(), recordDescriptor, framesLimit, new RunFileWriter(outFile));
        merger.process();

        try {
            reader = new RunFileReader(outFile);
        } catch (FileNotFoundException e) {
            throw new HyracksDataException(e);
        }
        reader.open();
    }

    @Override
    public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
        return reader.nextFrame(buffer);
    }

    @Override
    public void close() throws HyracksDataException {
        reader.close();
    }

    private class RunInfo {
        private final ByteBuffer buffer;
        private final FrameTupleAppender fta;

        private File file;
        private RandomAccessFile raf;
        private FileChannel channel;
        private int blockId;

        public RunInfo() {
            buffer = ctx.getResourceManager().allocateFrame();
            fta = new FrameTupleAppender(ctx);
        }

        public void reset(int blockId) throws HyracksDataException {
            this.blockId = blockId;
            fta.reset(buffer, true);
            try {
                file = ctx.getResourceManager().createFile(ShuffleFrameReader.class.getName(), ".run");
                raf = new RandomAccessFile(file, "rw");
                channel = raf.getChannel();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
        }

        public void write(FrameTupleAccessor accessor, int tIdx) throws HyracksDataException {
            if (!fta.append(accessor, tIdx)) {
                flush();
                if (!fta.append(accessor, tIdx)) {
                    throw new IllegalStateException();
                }
            }
        }

        public void close() throws HyracksDataException {
            flush();
            try {
                channel.close();
                raf.close();
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
            runs.add(file);
            file2BlockIdMap.put(file, blockId);
            blockId2FileMap.put(blockId, file);
        }

        private void flush() throws HyracksDataException {
            if (fta.getTupleCount() <= 0) {
                return;
            }
            buffer.limit(buffer.capacity());
            buffer.position(0);
            try {
                int remaining = buffer.remaining();
                while (remaining > 0) {
                    int wLen = channel.write(buffer);
                    remaining -= wLen;
                }
            } catch (IOException e) {
                throw new HyracksDataException(e);
            }
            fta.reset(buffer, true);
        }
    }
}