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
package edu.uci.ics.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;

import edu.uci.ics.hyracks.api.comm.IFrameReader;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.std.sort.PredictingFrameReaderCollection;

public class PredictingFrameReader implements IFrameReader {
    private final IFrameReader fileReader;
    private final int index;
    private final PredictingFrameReaderCollection predictingFrameReaderCollection;

    public PredictingFrameReader(IFrameReader fileReader, int index,
            PredictingFrameReaderCollection predictingFrameReaderCollection) {
        this.fileReader = fileReader;
        this.index = index;
        this.predictingFrameReaderCollection = predictingFrameReaderCollection;
    }

    @Override
    public void open() throws HyracksDataException {
        fileReader.open();
    }

    @Override
    public boolean nextFrame(ByteBuffer buffer) throws HyracksDataException {
        buffer.clear();
        predictingFrameReaderCollection.getFrame(buffer, index);
        return true;
    }

    @Override
    public void close() throws HyracksDataException {
        fileReader.close();
    }
}
