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

package edu.uci.ics.hyracks.imru.api2;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public interface IMRUJob2<Model> extends Serializable {
    public Model initModel();

    public int getCachedDataFrameSize();

    public void parse(IHyracksTaskContext ctx, InputStream in, IFrameWriter writer) throws IOException;

    public void map(IHyracksTaskContext ctx, Iterator<ByteBuffer> input, Model model, OutputStream output,
            int cachedDataFrameSize) throws HyracksDataException;

    public void reduce(IHyracksTaskContext ctx, Iterator<byte[]> input, OutputStream output)
            throws HyracksDataException;

    public void update(IHyracksTaskContext ctx, Iterator<byte[]> input, Model model) throws HyracksDataException;

    public boolean shouldTerminate(Model model);
}
