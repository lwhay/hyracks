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

package edu.uci.ics.hyracks.imru.api;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;


/**
 * Low level IMRU job interface. Data passed through
 * is raw binary data.
 * 
 * @author Rui Wang
 * @param <Model>
 */
public interface IIMRUJob2<Model> extends Serializable {
    /**
     * Frame size must be large enough to store at least one tuple
     */
    public int getCachedDataFrameSize();

    /**
     * Parse input data and output binary data
     */
    public void parse(IMRUContext ctx, InputStream in, FrameWriter writer) throws IOException;

    /**
     * For a list of binary data, return one binary data
     */
    public void map(IMRUContext ctx, Iterator<ByteBuffer> input, Model model, OutputStream output,
            int cachedDataFrameSize) throws IMRUDataException;

    /**
     * Combine multiple raw data to one binary data
     */
    public void reduce(IMRUReduceContext ctx, Iterator<byte[]> input, OutputStream output) throws IMRUDataException;

    /**
     * update the model using combined binary data.
     * Return the same model object or return another object.
     */
    public Model update(IMRUContext ctx, Iterator<byte[]> input, Model model) throws IMRUDataException;

    /**
     * Return true to exit loop
     */
    public boolean shouldTerminate(Model model);
}
