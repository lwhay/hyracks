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
import java.io.Serializable;
import java.util.Iterator;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.control.nc.application.NCApplicationContext;
import edu.uci.ics.hyracks.imru.example.utils.R;

public abstract class IMRUJobV3<Model extends Serializable, Data extends Serializable, IntermediateResult extends Serializable>
        implements IMRUJobV2<Model, IntermediateResult> {
    @Override
    public int getFieldCount() {
        return 1;
    }

    @Override
    final public void parse(IHyracksTaskContext ctx, InputStream input, TupleWriter output) throws IOException {
        parse(ctx, input, new DataWriter<Data>(output));
    }

    public abstract void parse(IHyracksTaskContext ctx, InputStream input, DataWriter<Data> output) throws IOException;

    @Override
    final public IntermediateResult map(final IHyracksTaskContext ctx, final TupleReader input, Model model,
            int cachedDataFrameSize) throws IOException {
        Iterator<Data> dataInterator = new Iterator<Data>() {
            boolean first = true;

            @Override
            public boolean hasNext() {
                if (first)
                    return true;
                return input.hasNextTuple();
            }

            public Data read() throws Exception {
                int length = input.readInt();
                byte[] bs = new byte[length];
                int len = input.read(bs);
                if (len != length)
                    R.p("read half");
                NCApplicationContext appContext = (NCApplicationContext) ctx.getJobletContext().getApplicationContext();
                return (Data) appContext.deserialize(bs);
            }

            @Override
            public Data next() {
                if (!hasNext())
                    return null;
                try {
                    if (first) {
                        first = false;
                        return read();
                    } else {
                        input.nextTuple();
                        return read();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public void remove() {
            }
        };
        return map(ctx, dataInterator, model);
    }

    public abstract IntermediateResult map(IHyracksTaskContext ctx, Iterator<Data> input, Model model)
            throws IOException;

    @Override
    public IntermediateResult reduce(IHyracksTaskContext ctx, Iterator<IntermediateResult> input)
            throws HyracksDataException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean shouldTerminate(Model model) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void update(IHyracksTaskContext ctx, Iterator<IntermediateResult> input, Model model)
            throws HyracksDataException {
        // TODO Auto-generated method stub

    }

}
