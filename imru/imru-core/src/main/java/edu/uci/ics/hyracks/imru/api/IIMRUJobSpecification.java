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

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Iterator;

import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

public interface IIMRUJobSpecification<Model extends IModel> extends Serializable {
    /* Data Loading */
    /**
     * Returns the size of cached data frames.
     * <p>
     * Cached data frames may be larger than the frames sent over the
     * network in order to accommodate large, indivisible input
     * records.
     *
     * @return The size, in bytes, of cached input frames.
     */
    int getCachedDataFrameSize();

    /**
     * @return A tuple parser factory for parsing the input records.
     */
    ITupleParserFactory getTupleParserFactory();

    /* UDFs */
    IMapFunctionFactory<Model> getMapFunctionFactory();

    IReduceFunctionFactory getReduceFunctionFactory();

    IUpdateFunctionFactory<Model> getUpdateFunctionFactory();

    boolean shouldTerminate(Model model);

    /* Optimization */
    /* AggregationStrategy getAggregationStrategy */
}
