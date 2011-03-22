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
package edu.uci.ics.hyracks.dataflow.std.aggregators;

import java.io.DataOutput;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;

public interface IAggregatorDescriptor {

    /**
     * Initialize the aggregator for the group indicated by the input tuple. Keys,
     * and the aggregation values will be written out onto the frame wrapped in the appender.
     * 
     * @param accessor
     *            The frame containing the input tuple.
     * @param tIndex
     *            The tuple index in the frame.
     * @param appender
     *            The output frame.
     * @return
     * @throws HyracksDataException
     */
    public boolean init(IFrameTupleAccessor accessor, int tIndex, FrameTupleAppender appender)
            throws HyracksDataException;

    /**
     * Get the initial aggregation value from the input tuple.
     * Compared with {@link #init(IFrameTupleAccessor, int, FrameTupleAppender)}, instead of
     * writing the value to a frame appender, the value will be written through the given {@link DataOutput}.
     * 
     * @param accessor
     *            The frame containing the input tuple.
     * @param tIndex
     *            The tuple index in the frame.
     * @param dataOutput
     *            The data output wrapper.
     * @throws HyracksDataException
     */
    public void getInitValue(IFrameTupleAccessor accessor, int tIndex, DataOutput dataOutput)
            throws HyracksDataException;

    /**
     * Aggregate the input tuple with the given partial aggregation value. The partial aggregation value will
     * be updated after the aggregation.
     * 
     * @param accessor1
     *            The frame containing the input tuple to be aggregated.
     * @param tIndex1
     *            The tuple index in the frame.
     * @param accessor2
     *            The frame containing the partial aggregation value.
     * @param tIndex2
     *            The tuple index of the partial aggregation value. Note that finally this value will be
     *            updated to be the new aggregation value.
     * @throws HyracksDataException
     */
    public void aggregate(IFrameTupleAccessor accessor1, int tIndex1, IFrameTupleAccessor accessor2, int tIndex2)
            throws HyracksDataException;

    /**
     * Merge the partial aggregation value with another partial aggregation value. After merging, the aggregation
     * value of the second partial aggregator will be updated.
     * 
     * @param accessor1
     *            The frame containing the tuple of partial aggregation value to be aggregated.
     * @param tIndex1
     *            The index of the tuple of partial aggregation value to be aggregated.
     * @param accessor2
     *            The frame containing the tuple of partial aggregation value to be updated.
     * @param tIndex2
     *            The index of the tuple of partial aggregation value to be updated. Note that after merging,
     *            this value will be updated to be the new aggregation value.
     */
    public void merge(IFrameTupleAccessor accessor1, int tIndex1, IFrameTupleAccessor accessor2, int tIndex2)
            throws HyracksDataException;

    /**
     * Output the partial aggregation result into a frame appender. The outputted result can be used to
     * aggregate a new tuple from the input frame.
     * This method, together with the {@link #outputPartialResult(IFrameTupleAccessor, int, FrameTupleAppender)},
     * is for the case when different processing logics are applied to partial aggregation result and the merged
     * aggregation result.
     * For example, in an aggregator for variable-length aggregation results, aggregation values should be maintained
     * inside of the aggregators, instead of the frames. A reference will be used to indicate the aggregation value
     * in the partial aggregation result, while the actual aggregation value will be used in the merged aggregation
     * result.
     * 
     * @param accessor
     * @param tIndex
     * @param appender
     * @return
     * @throws HyracksDataException
     */
    public boolean outputPartialResult(IFrameTupleAccessor accessor, int tIndex, FrameTupleAppender appender)
            throws HyracksDataException;

    /**
     * Output the merged aggregation result into a frame appender. The outputted result can be used to
     * merge another partial aggregation result.
     * See {@link #outputPartialResult(IFrameTupleAccessor, int, FrameTupleAppender)} for the difference
     * between these two methods.
     * 
     * @param accessor
     * @param tIndex
     * @param appender
     * @return
     * @throws HyracksDataException
     */
    public boolean outputMergeResult(IFrameTupleAccessor accessor, int tIndex, FrameTupleAppender appender)
            throws HyracksDataException;

    /**
     * Get the partial aggregation value from the merged aggregator indicated by the frame and the tuple index.
     * Compared with {@link #outputPartialResult(IFrameTupleAccessor, int, FrameTupleAppender)}, this will output the value
     * through the given {@link DataOutput}.
     * 
     * @param accessor
     * @param tIndex
     * @param dataOutput
     * @throws HyracksDataException
     */
    public void getPartialOutputValue(IFrameTupleAccessor accessor, int tIndex, DataOutput dataOutput)
            throws HyracksDataException;
    
    /**
     * Get the merged aggregation value from the merged aggregator indicated by the frame and the tuple index.
     * Compared with {@link #outputMergeResult(IFrameTupleAccessor, int, FrameTupleAppender)}, this will output the value
     * through the given {@link DataOutput}.
     * 
     * @param accessor
     * @param tIndex
     * @param dataOutput
     * @throws HyracksDataException
     */
    public void getMergeOutputValue(IFrameTupleAccessor accessor, int tIndex, DataOutput dataOutput)
            throws HyracksDataException;

    /**
     * Close the aggregator. Necessary clean-up code should be implemented here.
     */
    public void close();

}
