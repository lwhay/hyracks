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

package edu.uci.ics.hyracks.imru.deserialized;

import java.io.Serializable;

import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;
import edu.uci.ics.hyracks.imru.api.IModel;

interface IDeserializingIMRUJobSpecification<T extends Serializable, Model extends IModel> {
    public int getCachedDataFrameSize();
    public ITupleParserFactory getTupleParserFactory();
    public IDeserializedMapFunctionFactory<T, Model> getDeserializedMapFunctionFactory();
    public IDeserializedReduceFunctionFactory<T> getDeserializedReduceFunctionFactory();
    public IDeserializedUpdateFunctionFactory<T, Model> getDeserializedUpdateFunctionFactory();
}
