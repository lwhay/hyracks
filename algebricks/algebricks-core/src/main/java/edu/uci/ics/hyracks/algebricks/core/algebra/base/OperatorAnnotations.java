/*
 * Copyright 2009-2013 by The Regents of the University of California
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
package edu.uci.ics.hyracks.algebricks.core.algebra.base;

public interface OperatorAnnotations {
    // hints for group by
    public static final String USE_HASH_SORT_GROUP_BY = "USE_HASH_SORT_GROUP_BY"; // -->
    public static final String USE_HYBRID_HASH_GROUP_BY = "USE_HYBRID_HASH_GROUP_BY";

    // Boolean
    public static final String CARDINALITY = "CARDINALITY"; // -->
    // Integer
    public static final String MAX_NUMBER_FRAMES = "MAX_NUMBER_FRAMES"; // -->
    // Integer
    
    // for hybrid-hash-gby
    public static final String INPUT_RECORD_COUNT = "INPUT_RECORD_COUNT";
    public static final String INPUT_RECORD_SIZE_IN_BYTES = "INPUT_RECORD_SIZE_IN_BYTES";
    public static final String HASHTABLE_SLOT_COUNT = "HASHTABLE_SLOT_COUNT";
}
