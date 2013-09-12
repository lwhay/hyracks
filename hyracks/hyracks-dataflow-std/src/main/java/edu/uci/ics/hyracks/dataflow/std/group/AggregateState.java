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
package edu.uci.ics.hyracks.dataflow.std.group;

import java.io.Serializable;

/**
 *
 */
public class AggregateState implements Serializable {

    private static final long serialVersionUID = 1L;

    public Object state = null;

    public AggregateState() {
        state = null;
    }

    public AggregateState(Object obj) {
        state = obj;
    }

    public void reset() {
        if (state == null) {
            return;
        }
        if (state instanceof AggregateState) {
            ((AggregateState) state).reset();
            return;
        }
        if (state.getClass().isArray()) {
            for (Object s : (Object[]) state) {
                if (s instanceof AggregateState) {
                    ((AggregateState) s).reset();
                } else {
                    s = null;
                }
            }
            return;
        }
        state = null;
    }

    public void close() {
        state = null;
    }

}
