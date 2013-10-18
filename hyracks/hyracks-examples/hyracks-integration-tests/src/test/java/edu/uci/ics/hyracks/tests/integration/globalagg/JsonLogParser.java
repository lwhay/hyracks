/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.tests.integration.globalagg;

import java.io.DataOutput;
import java.util.LinkedList;
import java.util.List;

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class JsonLogParser {

    private enum ParserState {
        INIT,
        SKIPABLE,
        INTO_RECORD,
        OUT_OF_RECORD,
        INTO_FIELD,
        OUT_OF_FIELD,
        INTO_LIST,
        OUT_OF_LIST,
        EOF
    }

    private ParserState state;

    private List<String> keyBuffer;
    private StringBuilder valueBuffer;

    private final DataOutput output;

    public JsonLogParser(DataOutput output) {
        this.output = output;
        this.state = ParserState.INIT;
        this.keyBuffer = new LinkedList<>();
        this.valueBuffer = new StringBuilder();
    }

    public void readSequence(String newSequence) throws HyracksDataException {
        for (char c : newSequence.toCharArray()) {
            switch (state) {
                case INIT:
                    if (isRecordStart(c)) {
                        state = ParserState.INTO_RECORD;
                    } else {
                        throw new HyracksDataException("Illegal state of encountering " + c + " in the INIT state.");
                    }
                    break;

            }
        }
    }

    protected boolean isRecordStart(char c) {
        return c == '{';
    }

    protected boolean isRecordEnd(char c) {
        return c == '}';
    }

    protected boolean isSkippable(char c) {
        return (state != ParserState.INTO_FIELD) && (c == ' ' || c == '\n' || c == '\t');
    }
}
