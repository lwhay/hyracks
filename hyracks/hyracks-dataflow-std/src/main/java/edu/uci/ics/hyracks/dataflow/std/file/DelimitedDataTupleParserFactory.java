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
package edu.uci.ics.hyracks.dataflow.std.file;

import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.util.Arrays;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParser;
import edu.uci.ics.hyracks.dataflow.common.data.parsers.IValueParserFactory;

public class DelimitedDataTupleParserFactory implements ITupleParserFactory {
    private static final long serialVersionUID = 1L;
    private IValueParserFactory[] valueParserFactories;
    private char fieldDelimiter;
    private char quote;

    public DelimitedDataTupleParserFactory(IValueParserFactory[] fieldParserFactories, char fieldDelimiter) {
        this.valueParserFactories = fieldParserFactories;
        this.fieldDelimiter = fieldDelimiter;
        this.quote = '\u0000';
    }

    public DelimitedDataTupleParserFactory(IValueParserFactory[] fieldParserFactories, char fieldDelimiter, char quote) {
        this.valueParserFactories = fieldParserFactories;
        this.fieldDelimiter = fieldDelimiter;
        this.quote = quote;
    }

    @Override
    public ITupleParser createTupleParser(final IHyracksTaskContext ctx) {
        return new ITupleParser() {
            @SuppressWarnings("null")
            @Override
            public void parse(InputStream in, IFrameWriter writer) throws HyracksDataException {
                try {
                    IValueParser[] valueParsers = new IValueParser[valueParserFactories.length];
                    for (int i = 0; i < valueParserFactories.length; ++i) {
                        valueParsers[i] = valueParserFactories[i].createValueParser();
                    }
                    ByteBuffer frame = ctx.allocateFrame();
                    FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
                    appender.reset(frame, true);
                    ArrayTupleBuilder tb = new ArrayTupleBuilder(valueParsers.length);
                    DataOutput dos = tb.getDataOutput();

                    FieldCursor cursor = new FieldCursor(new InputStreamReader(in));
                    while (cursor.nextRecord()) {
                        tb.reset();
                        for (int i = 0; i < valueParsers.length; ++i) {
                            if (!cursor.nextField()) {
                                break;
                            }
                            valueParsers[i].parse(cursor.buffer, cursor.fStart, cursor.fEnd - cursor.fStart, dos);
                            tb.addFieldEndOffset();
                        }
                        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                            FrameUtils.flushFrame(frame, writer);
                            appender.reset(frame, true);
                            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                throw new HyracksDataException("Record size (" + tb.getSize() + ") larger than frame size (" + appender.getBuffer().capacity() + ")");
                            }
                        }
                    }
                    if (appender.getTupleCount() > 0) {
                        FrameUtils.flushFrame(frame, writer);
                    }
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
            }

        };
    }

    private enum State {
        INIT, IN_RECORD, EOR, CR, EOF
    }

    private class FieldCursor {
        private static final int INITIAL_BUFFER_SIZE = 4096;
        private static final int INCREMENT = 4096;

        private final Reader in;

        private char[] buffer;
        private int start;
        private int end;
        private State state;

        private int fStart;
        private int fEnd;

        public FieldCursor(Reader in) {
            this.in = in;
            buffer = new char[INITIAL_BUFFER_SIZE];
            start = 0;
            end = 0;
            state = State.INIT;
        }

        public boolean nextRecord() throws IOException {
            while (true) {
                switch (state) {
                    case INIT:
                        boolean eof = !readMore();
                        if (eof) {
                            state = State.EOF;
                            return false;
                        } else {
                            state = State.IN_RECORD;
                            return true;
                        }

                    case IN_RECORD:
                        int p = start;
                        while (true) {
                            if (p >= end) {
                                int s = start;
                                eof = !readMore();
                                if (eof) {
                                    state = State.EOF;
                                    return start < end;
                                }
                                p -= (s - start);
                            }
                            char ch = buffer[p];
                            if (ch == '\n') {
                                start = p + 1;
                                state = State.EOR;
                                break;
                            } else if (ch == '\r') {
                                start = p + 1;
                                state = State.CR;
                                break;
                            }
                            ++p;
                        }
                        break;

                    case CR:
                        if (start >= end) {
                            eof = !readMore();
                            if (eof) {
                                state = State.EOF;
                                return false;
                            }
                        }
                        char ch = buffer[start];
                        if (ch == '\n') {
                            ++start;
                            state = State.EOR;
                        } else {
                            state = State.IN_RECORD;
                            return true;
                        }

                    case EOR:
                        if (start >= end) {
                            eof = !readMore();
                            if (eof) {
                                state = State.EOF;
                                return false;
                            }
                        }
                        state = State.IN_RECORD;
                        return start < end;

                    case EOF:
                        return false;
                }
            }
        }

        public boolean nextField() throws IOException {
            switch (state) {
                case INIT:
                case EOR:
                case EOF:
                case CR:
                    return false;

                case IN_RECORD:
                    boolean eof;
                    boolean startedQuote = false;
                    int lastQuotePosition = -1;
                    int p = start;
                    while (true) {
                        if (p >= end) {
                            int s = start;
                            eof = !readMore();
                            if (eof) {
                                state = State.EOF;
                                return true;
                            }
                            p -= (s - start);
                        }
                        char ch = buffer[p];
                        if (ch == quote) {
                            startedQuote = true;
                            lastQuotePosition = p;
                        } else if (ch == fieldDelimiter) {
                            if (!startedQuote) {
                                fStart = start;
                                fEnd = p;
                                start = p + 1;
                                return true;
                            } else if (startedQuote && lastQuotePosition == p - 1) {
                                // set the position of fStart to +1, fEnd to -1 to remove quote character
                                fStart = start + 1;
                                fEnd = p - 1;
                                start = p + 1;
                                startedQuote = false;
                                lastQuotePosition = -1;
                                return true;
                            }
                            // If the control flow reaches here: we have comma in this field and
                            // there should be a quote in the beginning and the end of
                            // this field. So, just continue reading next character
                        } else if (ch == '\n') {
                            if (!startedQuote) {
                                fStart = start;
                                fEnd = p;
                            } else if (startedQuote && lastQuotePosition == p - 1) {
                                // set the position of fStart to +1, fEnd to -1 to remove quote character
                                fStart = start + 1;
                                fEnd = p - 1;
                                startedQuote = false;
                                lastQuotePosition = -1;
                            } else {
                                throw new IllegalStateException(
                                        "Check the field value. It needs to be enclosed by quotes if the delimiter is in the field.");
                            }
                            start = p + 1;
                            state = State.EOR;
                            return true;
                        } else if (ch == '\r') {
                            if (!startedQuote) {
                                fStart = start;
                                fEnd = p;
                            } else if (startedQuote && lastQuotePosition == p - 1) {
                                // set the position of fStart to +1, fEnd to -1 to remove quote character
                                fStart = start + 1;
                                fEnd = p - 1;
                                startedQuote = false;
                                lastQuotePosition = -1;
                            } else {
                                throw new IllegalStateException(
                                        "Check the field value. It needs to be enclosed by quotes if the delimiter is in the field.");
                            }
                            start = p + 1;
                            state = State.CR;
                            return true;
                        }
                        ++p;
                    }
            }
            throw new IllegalStateException();
        }

        private boolean readMore() throws IOException {
            if (start > 0) {
                System.arraycopy(buffer, start, buffer, 0, end - start);
            }
            end -= start;
            start = 0;

            if (end == buffer.length) {
                buffer = Arrays.copyOf(buffer, buffer.length + INCREMENT);
            }

            int n = in.read(buffer, end, buffer.length - end);
            if (n < 0) {
                return false;
            }
            end += n;
            return true;
        }
    }
}
