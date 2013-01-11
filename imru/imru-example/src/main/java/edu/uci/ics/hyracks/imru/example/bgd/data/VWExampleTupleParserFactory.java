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

package edu.uci.ics.hyracks.imru.example.bgd.data;

import java.io.BufferedReader;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import edu.uci.ics.hyracks.api.comm.IFrameWriter;
import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.FrameTupleAppender;
import edu.uci.ics.hyracks.dataflow.common.comm.util.FrameUtils;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParser;
import edu.uci.ics.hyracks.dataflow.std.file.ITupleParserFactory;

/**
 * Parser for Vowpal Wabbit input files.
 *
 * @see https://github.com/JohnLangford/vowpal_wabbit/wiki/Input-format
 * @author Josh Rosen
 */
public class VWExampleTupleParserFactory implements ITupleParserFactory {
    private static final long serialVersionUID = 1L;
    final int featureLength;
    private static Logger LOG = Logger.getLogger(VWExampleTupleParserFactory.class.getName());

    public VWExampleTupleParserFactory(int featureLength) {
        this.featureLength = featureLength;
    }

    @Override
    public ITupleParser createTupleParser(final IHyracksTaskContext ctx) {
        return new ITupleParser() {

            @Override
            public void parse(InputStream in, IFrameWriter writer) throws HyracksDataException {
                ByteBuffer frame = ctx.allocateFrame();
                FrameTupleAppender appender = new FrameTupleAppender(ctx.getFrameSize());
                appender.reset(frame, true);
                ArrayTupleBuilder tb = new ArrayTupleBuilder(2);
                DataOutput dos = tb.getDataOutput();
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                int activeFeatures = 0;
                try {
                    Pattern whitespacePattern = Pattern.compile("\\s+");
                    Pattern colonPattern = Pattern.compile(":");
                    while (true) {
                        tb.reset();
                        String line = reader.readLine();
                        if (line == null) {
                            break;
                        }
                        String[] comps = whitespacePattern.split(line);

                        // Label
                        // Ignore leading plus sign
                        int label = Integer.parseInt(comps[0]);
                        dos.writeInt(label);
                        tb.addFieldEndOffset();

                        int firstNamespaceIndex;
                        // The importance and tag are both optional.
                        // For now, we ignore them.


                        if (!comps[1].contains("|")) {
                            firstNamespaceIndex = 2;
                        } else {
                            firstNamespaceIndex = 1;
                        }

                        // Process the first namespace:
                        String namespace;
                        float namespaceWeight;
                        comps[firstNamespaceIndex] = comps[firstNamespaceIndex].replaceFirst("[^|]*\\|", "");
                        if (comps[firstNamespaceIndex].contains(":")) {
                            String[] namespaceWeightPair = colonPattern.split(comps[firstNamespaceIndex]);
                            namespace = namespaceWeightPair[0];
                            namespaceWeight = Float.parseFloat(namespaceWeightPair[1]);
                        } else {
                            namespace = comps[firstNamespaceIndex];
                            namespaceWeight = 1.0f;
                        }

                        TreeMap<Integer, Float> features = new TreeMap<Integer, Float>();

                        for (int i = (firstNamespaceIndex + 1); i < comps.length; i++) {
                            if (comps[i].charAt(0) == '|') { // Namespace declaration
                                comps[i] = comps[i].substring(1);
                                if (comps[i].contains(":")) {
                                    String[] namespaceWeightPair = colonPattern.split(comps[i]);
                                    namespace = namespaceWeightPair[0];
                                    namespaceWeight = Float.parseFloat(namespaceWeightPair[1]);
                                } else {
                                    namespace = comps[i];
                                    namespaceWeight = 1.0f;
                                }
                            } else { // Feature
                                String[] nameValuePair = colonPattern.split(comps[i]);
                                String featureName = namespace + nameValuePair[0];
                                float featureValue;
                                if (nameValuePair.length == 2) {
                                    featureValue = Float.parseFloat(nameValuePair[1]);
                                } else {
                                    featureValue = 1.0f;
                                }
                                featureValue *= namespaceWeight;
                                int featureIndex = Math.abs(featureName.hashCode() % featureLength) + 1;
                                features.put(featureIndex, featureValue);
                            }
                        }
                        for (Entry<Integer, Float> feature : features.entrySet()) {
                            dos.writeInt(feature.getKey());
                            dos.writeFloat(feature.getValue());
                        }
                        activeFeatures += features.size();
                        dos.writeInt(-1); // Marks the end of the sparse array.
                        tb.addFieldEndOffset();
                        if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                            FrameUtils.flushFrame(frame, writer);
                            appender.reset(frame, true);
                            if (!appender.append(tb.getFieldEndOffsets(), tb.getByteArray(), 0, tb.getSize())) {
                                LOG.severe("Example too large to fit in frame: " + line);
                                throw new IllegalStateException();
                            }
                        }
                    }
                    if (appender.getTupleCount() > 0) {
                        FrameUtils.flushFrame(frame, writer);
                    }
                } catch (IOException e) {
                    throw new HyracksDataException(e);
                }
                LOG.info("Parsed input partition containing " + activeFeatures + " active features");
            }
        };
    }
}