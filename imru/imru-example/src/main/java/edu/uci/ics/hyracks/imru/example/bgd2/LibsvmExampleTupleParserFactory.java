package edu.uci.ics.hyracks.imru.example.bgd2;

import java.io.BufferedReader;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.Scanner;
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

public class LibsvmExampleTupleParserFactory implements ITupleParserFactory {
    private static final long serialVersionUID = 1L;
    final int featureLength;
    private static Logger LOG = Logger.getLogger(LibsvmExampleTupleParserFactory.class.getName());

    public LibsvmExampleTupleParserFactory(int featureLength) {
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
                    Pattern labelFeaturePattern = Pattern.compile("[:=]");
                    String line;
                    boolean firstLine = true;
                    while (true) {
                        tb.reset();
                        if (firstLine) {
                            long start = System.currentTimeMillis();
                            line = reader.readLine();
                            long end = System.currentTimeMillis();
                            LOG.info("First call to reader.readLine() took " + (end - start) + " milliseconds");
                            firstLine = false;
                        } else {
                            line = reader.readLine();
                        }
                        if (line == null) {
                            break;
                        }
                        String[] comps = whitespacePattern.split(line, 2);

                        // Label
                        // Ignore leading plus sign
                        if (comps[0].charAt(0) == '+') {
                            comps[0] = comps[0].substring(1);
                        }

                        int label = Integer.parseInt(comps[0]);
                        dos.writeInt(label);
                        tb.addFieldEndOffset();
                        Scanner scan = new Scanner(comps[1]);
                        scan.useDelimiter(",|\\s+");
                        while (scan.hasNext()) {
                            String[] parts = labelFeaturePattern.split(scan.next());
                            int index = Integer.parseInt(parts[0]);
                            if (index > featureLength) {
                                throw new IndexOutOfBoundsException("Feature index " + index
                                        + " exceed the declared number of features (" + featureLength + ")");
                            }
                            // Ignore leading plus sign.
                            if (parts[1].charAt(0) == '+') {
                                parts[1] = parts[1].substring(1);
                            }
                            float value = Float.parseFloat(parts[1]);
                            dos.writeInt(index);
                            dos.writeFloat(value);
                            activeFeatures++;
                        }
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