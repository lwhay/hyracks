package edu.uci.ics.hyracks.storage.am.lsm.inverteredindex;

import static org.junit.Assert.fail;

import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IToken;

public abstract class AbstractInvertedIndexBulkloadTest extends AbstractInvertedIndexTest {

    @Test
    protected void bulkLoadTest() {
        List<TokenIdPair> pairs = new ArrayList<TokenIdPair>();
        ArrayTupleBuilder tb = new ArrayTupleBuilder(2);
        ArrayTupleReference tuple = new ArrayTupleReference();
        int docId = 0;

        try {
            IIndexBulkLoadContext bulkLoadCtx = invertedIndex.beginBulkLoad(1.0f);

            // Generate pairs for sorting and bulk-loading
            for (String s : documents) {
                ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
                DataOutputStream dos = new DataOutputStream(baaos);
                UTF8StringSerializerDeserializer.INSTANCE.serialize(s, dos);
                tokenizer.reset(baaos.getByteArray(), 0, baaos.size());
                while (tokenizer.hasNext()) {
                    tokenizer.next();
                    IToken token = tokenizer.getToken();
                    pairs.add(new TokenIdPair(token, docId));
                }
                ++docId;
            }

            Collections.sort(pairs);

            for (TokenIdPair t : pairs) {
                tb.reset();
                tb.addField(t.baaos.getByteArray(), 0, t.baaos.getByteArray().length);
                IntegerSerializerDeserializer.INSTANCE.serialize(t.id, tb.getDataOutput());
                tb.addFieldEndOffset();
                tuple.reset(tb.getFieldEndOffsets(), tb.getByteArray());
                invertedIndex.bulkLoadAddTuple(tuple, bulkLoadCtx);
            }
            invertedIndex.endBulkLoad(bulkLoadCtx);
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }
}
