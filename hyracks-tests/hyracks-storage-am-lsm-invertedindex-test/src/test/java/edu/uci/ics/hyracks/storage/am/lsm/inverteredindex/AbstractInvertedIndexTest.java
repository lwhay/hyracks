package edu.uci.ics.hyracks.storage.am.lsm.inverteredindex;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.After;
import org.junit.Before;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.exceptions.HyracksException;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexCursor;
import edu.uci.ics.hyracks.storage.am.common.api.IndexException;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.impls.InvertedIndexSearchPredicate;
import edu.uci.ics.hyracks.storage.am.invertedindex.searchmodifiers.ConjunctiveSearchModifier;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IToken;

public abstract class AbstractInvertedIndexTest {
    protected Logger LOGGER;
    protected LSMInvertedIndexTestHarness harness = new LSMInvertedIndexTestHarness();

    protected IInvertedIndex invertedIndex;
    protected IIndexAccessor invertedIndexAccessor;
    protected IBinaryComparator[] tokenComparators;
    protected IBinaryTokenizer tokenizer;

    // This number will only be used in generating random documents.
    // If predefined data is generated, then the number of documents is fixed.
    protected int numDocuments = 1000;
    protected int maxDocumentLength = 50;
    protected Set<String> documents = new HashSet<String>();
    protected Map<String, SortedSet<Integer>> baselineInvertedIndex = new HashMap<String, SortedSet<Integer>>();
    
    // Generate random data is false by default (generate predefined data is true!)
    protected boolean random = false;

    // Subclasses must implement these methods by initializing the proper class members
    protected abstract void setTokenizer();

    protected abstract void setInvertedIndex();

    protected abstract void setLogger();

    protected abstract void setRandom();

    @Before
    public void setUp() throws HyracksException {
        harness.setUp();
        setTokenizer();
        setInvertedIndex();
        setLogger();
        setRandom();
        generateData();

        invertedIndex.create(harness.getFileId());
        invertedIndex.open(harness.getFileId());
        invertedIndexAccessor = invertedIndex.createAccessor();
    }

    @After
    public void tearDown() throws HyracksDataException {
        invertedIndex.close();
        harness.tearDown();
    }

    protected void generateData() {
        if (random) {
            generateRandomDocumentData();
        } else {
            generatePredefinedDocumentData();
        }
    }

    protected void generateRandomDocumentData() {
        int documentLength;
        String validCharacters = "abcdefghijklmnopqrstuvwxyz ";
        StringBuilder builder = new StringBuilder();
        Random rng = new Random();

        // Generate numDocuments random documents (strings)
        documents.clear();
        for (int i = 0; i < numDocuments; i++) {

            // Generate a random string of size [0, maxDocumentLength] with 
            // characters chosen from the set of valid characters defined above
            documentLength = rng.nextInt(maxDocumentLength + 1);
            for (int j = 0; j < documentLength; j++) {
                builder.append(validCharacters.charAt(rng.nextInt(validCharacters.length())));
            }

            // Ensure that numDocuments is honored by regenerating the document 
            // if it is a duplicate.
            if (!documents.add(builder.toString())) {
                i--;
            }

            builder.setLength(0);
        }
    }

    protected void generatePredefinedDocumentData() {
        List<String> firstNames = new ArrayList<String>();
        List<String> lastNames = new ArrayList<String>();

        // Generate first names
        firstNames.add("Kathrin");
        firstNames.add("Cathrin");
        firstNames.add("Kathryn");
        firstNames.add("Cathryn");
        firstNames.add("Kathrine");
        firstNames.add("Cathrine");
        firstNames.add("Kathryne");
        firstNames.add("Cathryne");
        firstNames.add("Katherin");
        firstNames.add("Catherin");
        firstNames.add("Katheryn");
        firstNames.add("Catheryn");
        firstNames.add("Katherine");
        firstNames.add("Catherine");
        firstNames.add("Katheryne");
        firstNames.add("Catheryne");
        firstNames.add("John");
        firstNames.add("Jack");
        firstNames.add("Jonathan");
        firstNames.add("Nathan");

        // Generate last names
        lastNames.add("Miller");
        lastNames.add("Myller");
        lastNames.add("Keller");
        lastNames.add("Ketler");
        lastNames.add("Muller");
        lastNames.add("Fuller");
        lastNames.add("Smith");
        lastNames.add("Smyth");
        lastNames.add("Smithe");
        lastNames.add("Smythe");

        // Generate all 'firstName lastName' combinations
        documents.clear();
        for (String first : firstNames) {
            for (String last : lastNames) {
                documents.add(first + " " + last);
            }
        }

        // The number of documents is fixed since the data is predefined
        numDocuments = documents.size();
    }

    protected class TokenIdPair implements Comparable<TokenIdPair> {
        public ByteArrayAccessibleOutputStream baaos = new ByteArrayAccessibleOutputStream();
        public DataOutputStream dos = new DataOutputStream(baaos);
        public int id;

        TokenIdPair(IToken token, int id) throws IOException {
            token.serializeToken(dos);
            this.id = id;
        }

        @Override
        public int compareTo(TokenIdPair o) {
            int cmp = tokenComparators[0].compare(baaos.getByteArray(), 0, baaos.getByteArray().length,
                    o.baaos.getByteArray(), 0, o.baaos.getByteArray().length);
            if (cmp == 0) {
                return id - o.id;
            } else {
                return cmp;
            }
        }
    }

    protected void buildBaselineIndex() throws IOException {
        ITupleReference tuple;
        IToken token;
        SortedSet<Integer> baselineInvertedList = null;
        ByteArrayAccessibleOutputStream baos = new ByteArrayAccessibleOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        ISerializerDeserializer[] fieldSerDes = new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

        int docId = 0;
        for (String document : documents) {
            tuple = TupleUtils.createTuple(fieldSerDes, document, docId);

            // Insert into the baseline
            tokenizer.reset(tuple.getFieldData(0), tuple.getFieldStart(0), tuple.getFieldLength(0));
            while (tokenizer.hasNext()) {
                baos.reset();
                tokenizer.next();
                token = tokenizer.getToken();
                token.serializeToken(dos);
                String tokenStr = (String) fieldSerDes[0].deserialize(new DataInputStream(new ByteArrayInputStream(baos
                        .getByteArray())));
                baselineInvertedList = baselineInvertedIndex.get(tokenStr);
                if (baselineInvertedList == null) {
                    baselineInvertedList = new TreeSet<Integer>();
                    baselineInvertedIndex.put(tokenStr, baselineInvertedList);
                }
                baselineInvertedList.add(docId);
            }
            docId++;
        }
    }
    
    
    protected void insertDocuments() throws HyracksDataException, IndexException {
        ITupleReference tuple;
        ISerializerDeserializer[] fieldSerDes = new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };

        // Insert the documents into the index while building the baseline
        int docId = 0;
        for (String document : documents) {
            // Insert into the index to be tested
            tuple = TupleUtils.createTuple(fieldSerDes, document, docId);
            invertedIndexAccessor.insert(tuple);
            docId++;
        }
    }

    protected void verifyAgainstBaseline() throws HyracksDataException, IndexException {
        ITupleReference tuple;
        int docId;
        SortedSet<Integer> baselineInvertedList = null;
        SortedSet<Integer> testInvertedList = new TreeSet<Integer>();

        // Query all tokens in the baseline
        IIndexCursor resultCursor = invertedIndexAccessor.createSearchCursor();
        ConjunctiveSearchModifier searchModifier = new ConjunctiveSearchModifier();
        InvertedIndexSearchPredicate searchPred = new InvertedIndexSearchPredicate(searchModifier);
        for (String tokenStr : baselineInvertedIndex.keySet()) {
            tuple = TupleUtils.createTuple(new ISerializerDeserializer[] { UTF8StringSerializerDeserializer.INSTANCE },
                    tokenStr);
            searchPred.setQueryTuple(tuple);
            searchPred.setQueryFieldIndex(0);
            resultCursor.reset();
            invertedIndexAccessor.search(resultCursor, searchPred);

            // Check the matches
            testInvertedList.clear();
            while (resultCursor.hasNext()) {
                resultCursor.next();
                baselineInvertedList = baselineInvertedIndex.get(tokenStr);
                tuple = resultCursor.getTuple();
                docId = IntegerSerializerDeserializer.getInt(tuple.getFieldData(0), tuple.getFieldStart(0));
                testInvertedList.add(docId);
            }

            if (LOGGER != null && LOGGER.isLoggable(Level.INFO)) {
                LOGGER.info("\nQuery:\t\t\"" + tokenStr + "\"\n" + "Baseline:\t" + baselineInvertedList.toString()
                        + "\n" + "Test:\t\t" + testInvertedList.toString() + "\n");
            }
            assertTrue(baselineInvertedList.containsAll(testInvertedList));
        }
    }
    
    protected void bulkLoadDocuments() throws IndexException, IOException {
        List<TokenIdPair> pairs = new ArrayList<TokenIdPair>();
        ArrayTupleBuilder tb = new ArrayTupleBuilder(2);
        ArrayTupleReference tuple = new ArrayTupleReference();

        // Generate pairs for sorting and bulk-loading
        int docId = 0;
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
            docId++;
        }

        Collections.sort(pairs);

        IIndexBulkLoadContext bulkLoadCtx = invertedIndex.beginBulkLoad(1.0f);
        for (TokenIdPair t : pairs) {
            tb.reset();
            tb.addField(t.baaos.getByteArray(), 0, t.baaos.getByteArray().length);
            IntegerSerializerDeserializer.INSTANCE.serialize(t.id, tb.getDataOutput());
            tb.addFieldEndOffset();
            tuple.reset(tb.getFieldEndOffsets(), tb.getByteArray());
            invertedIndex.bulkLoadAddTuple(tuple, bulkLoadCtx);
        }
        invertedIndex.endBulkLoad(bulkLoadCtx);
    }
}
