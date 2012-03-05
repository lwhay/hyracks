package edu.uci.ics.hyracks.storage.am.lsm.inverteredindex;

import static org.junit.Assert.fail;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import edu.uci.ics.hyracks.dataflow.common.comm.io.ByteArrayAccessibleOutputStream;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexAccessor;
import edu.uci.ics.hyracks.storage.am.common.api.IIndexBulkLoadContext;
import edu.uci.ics.hyracks.storage.am.invertedindex.api.IInvertedIndex;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IBinaryTokenizer;
import edu.uci.ics.hyracks.storage.am.invertedindex.tokenizers.IToken;

public abstract class AbstractInvertedIndexTest {
    protected IInvertedIndex invertedIndex;
    protected IIndexAccessor invertedIndexAccessor;
    protected IBinaryComparator[] tokenComparators;
    protected IBinaryTokenizer tokenizer;

    // This number will only be used in generating random documents.
    // If predefined data is generated, then the number of documents is fixed.
    protected int numDocuments = 1000;
    protected int maxDocumentLength = 300;
    protected Set<String> documents = new HashSet<String>();

    @Before
    protected abstract void setUp();

    @After
    protected abstract void tearDown();
    
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
}
