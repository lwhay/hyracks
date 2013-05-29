import java.util.List;
import edu.uci.ics.hyracks.algebricks.examples.piglet.types.Type;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;

public class TestSchema {
    private List<Pair<String, Type>> schema;

    public TestSchema(List<Pair<String, Type>> schema) {
        this.schema = schema;
    }

    public List<Pair<String, Type>> getSchema() {
        return schema;
    }
}