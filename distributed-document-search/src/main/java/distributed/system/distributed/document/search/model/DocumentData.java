package distributed.system.distributed.document.search.model;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class DocumentData implements Serializable {

    private Map<String, Double> termToFrequency = new HashMap<>();

    public void putTermFrequency(String queryTerm, double frequency) {
        termToFrequency.put(queryTerm,frequency);
    }

    public double getFrequency(String queryTerm) {
        return termToFrequency.get(queryTerm);
    }
}
