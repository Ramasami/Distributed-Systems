package distributed.system.distributed.document.search.model;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class Task implements Serializable {

    private final List<String> queryTerms;
    private final List<String> documents;

    public Task(List<String> queryTerms, List<String> documents) {
        this.queryTerms = queryTerms;
        this.documents = documents;
    }

    public List<String> getQueryTerms() {
        return Collections.unmodifiableList(queryTerms);
    }

    public List<String> getDocuments() {
        return Collections.unmodifiableList(documents);
    }
}
