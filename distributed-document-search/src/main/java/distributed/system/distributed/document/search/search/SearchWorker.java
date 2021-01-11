package distributed.system.distributed.document.search.search;

import distributed.system.distributed.document.search.model.DocumentData;
import distributed.system.distributed.document.search.model.Result;
import distributed.system.distributed.document.search.model.SerializationUtils;
import distributed.system.distributed.document.search.model.Task;
import distributed.system.distributed.document.search.networking.OnRequestCallBack;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class SearchWorker implements OnRequestCallBack {

    private static final String ENDPOINT = "/task";

    @Override
    public byte[] handleRequest(byte[] requestPayload) {
        Task task = (Task) SerializationUtils.deserialize(requestPayload);
        Result result = createResult(task);
        return SerializationUtils.serialize(result);
    }

    private Result createResult(Task task) {
        List<String> documents = task.getDocuments();
        System.out.println(String.format("Received %d documents to process", documents.size()));

        Result result = new Result();

        for(String document : documents) {
            List<String> documentTerms = null;
                documentTerms = parseWordsFromDocuments(document);
                DocumentData documentData = TFIDF.createDocumentData(documentTerms, task.getQueryTerms());
                result.addDocumentData(document, documentData);
        }
        return result;
    }

    private List<String> parseWordsFromDocuments(String document) {
        try {
            ClassLoader classLoader = SearchWorker.class.getClassLoader();
            URL resource = classLoader.getResource(document);

            FileReader fileReader = new FileReader(resource.getFile());
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            List<String> lines = bufferedReader.lines().collect(Collectors.toList());
            List<String> documentTerms = TFIDF.getWordsFromLines(lines);
            return documentTerms;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    @Override
    public String getEndPoint() {
        return ENDPOINT;
    }
}
