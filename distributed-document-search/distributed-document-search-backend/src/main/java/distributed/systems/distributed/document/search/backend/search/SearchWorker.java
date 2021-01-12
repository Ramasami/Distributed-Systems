package distributed.systems.distributed.document.search.backend.search;

import distributed.systems.distributed.document.search.backend.model.DocumentData;
import distributed.systems.distributed.document.search.backend.model.SerializationUtils;
import distributed.systems.distributed.document.search.backend.networking.OnRequestCallBack;
import distributed.systems.distributed.document.search.backend.model.Result;
import distributed.systems.distributed.document.search.backend.model.Task;

import java.io.*;
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
            InputStream bufferedReader = classLoader.getResourceAsStream(document);

            List<String> documentTerms = TFIDF.getWordsFromLine(readAllBytes(bufferedReader));
            return documentTerms;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    private String readAllBytes(InputStream requestBody) throws IOException {
        byte message[] = new byte[requestBody.available()];
        requestBody.read(message);
        return new String(message);
    }

    @Override
    public String getEndPoint() {
        return ENDPOINT;
    }
}
