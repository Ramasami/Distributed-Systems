package distributed.systems.distributed.document.search.backend.search;

import com.google.protobuf.InvalidProtocolBufferException;
import distributed.systems.distributed.document.search.backend.cluster.management.ServiceRegistry;
import distributed.systems.distributed.document.search.backend.model.DocumentData;
import distributed.systems.distributed.document.search.backend.model.SerializationUtils;
import distributed.systems.distributed.document.search.backend.model.proto.SearchModel;
import distributed.systems.distributed.document.search.backend.networking.OnRequestCallBack;
import distributed.systems.distributed.document.search.backend.networking.WebClient;
import distributed.systems.distributed.document.search.backend.model.Result;
import distributed.systems.distributed.document.search.backend.model.Task;
import org.apache.http.HttpResponse;
import org.apache.zookeeper.KeeperException;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class SearchCoordinator implements OnRequestCallBack {

    private static final String ENDPOINT = "/distributed/systems/distributed/document/search/backend";
    public static final String BOOKS_DIRECTORY = "books";
    private final ServiceRegistry workersServiceRegistry;
    private final WebClient client;
    private final List<String> documents;


    public SearchCoordinator(ServiceRegistry workerServiceRegistry, WebClient client) throws URISyntaxException {
        this.workersServiceRegistry = workerServiceRegistry;
        this.client = client;
        this.documents = readDocumentsList();
    }

    @Override
    public byte[] handleRequest(byte[] requestPayload) {
        try {
            SearchModel.Request request = SearchModel.Request.parseFrom(requestPayload);
            SearchModel.Response response = createResponse(request);
            return response.toByteArray();
        } catch (InvalidProtocolBufferException | InterruptedException | KeeperException e) {
            e.printStackTrace();
            return SearchModel.Response.getDefaultInstance().toByteArray();
        }

    }

    private SearchModel.Response createResponse(SearchModel.Request request) throws KeeperException, InterruptedException {
        SearchModel.Response.Builder searchResponse = SearchModel.Response.newBuilder();

        System.out.println("Received Search Query: " + request.getSearchQuery());

        List<String> queryTerms = TFIDF.getWordsFromLine(request.getSearchQuery());

        List<String> workers = workersServiceRegistry.getAllServiceAddresses();

        if(workers.isEmpty()) {
            System.out.println("No search workers currently available");
            return searchResponse.build();
        }

        List<Task> tasks = createTasks(workers.size(), queryTerms);
        List<Result> results = sendTasksToWorkers(workers, tasks);

        List<SearchModel.Response.DocumentStats> sortedDocuments = aggregateResults(results ,queryTerms);
        searchResponse.addAllRelevantDocuments(sortedDocuments);
        return searchResponse.build();
    }

    private List<SearchModel.Response.DocumentStats> aggregateResults(List<Result> results, List<String> queryTerms) {
        Map<String, DocumentData> allDocumentsResults = new HashMap<>();

        for(Result result : results) {
            allDocumentsResults.putAll(result.getDocumentToDocumentData());
        }

        System.out.println("Calculating score for all the documents");
        Map<Double, List<String>> scoreToDocuments = TFIDF.getDocumentsSortedByScore(queryTerms, allDocumentsResults);
        return returnSortedDocumentsByScore(scoreToDocuments);
    }

    private List<SearchModel.Response.DocumentStats> returnSortedDocumentsByScore(Map<Double, List<String>> scoreToDocuments) {
        List<SearchModel.Response.DocumentStats> sortedDocumentsStatsList = new ArrayList<>();

        for(Map.Entry<Double, List<String>> docScorePair : scoreToDocuments.entrySet()) {
            double score = docScorePair.getKey();

            for(String document : docScorePair.getValue()) {
                File documentPath = new File(document);

                SearchModel.Response.DocumentStats documentStats = SearchModel.Response.DocumentStats.newBuilder()
                        .setScore(score)
                        .setDocumentName(documentPath.getName())
                        .setDocumentSize(documentPath.length())
                        .build();

                sortedDocumentsStatsList.add(documentStats);
            }
        }
        return sortedDocumentsStatsList;
    }

    @Override
    public String getEndPoint() {
        return ENDPOINT;
    }

    private List<Result> sendTasksToWorkers(List<String> workers, List<Task> tasks) {
        Future[] futures = new Future[workers.size()];
        for(int i=0;i<workers.size();i++) {
            String worker = workers.get(i);
            Task task = tasks.get(i);
            byte[] payload = SerializationUtils.serialize(task);
            futures[i] = client.sendTask(worker, payload);
        }

        List<Result> results = new ArrayList<>();
        for(Future<HttpResponse> future : futures) {
            try {
                HttpResponse response = future.get();
                InputStream input = response.getEntity().getContent();
                byte[] responseBytes = new byte[input.available()];
                input.read(responseBytes);
                Result result = (Result) SerializationUtils.deserialize(responseBytes);
                results.add(result);
            } catch (InterruptedException | ExecutionException | IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println(String.format("Received %d/%d results", results.size(), tasks.size()));
        return results;
    }


    public List<Task> createTasks(int numberOfWorkers, List<String> queryTerms) {
        List<List<String>> workerDocuments = splitDocumentList(numberOfWorkers, documents);

        List<Task> tasks = new ArrayList<>();

        for(List<String> documentsForWorker : workerDocuments) {
            Task task = new Task(queryTerms, documentsForWorker);
            tasks.add(task);
        }
        return tasks;
    }

    private static List<List<String>> splitDocumentList(int numberOfWorkers, List<String> documents) {
        int numberOfDocumentsPerWorker = (documents.size() + numberOfWorkers - 1) / numberOfWorkers;
        List<List<String>> workerDocuments = new ArrayList<>();

        for(int i=0;i<numberOfWorkers;i++) {
            int firstDocumentIndex = i * numberOfDocumentsPerWorker;
            int lastDocumentIndexExclusive = Math.min(firstDocumentIndex + numberOfDocumentsPerWorker, documents.size());

            if(firstDocumentIndex >= lastDocumentIndexExclusive) {
                break;
            }
            List<String> currentWorkerDocuments = new ArrayList<>(documents.subList(firstDocumentIndex, lastDocumentIndexExclusive));
            workerDocuments.add(currentWorkerDocuments);
        }
        return workerDocuments;
    }

    private List<String> readDocumentsList() throws URISyntaxException {

        File documentDirectory = getFile(BOOKS_DIRECTORY);
        return Arrays.stream(documentDirectory.list())
                .map(documentName -> BOOKS_DIRECTORY + "/" + documentName)
                .collect(Collectors.toList());
    }

    private File getFile(String path) throws URISyntaxException {
        ClassLoader classLoader = SearchCoordinator.class.getClassLoader();
        URL resource = classLoader.getResource(path);
        return new File(resource.toURI());
    }
}
