package distributed.systems.distributed.document.search.frontend.search;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import distributed.systems.distributed.document.search.frontend.cluster.management.ServiceRegistry;
import distributed.systems.distributed.document.search.frontend.model.frontend.FrontendSearchRequest;
import distributed.systems.distributed.document.search.frontend.model.frontend.FrontendSearchResponse;
import distributed.systems.distributed.document.search.frontend.model.proto.SearchModel;
import distributed.systems.distributed.document.search.frontend.networking.OnRequestCallBack;
import distributed.systems.distributed.document.search.frontend.networking.WebClient;
import org.apache.http.HttpResponse;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class UserSearchHandler implements OnRequestCallBack {
    private static final String ENDPOINT = "/documents_search";
    private static final String DOCUMENTS_LOCATION = "books";
    private final ObjectMapper objectMapper;
    private final WebClient client;
    private final ServiceRegistry searchCoordinator;

    public UserSearchHandler(ServiceRegistry searchCoordinator) throws IOReactorException {
        this.searchCoordinator = searchCoordinator;
        this.client = new WebClient();
        this.objectMapper = new ObjectMapper();
        this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.objectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
    }


    @Override
    public byte[] handleRequest(byte[] requestPayload) {
        try {
            FrontendSearchRequest frontendSearchRequest =
                    objectMapper.readValue(requestPayload, FrontendSearchRequest.class);

            FrontendSearchResponse frontendSearchResponse = createFrontendResponse(frontendSearchRequest);

            return objectMapper.writeValueAsBytes(frontendSearchResponse);
        } catch (IOException e) {
            e.printStackTrace();
            return new byte[0];
        }
    }

    private FrontendSearchResponse createFrontendResponse(FrontendSearchRequest frontendSearchRequest) {
        SearchModel.Response searchClusterResponse = sendRequestToSearchCluster(frontendSearchRequest.getSearchQuery());

        List<FrontendSearchResponse.SearchResultInfo> filteredResults =
                filterResults(searchClusterResponse,
                        frontendSearchRequest.getMaxNumberOfResults(),
                        frontendSearchRequest.getMinScore());

        return new FrontendSearchResponse(filteredResults, DOCUMENTS_LOCATION);
    }

    private List<FrontendSearchResponse.SearchResultInfo> filterResults(SearchModel.Response searchClusterResponse,
                                                                        long maxResults,
                                                                        double minScore) {

        double maxScore = getMaxScore(searchClusterResponse);

        List<FrontendSearchResponse.SearchResultInfo> searchResultInfoList = new ArrayList<>();

        for (int i = 0; i < searchClusterResponse.getRelevantDocumentsCount() && i < maxResults; i++) {

            int normalizedDocumentScore = normalizeScore(searchClusterResponse.getRelevantDocuments(i).getScore(), maxScore);
            if (normalizedDocumentScore < minScore) {
                continue; // break in the lecture is an error
            }

            String documentName = searchClusterResponse.getRelevantDocuments(i).getDocumentName();

            String title = getDocumentTitle(documentName);
            String extension = getDocumentExtension(documentName);

            FrontendSearchResponse.SearchResultInfo resultInfo =
                    new FrontendSearchResponse.SearchResultInfo(title, extension, normalizedDocumentScore);

            searchResultInfoList.add(resultInfo);
        }

        return searchResultInfoList;
    }

    @Override
    public String getEndPoint() {
        return ENDPOINT;
    }

    private static String getDocumentExtension(String document) {
        String[] parts = document.split("\\.");
        if (parts.length == 2) {
            return parts[1];
        }
        return "";
    }

    private static String getDocumentTitle(String document) {
        return document.split("\\.")[0];
    }

    private static int normalizeScore(double inputScore, double maxScore) {
        return (int) Math.ceil(inputScore * 100.0 / maxScore);
    }

    private static double getMaxScore(SearchModel.Response searchClusterResponse) {
        if (searchClusterResponse.getRelevantDocumentsCount() == 0) {
            return 0;
        }
        return searchClusterResponse.getRelevantDocumentsList()
                .stream()
                .map(document -> document.getScore())
                .max(Double::compareTo)
                .get();
    }

    private SearchModel.Response sendRequestToSearchCluster(String searchQuery) {
        SearchModel.Request searchRequest = SearchModel.Request.newBuilder()
                .setSearchQuery(searchQuery)
                .build();

        try {
            String coordinatorAddress = searchCoordinator.getRandomServiceAddress();
            if (coordinatorAddress == null) {
                System.out.println("Search Cluster Coordinator is unavailable");
                return SearchModel.Response.getDefaultInstance();
            }

            Future<HttpResponse> futureResponse = client.sendTask(coordinatorAddress, searchRequest.toByteArray());
            HttpResponse response = futureResponse.get();

            byte[] payloadBody = readAllBytes(response.getEntity().getContent());

            return SearchModel.Response.parseFrom(payloadBody);
        } catch (InterruptedException | KeeperException | ExecutionException | IOException e) {
            e.printStackTrace();
            return SearchModel.Response.getDefaultInstance();
        }
    }

    private byte[] readAllBytes(InputStream requestBody) throws IOException {
        byte[] message = new byte[requestBody.available()];
        requestBody.read(message);
        return message;
    }
}
