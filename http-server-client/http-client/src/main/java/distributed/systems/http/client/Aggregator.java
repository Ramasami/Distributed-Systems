package distributed.systems.http.client;

import org.apache.http.HttpResponse;
import org.apache.http.nio.reactor.IOReactorException;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Aggregator {

    private WebClient client;

    public Aggregator() throws IOReactorException {
        client = new WebClient();
    }

    public List<String> sendTasksToWorkers(List<String> workersAddresses, List<String> tasks) {
        Future<HttpResponse> [] futures = new Future[workersAddresses.size()];

        for(int i=0;i<workersAddresses.size();i++) {
            String workerAddress = workersAddresses.get(i);
            String task = tasks.get(i);

            byte[] requestPayload = task.getBytes();
            futures[i] = client.sendTask(workerAddress,requestPayload);
        }

        return Stream.of(futures).map(httpResponseFuture -> {
            try {
                return httpResponseFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            return null;
        }).filter(Objects::nonNull)
                .map(response -> {
            byte[] responseByte = null;
            try {
                responseByte = new byte[response.getEntity().getContent().available()];
                response.getEntity().getContent().read(responseByte);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if(responseByte == null)
                return "";
            return new String(responseByte);
        }).collect(Collectors.toList());
    }
}

