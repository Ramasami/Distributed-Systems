package distributed.systems.http.client;

import org.apache.http.nio.reactor.IOReactorException;

import java.util.Arrays;

public class Application {

    public static final String WORKER_1 = "http://localhost:8081/task";
    public static final String WORKER_2 = "http://localhost:8082/task";

    public static void main(String[] args) throws IOReactorException {
        Aggregator aggregator = new Aggregator();
        System.out.println(aggregator.sendTasksToWorkers(Arrays.asList(WORKER_1,WORKER_2), Arrays.asList("1,2,3","3,4,5")));
    }
}
