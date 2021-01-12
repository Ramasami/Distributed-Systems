package distributed.systems.distributed.document.search.backend;

import distributed.systems.distributed.document.search.backend.cluster.management.OnElectionCallback;
import distributed.systems.distributed.document.search.backend.networking.WebClient;
import distributed.systems.distributed.document.search.backend.networking.WebServer;
import distributed.systems.distributed.document.search.backend.cluster.management.ServiceRegistry;
import distributed.systems.distributed.document.search.backend.search.SearchCoordinator;
import distributed.systems.distributed.document.search.backend.search.SearchWorker;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.zookeeper.KeeperException;

import java.net.InetAddress;
import java.net.URISyntaxException;
import java.net.UnknownHostException;

public class OnElectionAction implements OnElectionCallback {

    private final ServiceRegistry workerRegistry;
    private final ServiceRegistry coordinatorRegistry;
    private WebServer webserver;
    private WebClient webClient;
    private final int port;

    public OnElectionAction(ServiceRegistry workerRegistry, ServiceRegistry coordinatorRegistry, int port) throws IOReactorException {
        this.workerRegistry = workerRegistry;
        this.coordinatorRegistry = coordinatorRegistry;
        this.port = port;
        webClient = new WebClient();
    }


    @Override
    public void onElectedToBeLeader() throws KeeperException, InterruptedException, URISyntaxException {
        workerRegistry.unregisterFromCluster();
        workerRegistry.registerForUpdates();

        if(webserver != null) {
            webserver.stop();
        }

        SearchCoordinator searchCoordinator = new SearchCoordinator(workerRegistry,webClient);
        WebServer webserver = new WebServer(port, searchCoordinator);
        webserver.startServer();
        try {
            String currentServerAddress = String.format("http://%s:%d%s", InetAddress.getLocalHost().getCanonicalHostName(), port, searchCoordinator.getEndPoint());
            System.out.println(currentServerAddress);
            coordinatorRegistry.registerToCluster(currentServerAddress);
        } catch (UnknownHostException | InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onWorker() {
        SearchWorker searchWorker = new SearchWorker();
        WebServer webserver = new WebServer(port, searchWorker);
        webserver.startServer();
        try {
            String currentServerAddress = String.format("http://%s:%d%s", InetAddress.getLocalHost().getCanonicalHostName(), port, searchWorker.getEndPoint());
            System.out.println(currentServerAddress);
            workerRegistry.registerToCluster(currentServerAddress);
            workerRegistry.registerForUpdates();
        } catch (UnknownHostException | InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }
}
