package distributed.system.distributed.document.search;

import distributed.system.distributed.document.search.cluster.management.OnElectionCallback;
import distributed.system.distributed.document.search.cluster.management.ServiceRegistry;
import distributed.system.distributed.document.search.networking.WebServer;
import distributed.system.distributed.document.search.search.SearchCoordinator;
import distributed.system.distributed.document.search.search.SearchWorker;
import org.apache.zookeeper.KeeperException;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class OnElectionAction implements OnElectionCallback {

    private final ServiceRegistry workerRegistry;
    private final ServiceRegistry coordinatorRegistry;
    private WebServer webserver;
    private final int port;

    public OnElectionAction(ServiceRegistry workerRegistry, ServiceRegistry coordinatorRegistry, int port) {
        this.workerRegistry = workerRegistry;
        this.coordinatorRegistry = coordinatorRegistry;
        this.port = port;
    }


    @Override
    public void onElectedToBeLeader() throws KeeperException, InterruptedException {
        workerRegistry.unregisterFromCluster();
        workerRegistry.registerForUpdates();

        if(webserver != null) {
            webserver.stop();
        }

        SearchCoordinator searchCoordinator = new SearchCoordinator();
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
