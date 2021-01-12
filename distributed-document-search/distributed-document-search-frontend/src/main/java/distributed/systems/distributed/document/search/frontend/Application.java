package distributed.systems.distributed.document.search.frontend;

import distributed.systems.distributed.document.search.frontend.cluster.management.ServiceRegistry;
import distributed.systems.distributed.document.search.frontend.networking.WebServer;
import distributed.systems.distributed.document.search.frontend.search.UserSearchHandler;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public class Application implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private ZooKeeper zooKeeper;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        int currentServerPort = 9000;
        if (args.length == 1) {
            currentServerPort = Integer.parseInt(args[0]);
        }
        Application application = new Application();
        ZooKeeper zooKeeper = application.connectToZookeeper();

        ServiceRegistry coordinatorsService = new ServiceRegistry(zooKeeper, ServiceRegistry.COORDINATOR);

        UserSearchHandler searchHandler = new UserSearchHandler(coordinatorsService);
        WebServer webServer = new WebServer(currentServerPort, searchHandler);
        webServer.startServer();

        System.out.println("Server is listening on port " + currentServerPort);

        application.run();
        application.close();
        System.out.println("Disconnected from Zookeeper, exiting application");
    }

    public ZooKeeper connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
        return zooKeeper;
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to Zookeeper");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from Zookeeper event");
                        zooKeeper.notifyAll();
                    }
                }
        }
    }
}
