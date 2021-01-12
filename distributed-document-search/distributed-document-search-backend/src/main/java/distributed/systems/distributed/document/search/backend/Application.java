package distributed.systems.distributed.document.search.backend;

import distributed.systems.distributed.document.search.backend.cluster.management.LeaderElection;
import distributed.systems.distributed.document.search.backend.cluster.management.OnElectionCallback;
import distributed.systems.distributed.document.search.backend.cluster.management.ServiceRegistry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.net.URISyntaxException;

public class Application implements Watcher {
    private static ZooKeeper zooKeeper;
    private static final int SESSION_TIMEOUT = 3000;
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException, URISyntaxException {
        int currentServerPort = 8092;
        if (args.length == 1) {
            currentServerPort = Integer.parseInt(args[0]);
        }
        Application application = new Application();
        application.connectToZookeeper();

        ServiceRegistry workerService = new ServiceRegistry(zooKeeper, ServiceRegistry.WORKER);
        ServiceRegistry coordinatorService = new ServiceRegistry(zooKeeper,ServiceRegistry.COORDINATOR);
        OnElectionCallback onElectionCallback = new OnElectionAction(workerService, coordinatorService, currentServerPort);

        LeaderElection leader = new LeaderElection(zooKeeper, onElectionCallback);
        leader.volunteerForLeadership();
        leader.reelectLeader();

        application.run();

    }

    private void connectToZookeeper() throws IOException {
        zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    private void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
            close();
        }
    }

    private void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Watcher.Event.EventType.None) {
            if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
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
