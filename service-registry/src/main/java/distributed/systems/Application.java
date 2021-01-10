package distributed.systems;

import distributed.systems.leader.election.LeaderElection;
import distributed.systems.service.registry.ServiceRegistry;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Random;

public class Application implements Watcher {

    private static ZooKeeper zooKeeper;
    private static LeaderElection leader;
    private static ServiceRegistry service;
    private static OnElectionCallback onElectionCallback;
    private static final int SESSION_TIMEOUT = 3000;
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        Application application = new Application();
        application.connect();
        service = new ServiceRegistry(zooKeeper);
        int random = (int)(new Random().nextDouble()*1000);
        onElectionCallback = new OnElectionAction(service, random);
        leader = new LeaderElection(zooKeeper,onElectionCallback);
        leader.volunteerForLeadership();
        run();

    }

    private void connect() throws IOException {
        zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    public static void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
            close();
        }
    }

    public static void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getType() == Event.EventType.None) {
            if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
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
