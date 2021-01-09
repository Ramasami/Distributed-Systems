package distributed.systems.leader.election;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ZK2_ElectAsLeader implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final String ELECTION_NAMESPACE = "/election";
    private static final int SESSION_TIMEOUT = 3000;
    private ZooKeeper zooKeeper;
    private String currentZnodeName;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZK2_ElectAsLeader electAsLeader = new ZK2_ElectAsLeader();
        electAsLeader.connectToZookeeper();
        electAsLeader.volunteerForLeadership();
        electAsLeader.electLeader();
        electAsLeader.run();
        electAsLeader.close();
        System.out.println("Disconnected from Zookeeper, exiting application");
    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
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
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None:
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

    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("znode name " + znodeFullPath);
        this.currentZnodeName = znodeFullPath.replace(ELECTION_NAMESPACE + "/", "");

    }

    public void electLeader() throws KeeperException, InterruptedException {
        List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);

        Collections.sort(children);
        String smallestChild = children.get(0);
        if (smallestChild.equals(currentZnodeName)) {
            System.out.println("I am the leader");
        } else {
            System.out.println("I am not the leader, " + smallestChild + " is the leader");
        }
    }
}
