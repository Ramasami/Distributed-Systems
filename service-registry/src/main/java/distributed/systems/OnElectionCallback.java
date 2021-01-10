package distributed.systems;

import org.apache.zookeeper.KeeperException;

public interface OnElectionCallback {

    void onElectedToBeLeader() throws KeeperException, InterruptedException;
    void onWorker();
}
