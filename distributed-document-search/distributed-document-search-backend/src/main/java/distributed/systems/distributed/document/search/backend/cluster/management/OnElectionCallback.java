package distributed.systems.distributed.document.search.backend.cluster.management;

import org.apache.zookeeper.KeeperException;

public interface OnElectionCallback {

    void onElectedToBeLeader() throws KeeperException, InterruptedException;
    void onWorker();
}
