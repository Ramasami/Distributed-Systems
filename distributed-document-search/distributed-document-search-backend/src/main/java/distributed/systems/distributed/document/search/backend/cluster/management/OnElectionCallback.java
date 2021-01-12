package distributed.systems.distributed.document.search.backend.cluster.management;

import org.apache.zookeeper.KeeperException;

import java.net.URISyntaxException;

public interface OnElectionCallback {

    void onElectedToBeLeader() throws KeeperException, InterruptedException, URISyntaxException;
    void onWorker();
}
