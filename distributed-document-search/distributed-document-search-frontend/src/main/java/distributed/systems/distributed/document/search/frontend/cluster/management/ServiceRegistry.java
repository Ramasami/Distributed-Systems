package distributed.systems.distributed.document.search.frontend.cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class ServiceRegistry implements Watcher {

    public static final String WORKER = "/worker";
    public static final String COORDINATOR = "/coordinator";
    private final String registryType;
    private ZooKeeper zooKeeper;
    private String currentZnodeName;
    private List<String> allServiceAddresses = null;

    public ServiceRegistry(ZooKeeper zooKeeper, String registryType) {
        this.zooKeeper = zooKeeper;
        this.registryType = registryType;
        createServiceRegistryNode();
    }

    private void createServiceRegistryNode() {
        try {
            if (zooKeeper.exists(registryType, false) == null) {
                zooKeeper.create(registryType, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public synchronized List<String> getAllServiceAddresses() throws KeeperException, InterruptedException {
        if(allServiceAddresses == null)
            updateAddresses();
        return allServiceAddresses;
    }

    public void registerForUpdates() {
        try {
            updateAddresses();
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void registerToCluster(String metadata) throws KeeperException, InterruptedException {
        if(currentZnodeName != null)
                return;
        currentZnodeName = zooKeeper.create(registryType +"/n_",metadata.getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Registered to service registry as " + (currentZnodeName.replace(registryType +"/","")));
    }

    public void unregisterFromCluster() throws KeeperException, InterruptedException {
        if(currentZnodeName != null && zooKeeper.exists(currentZnodeName,false) != null)
            zooKeeper.delete(currentZnodeName,-1);
    }

    private synchronized  void updateAddresses() throws KeeperException, InterruptedException {
        List<String> workerZnodes = zooKeeper.getChildren(registryType, this);
        List<String> addresses = new ArrayList<>(workerZnodes.size());
        for(String workZnode : workerZnodes) {
            String workerZnodeFullPath = registryType + "/" + workZnode;
            Stat stat = zooKeeper.exists(workerZnodeFullPath, false);
            if(stat == null)
                continue;
            byte[] addressBytes = zooKeeper.getData(workerZnodeFullPath, false,stat);
            String address = new String(addressBytes);
            addresses.add(address);
        }
        allServiceAddresses = Collections.unmodifiableList(addresses);
        System.out.println("The cluster addresses are : " + allServiceAddresses);
    }

    public String getRandomServiceAddress() throws KeeperException, InterruptedException {
        List<String> list = getAllServiceAddresses();
        if(list.size()==0)
            return null;
        int r = new Random().nextInt()%list.size();
        return list.get(r);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        try {
            updateAddresses();
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }
}
