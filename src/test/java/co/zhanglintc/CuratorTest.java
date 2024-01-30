package co.zhanglintc;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.data.Stat;
import org.junit.*;

import java.util.List;

public class CuratorTest {
    private static CuratorFramework zkClient;

    private String getNodeString(String path) throws Exception {
        byte[] data = zkClient.getData().forPath(path);
        return new String(data);
    }

    @BeforeClass
    public static void crateClient() throws Exception {
        zkClient = CuratorFrameworkFactory.builder()
                .connectString("localhost:2181")
                .namespace("zhanglintc")
                .retryPolicy(new RetryNTimes(3, 1000))
                .build();
        zkClient.start();
    }

    @Before
    public void cleanUp() throws Exception {
        List<String> children = zkClient.getChildren().forPath("/");
        for (String child : children) {
            child = "/" + child;
            zkClient.delete().guaranteed().deletingChildrenIfNeeded().forPath(child);
            System.out.printf("cleanUp: child %s deleted\n", child);
        }
    }

    @Test
    public void testCreateNode() throws Exception {
        String app1 = "/app1";
        String returned1 = zkClient.create().forPath(app1);
        String nodeData1 = getNodeString(app1);
        System.out.printf("path is: %s, data is: %s\n", returned1, nodeData1);

        String app2 = "/app2";
        String returned2 = zkClient.create().forPath(app2, "app2".getBytes());
        String nodeData2 = getNodeString(app2);
        System.out.printf("path is: %s, data is: %s\n", returned2, nodeData2);

        String app34 = "/app3/app4";
        String returned4 = zkClient.create().creatingParentsIfNeeded().forPath(app34, app34.getBytes());
        String nodeData3 = getNodeString("/app3");
        String nodeData4 = getNodeString(app34);
        System.out.printf("path is: %s, data3 is: '%s', data4 is: '%s'\n", returned4, nodeData3, nodeData4);
    }

    @Test
    public void testGetData() throws Exception {
        String app = "/app";
        zkClient.create().forPath(app);
        zkClient.setData().forPath(app, "null".getBytes());
        zkClient.setData().forPath(app, null);
        byte[] data = zkClient.getData().forPath(app);
        if (data == null) {
            System.out.println("data is real null");
        } else {
            System.out.println("data is string: " + new String(data));
        }

        Stat stat = new Stat();
        byte[] bytes = zkClient.getData().storingStatIn(stat).forPath(app);
        System.out.println(stat);
    }

    @Test
    public void testGetChildren() throws Exception {
        zkClient.create().forPath("/app1");
        zkClient.create().forPath("/app2");
        zkClient.create().forPath("/app3");
        List<String> children = zkClient.getChildren().forPath("/");
        System.out.println(children);
    }

    @Test
    public void testNodeCache() throws Exception {
        String app = "/app";
        NodeCache nodeCache = new NodeCache(zkClient, app);
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                byte[] data = nodeCache.getCurrentData().getData();
                String dataStr;
                if (data == null) {
                    dataStr = "real null";
                } else {
                    dataStr = new String(data);
                }
                System.out.printf(" => node changed. current: '%s'\n", dataStr);
            }
        });
        nodeCache.start(true);
        System.out.println("create");
        zkClient.create().forPath(app);
        Thread.sleep(1000);
        System.out.println("set real null");
        zkClient.setData().forPath(app, null);
        Thread.sleep(1000);
        System.out.println("set real null again");
        zkClient.setData().forPath(app, null);
        Thread.sleep(1000);
        System.out.println("set real string null");
        zkClient.setData().forPath(app, "null".getBytes());
        Thread.sleep(1000);
        System.out.println("set real string null again");
        zkClient.setData().forPath(app, "null".getBytes());
        Thread.sleep(1000);
    }

    @AfterClass
    public static void closeClient() {
        if (zkClient != null) {
            zkClient.close();
        }
    }
}
