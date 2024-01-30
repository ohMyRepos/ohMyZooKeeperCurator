package co.zhanglintc;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class CuratorTest {
    private CuratorFramework zkClient;

    private String getNodeString(String path) throws Exception {
        byte[] data = zkClient.getData().forPath(path);
        return new String(data);
    }

    @Before
    public void crateClient() throws Exception {
        zkClient = CuratorFrameworkFactory.builder()
                .connectString("localhost:2181")
                .namespace("zhanglintc")
                .retryPolicy(new RetryNTimes(3, 1000))
                .build();
        zkClient.start();
        List<String> children = zkClient.getChildren().forPath("/");
        for (String child : children) {
            child = "/" + child;
            zkClient.delete().deletingChildrenIfNeeded().forPath(child);
            System.out.printf("child %s deleted\n", child);
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
        byte[] data = zkClient.getData().forPath("/app");
        if (data == null) {
            System.out.println("data is real null");
        } else {
            System.out.println("data is string: " + new String(data));
        }
    }

    @Test
    public void testGetChildren() throws Exception {
        List<String> children = zkClient.getChildren().forPath("/");
        System.out.println(children);
    }

    @After
    public void closeClient() {
        if (zkClient != null) {
            zkClient.close();
        }
    }
}
