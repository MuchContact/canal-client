package com.alibaba.otter.canal.example;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;

import java.net.InetSocketAddress;

/**
 * 单机模式的测试例子
 *
 * @author jianghang 2013-4-15 下午04:19:20
 * @version 1.0.4
 */
public class SimpleCanalClientApp extends AbstractCanalClient {

    public SimpleCanalClientApp(String destination) {
        super(destination);
    }

    public static void main(String args[]) {
        // 根据ip，直接创建链接，无HA的功能
        String destination = "example";
        String ip = "localhost";
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(ip, 11111),
                destination,
                "root",
                "test");

        final SimpleCanalClientApp clientTest = new SimpleCanalClientApp(destination);
        clientTest.setConnector(connector);
        clientTest.setListener(new CanalListener() {

            @Override
            public void onMessage(CanalMessage entry) {
                System.out.println(JSON.toJSONString(entry));
            }
        });
        clientTest.start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                logger.info("## stop the canal client");
                clientTest.stop();
            } catch (Throwable e) {
                logger.warn("##something goes wrong when stopping canal:", e);
            } finally {
                logger.info("## canal client is down.");
            }
        }));
    }

}
