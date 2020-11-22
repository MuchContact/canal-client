package com.alibaba.otter.canal.example;


public interface CanalListener {
    void onMessage(CanalMessage msg);
}
