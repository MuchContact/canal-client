package com.alibaba.otter.canal.example;

import com.alibaba.otter.canal.protocol.CanalEntry;

public interface CanalListener {
    void onMessage(CanalMessage msg);
}
