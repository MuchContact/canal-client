package com.alibaba.otter.canal.example;

import com.alibaba.otter.canal.protocol.CanalEntry;
import org.apache.commons.lang.StringUtils;
import org.slf4j.MDC;
import org.springframework.util.Assert;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * 测试基类
 *
 * @author jianghang 2013-4-15 下午04:17:12
 * @version 1.0.4
 */
public class AbstractCanalClientTest extends BaseCanalClientTest {

    private CanalListener canalListener;

    public AbstractCanalClientTest(String destination) {
        this(destination, null);
    }

    public AbstractCanalClientTest(String destination, CanalConnector connector) {
        this.destination = destination;
        this.connector = connector;
    }

    protected void start() {
        Assert.notNull(connector, "connector is null");
        thread = new Thread(this::process);

        thread.setUncaughtExceptionHandler(handler);
        running = true;
        thread.start();
    }

    protected void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }

        MDC.remove("destination");
    }

    protected void process() {
        int batchSize = 5 * 1024;
        while (running) {
            try {
                MDC.put("destination", destination);
                connector.connect();
                connector.subscribe();
                while (running) {
                    Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
                    long batchId = message.getId();
                    int size = message.getEntries().size();
                    if (batchId == -1 || size == 0) {
                        // try {
                        // Thread.sleep(1000);
                        // } catch (InterruptedException e) {
                        // }
                    } else {
//                        printSummary(message, batchId, size);
//                        printEntry(message.getEntries());
                        for (CanalEntry.Entry entry : message.getEntries()) {
                            if (canalListener != null) {
                                CanalMessage canlMessage = convert2Message(entry);
                                canalListener.onMessage(canlMessage);
                            }
                        }
                    }

                    if (batchId != -1) {
                        connector.ack(batchId); // 提交确认
                    }
                }
            } catch (Throwable e) {
                logger.error("process error!", e);
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e1) {
                    // ignore
                }

                connector.rollback(); // 处理失败, 回滚数据
            } finally {
                connector.disconnect();
                MDC.remove("destination");
            }
        }
    }

    private CanalMessage convert2Message(CanalEntry.Entry entry) {
        CanalMessage canalMessage = new CanalMessage();
        canalMessage.setDatabase(entry.getHeader().getSchemaName());
        canalMessage.setTableName(entry.getHeader().getTableName());
        CanalEntry.RowChange rowChage = null;
        try {
            rowChage = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
        } catch (Exception e) {
            throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
        }

        CanalEntry.EventType eventType = rowChage.getEventType();
        canalMessage.setOperation(eventType.toString());
        List<CanalMessage.CanalColumn> canalColumns = new ArrayList<>();
        for (CanalEntry.RowData rowData : rowChage.getRowDatasList()) {
            if (eventType == CanalEntry.EventType.DELETE) {
                appendColumn(canalColumns, rowData.getBeforeColumnsList());
            } else if (eventType == CanalEntry.EventType.INSERT) {
                appendColumn(canalColumns, rowData.getAfterColumnsList());
            } else {
                appendColumn(canalColumns, rowData.getAfterColumnsList());
            }
        }
        canalMessage.setData(canalColumns);
        return canalMessage;
    }

    private void appendColumn(List<CanalMessage.CanalColumn> canalColumns, List<CanalEntry.Column> columns) {
        for (CanalEntry.Column column : columns) {
            try {
                if (StringUtils.containsIgnoreCase(column.getMysqlType(), "BLOB")
                        || StringUtils.containsIgnoreCase(column.getMysqlType(), "BINARY")) {
                    CanalMessage.CanalColumn e = new CanalMessage.CanalColumn();
                    e.setName(column.getName());
                    e.setValue(new String(column.getValue().getBytes("ISO-8859-1"), "UTF-8"));
                    canalColumns.add(e);
                } else {
                    CanalMessage.CanalColumn e = new CanalMessage.CanalColumn();
                    e.setName(column.getName());
                    e.setValue(column.getValue());
                    canalColumns.add(e);
                }
            } catch (UnsupportedEncodingException e) {
            }
        }
    }

    protected void setListener(CanalListener canalListener) {
        this.canalListener = canalListener;
    }
}
