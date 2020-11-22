package com.alibaba.otter.canal.example;

import java.util.List;

public class CanalMessage {
    private String database;
    private String tableName;
    private String operation;
    private List<CanalColumn> data;

    public List<CanalColumn> getData() {
        return data;
    }

    public void setData(List<CanalColumn> data) {
        this.data = data;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }

    public static class CanalColumn {
        private String name;
        private String value;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }
}
