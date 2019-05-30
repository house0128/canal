package com.alibaba.otter.canal.client.adapter.es.config;

import java.util.Objects;

/**
 * TODO: 类描述
 *
 * @author leizheng4
 * @date 2019/4/17 14:12
 */
public class RowKey {
    private String tableName;
    private String type;
    private String key;
    private int index;

    public RowKey(String tableName, String type, String key, int index) {
        this.tableName = tableName;
        this.type = type;
        this.key = key;
        this.index= index;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RowKey)) return false;
        RowKey rowKey = (RowKey) o;
        return index == rowKey.index &&
                Objects.equals(tableName, rowKey.tableName) &&
                Objects.equals(type, rowKey.type) &&
                Objects.equals(key, rowKey.key);
    }

    @Override
    public int hashCode() {

        return Objects.hash(tableName, type, key, index);
    }
}
