package com.alibaba.otter.canal.client.adapter.elasticsearch.support;

import org.elasticsearch.index.query.Operator;

import java.util.Map;

/**
 * TODO: 类描述
 *
 * @author leizheng4
 * @date 2019/5/28 11:17
 */
public class EntitySearchCondition {
    private Map<String, Object> searchMap;//查询条件 k->v
    private Operator operator;  //上下语句关系 and or
    private QueryFieldType queryFieldType; //字段与查询内容关系 equals like 等等

    public Map<String, Object> getSearchMap() {
        return searchMap;
    }

    public void setSearchMap(Map<String, Object> searchMap) {
        this.searchMap = searchMap;
    }

    public Operator getOperator() {
        return operator;
    }

    public void setOperator(Operator operator) {
        this.operator = operator;
    }

    public QueryFieldType getQueryFieldType() {
        return queryFieldType;
    }

    public void setQueryFieldType(QueryFieldType queryFieldType) {
        this.queryFieldType = queryFieldType;
    }
}
