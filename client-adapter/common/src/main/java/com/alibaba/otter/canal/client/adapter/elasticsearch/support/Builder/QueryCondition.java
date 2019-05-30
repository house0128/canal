package com.alibaba.otter.canal.client.adapter.elasticsearch.support.Builder;

import com.alibaba.otter.canal.client.adapter.elasticsearch.support.EntitySearchCondition;
import org.elasticsearch.index.query.BoolQueryBuilder;

import java.util.List;

/**
 * TODO: 类描述
 *
 * @author leizheng4
 * @date 2019/5/28 11:25
 */
public class QueryCondition {
    private BoolQueryBuilder queryBuilder;
    private List<EntitySearchCondition> list;

    public BoolQueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    public void setQueryBuilder(BoolQueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
    }

    public QueryCondition(QueryConditionsBuilder builder) {
        queryBuilder = builder.queryBuilder;
        list = builder.list;
    }

    public List<EntitySearchCondition> getList() {
        return list;
    }

    public void setList(List<EntitySearchCondition> list) {
        this.list = list;
    }
}
