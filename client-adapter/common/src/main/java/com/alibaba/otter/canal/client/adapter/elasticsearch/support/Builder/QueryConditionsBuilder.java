package com.alibaba.otter.canal.client.adapter.elasticsearch.support.Builder;

import com.alibaba.otter.canal.client.adapter.elasticsearch.support.EntitySearchCondition;
import com.alibaba.otter.canal.client.adapter.elasticsearch.support.QueryFieldType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * TODO: 类描述
 *
 * @author leizheng4
 * @date 2019/5/28 11:26
 */
public class QueryConditionsBuilder {

    public BoolQueryBuilder queryBuilder;
    public List<EntitySearchCondition> list;

    /**
     * 构建一个通配符Wildcard的查询条件
     * <pre>
     *      mappings 中的field字段为 keyword 类型, 搜索内容为*xxx* 模糊搜索
     * </pre>
     *
     * @param searchMap
     * @return
     */
    public QueryConditionsBuilder buildWildcardSearch(Map<String, Object> searchMap) {
        if (searchMap == null) {
            return null;
        }
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        Iterator iterator = searchMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            //字段
            String filed = entry.getKey().toString();
            //值
            Object value = entry.getValue();
            boolQueryBuilder.filter(QueryBuilders.wildcardQuery(filed, "*" + value.toString() + "*"));
        }
        queryBuilder = boolQueryBuilder;
        return this;
    }


    /**
     * 构建一个match的查询条件
     * <pre>
     *    mappings 中的field字段为 text 类型，则为默认分词 搜索为模糊搜索
     * </pre>
     *
     * @param searchMap
     * @return
     */
    public QueryConditionsBuilder buildMatchSearch(Map<String, Object> searchMap) {
        if (searchMap == null) {
            return null;
        }
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        Iterator iterator = searchMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            //字段
            String filed = entry.getKey().toString();
            //值
            Object value = entry.getValue();
            boolQueryBuilder.filter(QueryBuilders.matchQuery(filed, value));
        }
        queryBuilder = boolQueryBuilder;
        return this;
    }

    /**
     * 构建一个Term的查询条件
     *
     * @param searchMap
     * @return
     */
    public QueryConditionsBuilder buildTermSearch(Map<String, Object> searchMap) {
        if (searchMap == null) {
            return null;
        }
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        Iterator iterator = searchMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            //字段
            String filed = entry.getKey().toString();
            //值
            Object value = entry.getValue();
            boolQueryBuilder.filter(QueryBuilders.termQuery(filed, value));
        }
        queryBuilder = boolQueryBuilder;
        return this;
    }


    /**
     * 多重关系查询构建
     *
     * @param list 查询实体
     * @return QueryConditionsBuilder查询实体
     */
    public QueryConditionsBuilder buildComplexSearch(List<EntitySearchCondition> list) {
        if (org.springframework.util.CollectionUtils.isEmpty(list)) {
            return null;
        }
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        list.forEach(x -> {
            String fieldType = x.getQueryFieldType().getFieldType();
            Map<String, Object> searchMap = x.getSearchMap();
            switch (fieldType) {
                //字段查询为 and
                case "EQUALS":
                    AndBoolQueryBuilder(boolQueryBuilder, searchMap, x);
                    break;
                //字段查询为 like
                case "LIKE":
                    MathBoolQueryBuilder(boolQueryBuilder, searchMap, x);
                    break;
                //采用中文分词 ngrm term方式
                case "NGRAM":
                    AndBoolQueryBuilder(boolQueryBuilder, searchMap, x);
                    break;
                //采用Wildcard方式,右模糊
                case "RLIKE":
                    WildcardBoolSearchBuilder(boolQueryBuilder, searchMap, x,QueryFieldType.RLIKE);
                    break;
                //采用Wildcard方式,左模糊
                case "LLIKE":
                    WildcardBoolSearchBuilder(boolQueryBuilder, searchMap, x,QueryFieldType.LLIKE);
                    break;
                case "EXIST":
                    ExistBoolQueryBuilder(boolQueryBuilder,searchMap,x);
                    break;
                case "IN":
                    InBoolQueryBuilder(boolQueryBuilder,searchMap,x);
                    break;
                case "IKSMART":
                    IKSmartQueryBuilder(boolQueryBuilder,searchMap,x);
                    break;
                default:
                    break;
            }
        });
        queryBuilder = boolQueryBuilder;
        return this;
    }

    /**
     * 精确查询构建
     *
     * @param boolQueryBuilder      dml 查询条件
     * @param searchMap             查询内容
     * @param entitySearchCondition 查询实体
     */
    private void AndBoolQueryBuilder(BoolQueryBuilder boolQueryBuilder, Map<String, Object> searchMap, EntitySearchCondition entitySearchCondition) {
        if (searchMap == null) {
            return;
        }
        Iterator iterator = searchMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            //字段
            String filed = entry.getKey().toString();
            //值
            Object value = entry.getValue();
            //判断是否使用filter、should
            if (entitySearchCondition.getOperator() == Operator.AND) {
                boolQueryBuilder.filter(QueryBuilders.termQuery(filed, value));
            } else {
                boolQueryBuilder.should(QueryBuilders.termQuery(filed, value));
            }
        }
    }


    /**
     * Exist查询构建
     *
     * @param boolQueryBuilder      dml 查询条件
     * @param searchMap             查询内容
     * @param entitySearchCondition 查询实体
     */
    private void ExistBoolQueryBuilder(BoolQueryBuilder boolQueryBuilder, Map<String, Object> searchMap, EntitySearchCondition entitySearchCondition) {
        if (searchMap == null) {
            return;
        }
        Iterator iterator = searchMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            //字段
            String filed = entry.getKey().toString();
            //判断是否使用filter、should
            if (entitySearchCondition.getOperator() == Operator.AND) {
                boolQueryBuilder.filter(QueryBuilders.existsQuery(filed));
            } else {
                boolQueryBuilder.should(QueryBuilders.existsQuery(filed));
            }
        }
    }


    /**
     * In查询构建
     *
     * @param boolQueryBuilder      dml 查询条件
     * @param searchMap             查询内容 value是list
     * @param entitySearchCondition 查询实体
     */
    private void InBoolQueryBuilder(BoolQueryBuilder boolQueryBuilder, Map<String, Object> searchMap, EntitySearchCondition entitySearchCondition) {
        if (searchMap == null) {
            return;
        }
        Iterator iterator = searchMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            //字段
            String filed = entry.getKey().toString();
            //值
            List<Object> value = (List<Object>)entry.getValue();
            //判断是否使用filter、should
            if (entitySearchCondition.getOperator() == Operator.AND) {
                boolQueryBuilder.filter(QueryBuilders.termsQuery(filed,value));
            } else {
                boolQueryBuilder.should(QueryBuilders.termsQuery(filed,value));
            }
        }
    }


    /**
     * IK_SMART查询构建
     *
     * @param boolQueryBuilder      dml 查询条件
     * @param searchMap             查询内容 value是list
     * @param entitySearchCondition 查询实体
     */
    private void IKSmartQueryBuilder(BoolQueryBuilder boolQueryBuilder, Map<String, Object> searchMap, EntitySearchCondition entitySearchCondition) {
        if (searchMap == null) {
            return;
        }
        Iterator iterator = searchMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            //字段
            String filed = entry.getKey().toString();
            //值
            Object value = entry.getValue();
            //判断是否使用filter、should
            if (entitySearchCondition.getOperator() == Operator.AND) {
                boolQueryBuilder.must(QueryBuilders.matchQuery(filed,value).analyzer("ik_smart"));
            } else {
                boolQueryBuilder.should(QueryBuilders.matchQuery(filed,value).analyzer("ik_smart"));
            }
        }
    }

    /**
     * 模糊查询构建
     * <pre>
     *     mappings 中的field字段为 text 类型，则为默认分词 搜索为模糊搜索
     * </pre>
     *
     * @param boolQueryBuilder      dml 查询条件
     * @param searchMap             查询内容
     * @param entitySearchCondition 查询实体
     */
    private void MathBoolQueryBuilder(BoolQueryBuilder boolQueryBuilder, Map<String, Object> searchMap, EntitySearchCondition entitySearchCondition) {
        if (searchMap == null) {
            return;
        }
        Iterator iterator = searchMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            //字段
            String filed = entry.getKey().toString();
            //值
            Object value = entry.getValue();
            //判断是否使用filter、should
            if (entitySearchCondition.getOperator() == Operator.AND) {
                boolQueryBuilder.filter(QueryBuilders.matchQuery(filed, value));
            } else {
                boolQueryBuilder.should(QueryBuilders.matchQuery(filed,value));
            }
        }
    }


    /**
     *
     * 构建一个通配符Wildcard的查询条件
     * <pre>
     *      1、mappings 中的field字段为 keyword 类型, 默认为为*xxx* 模糊搜索
     *      2、根据queryFieldType类型判断为 左、右模糊
     * </pre>
     *
     * @param boolQueryBuilder
     * @param searchMap
     * @param entitySearchCondition
     * @param queryFieldType
     */
    private void WildcardBoolSearchBuilder(BoolQueryBuilder boolQueryBuilder, Map<String, Object> searchMap, EntitySearchCondition entitySearchCondition, QueryFieldType queryFieldType) {
        if (searchMap == null) {
            return;
        }
        Iterator iterator = searchMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry entry = (Map.Entry) iterator.next();
            //字段
            String filed = entry.getKey().toString();
            //值
            Object value = entry.getValue();
            String searchType=queryFieldType.getFieldType();
            //判断是否使用filter、should
            if (entitySearchCondition.getOperator() == Operator.AND) {
                switch (searchType) {
                    case "RLIKE":
                        //右模糊
                        boolQueryBuilder.filter(QueryBuilders.wildcardQuery(filed, value.toString() + "*"));
                        break;
                    case "LLIKE":
                        //左模糊
                        boolQueryBuilder.filter(QueryBuilders.wildcardQuery(filed, "*" + value.toString()));
                        break;

                }
            } else {
                switch (searchType) {
                    case "RLIKE":
                        //右模糊
                        boolQueryBuilder.should(QueryBuilders.wildcardQuery(filed, value.toString() + "*"));
                        break;
                    case "LLIKE":
                        //左模糊
                        boolQueryBuilder.should(QueryBuilders.wildcardQuery(filed, "*" + value.toString()));
                        break;

                }
            }
        }
    }



    public QueryCondition build() {
        return new QueryCondition(this);
    }
}
