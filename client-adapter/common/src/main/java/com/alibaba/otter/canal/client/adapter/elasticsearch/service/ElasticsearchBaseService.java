package com.alibaba.otter.canal.client.adapter.elasticsearch.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.client.adapter.elasticsearch.Param;
import com.alibaba.otter.canal.client.adapter.elasticsearch.support.Builder.QueryCondition;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import javax.swing.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * TODO: 类描述
 *
 * @author leizheng4
 * @date 2019/5/28 11:14
 */
public class ElasticsearchBaseService {

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchBaseService.class);
    @Resource
    private TransportClient transportClient;

    /**
     * es 通用查询
     *
     * @param index          索引
     * @param type           type 名
     * @param queryCondition 构建BoolQueryBuilder
     * @param sortFiled      排序字段
     * @param sort           排序
     * @param entityClass    实体
     * @param param          分页参数(兼容 basis 分页实体)
     * @param <T>
     * @return
     */
    public <T> List<T> search(String index, String type,
                              QueryCondition queryCondition,
                              String sortFiled,
                              String sort,
                              Class<T> entityClass, Param param) {

        if (!StringUtils.isNotEmpty(index) || !StringUtils.isNotEmpty(type)) {
            return null;
        }

        // 构建查询条件
        QueryBuilder queryBuilder;
        SearchResponse searchResponse;
        queryBuilder = queryCondition.getQueryBuilder();
        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(index).setTypes(type)
                .setSearchType(SearchType.DEFAULT)
                .setFrom(param.getPageIndex() - 1)
                .setSize(param.getLimit());
        if (StringUtils.isNotEmpty(sortFiled)) {
            if (sort.toUpperCase().equals("ASC")) {
                searchRequestBuilder.addSort(sortFiled, SortOrder.ASC);
            } else {
                searchRequestBuilder.addSort(sortFiled, SortOrder.DESC);
            }
        }
        searchResponse = searchRequestBuilder
                .setQuery(queryBuilder)
                .execute().actionGet();
        SearchHits hits = searchResponse.getHits();
        List<T> list = new ArrayList<T>();
        for (SearchHit hit : hits) {
            T o = JSON.parseObject(hit.getSourceAsString(), entityClass);
            list.add(o);
        }
        return list;

    }


    /**
     * es 查询请求返回 List<Map>
     *
     * @param index          索引
     * @param type           type 名
     * @param queryCondition 构建BoolQueryBuilder
     * @param sortFiled      排序字段
     * @param sort           排序
     * @param param          分页参数(兼容 basis 分页实体)
     * @return List<Map<String, Object>>
     */
    public List<Map<String, Object>> search(String index, String type,
                                            QueryCondition queryCondition,
                                            String sortFiled,
                                            String sort,
                                            Param param) {

        if (!StringUtils.isNotEmpty(index) || !StringUtils.isNotEmpty(type)) {
            return null;
        }

        // 构建查询条件
        QueryBuilder queryBuilder;
        SearchResponse searchResponse;
        queryBuilder = queryCondition.getQueryBuilder();
        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(index).setTypes(type)
                .setSearchType(SearchType.DEFAULT)
                .setFrom(param.getPageIndex() - 1)
                .setSize(param.getLimit());
        if (StringUtils.isNotEmpty(sortFiled)) {
            if (sort.toUpperCase().equals("ASC")) {
                searchRequestBuilder.addSort(sortFiled, SortOrder.ASC);
            } else {
                searchRequestBuilder.addSort(sortFiled, SortOrder.DESC);
            }
        }
        searchResponse = searchRequestBuilder
                .setQuery(queryBuilder)
                .execute().actionGet();

        //System.out.println(searchRequestBuilder.toString());
        SearchHits hits = searchResponse.getHits();
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        for (SearchHit hit : hits) {
            Map<String, Object> o = hit.getSourceAsMap();
            list.add(o);
        }
        return list;

    }


    /**
     * es 查询请求返回 List<Map>
     *
     * @param index          索引
     * @param type           type 名
     * @param queryCondition 构建BoolQueryBuilder
     * @param sortFiled      排序字段
     * @param sort           排序
     * @param param          分页参数(兼容 basis 分页实体)
     * @return List<Map<String, Object>>
     */
    public List<Map<String, Object>> search(String index, String type,
                                            QueryCondition queryCondition,
                                            String[] excluedFields,
                                            String sortFiled,
                                            String sort,
                                            Param param) {

        if (!StringUtils.isNotEmpty(index) || !StringUtils.isNotEmpty(type)) {
            return null;
        }

        // 构建查询条件
        QueryBuilder queryBuilder;
        SearchResponse searchResponse;
        queryBuilder = queryCondition.getQueryBuilder();
        SearchRequestBuilder searchRequestBuilder = transportClient.prepareSearch(index).setTypes(type)
                .setSearchType(SearchType.DEFAULT)
                .setFrom(param.getPageIndex() - 1)
                .setSize(param.getLimit());
        if (StringUtils.isNotEmpty(sortFiled)) {
            if (sort.toUpperCase().equals("ASC")) {
                searchRequestBuilder.addSort(sortFiled, SortOrder.ASC);
            } else {
                searchRequestBuilder.addSort(sortFiled, SortOrder.DESC);
            }
        }
        searchResponse = searchRequestBuilder
                .setFetchSource(excluedFields,null)
                .setQuery(queryBuilder)
                .execute().actionGet();
        SearchHits hits = searchResponse.getHits();
        List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
        for (SearchHit hit : hits) {
            Map<String, Object> o = hit.getSourceAsMap();
            list.add(o);
        }
        return list;

    }
}
