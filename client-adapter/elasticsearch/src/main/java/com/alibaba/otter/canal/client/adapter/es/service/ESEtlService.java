package com.alibaba.otter.canal.client.adapter.es.service;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.config.SchemaItem.FieldItem;
import com.alibaba.otter.canal.client.adapter.es.support.ESSyncUtil;
import com.alibaba.otter.canal.client.adapter.es.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.support.DatasourceConfig;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.google.common.base.Joiner;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ES ETL Service
 *
 * @author rewerma 2018-11-01
 * @version 1.0.0
 */
public class ESEtlService {

    private static Logger   logger = LoggerFactory.getLogger(ESEtlService.class);

    private TransportClient transportClient;
    private ESTemplate      esTemplate;
    private ESSyncConfig    config;

    public ESEtlService(TransportClient transportClient, ESSyncConfig config){
        this.transportClient = transportClient;
        this.esTemplate = new ESTemplate(transportClient);
        this.config = config;
    }

    public EtlResult importData(List<String> params, boolean bulk) {
        EtlResult etlResult = new EtlResult();
        AtomicLong impCount = new AtomicLong();
        List<String> errMsg = new ArrayList<>();
        String esIndex = "";
        if (config == null) {
            logger.warn("esSycnCofnig is null, etl go end !");
            etlResult.setErrorMessage("esSycnCofnig is null, etl go end !");
            return etlResult;
        }

        ESMapping mapping = config.getEsMapping();

        esIndex = mapping.get_index();
        DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        Pattern pattern = Pattern.compile(".*:(.*)://.*/(.*)\\?.*$");
        Matcher matcher = pattern.matcher(dataSource.getUrl());
        if (!matcher.find()) {
            throw new RuntimeException("Not found the schema of jdbc-url: " + config.getDataSourceKey());
        }
        String schema = matcher.group(2);

        logger.info("etl from db: {},  to es index: {}", schema, esIndex);
        long start = System.currentTimeMillis();
        try {
            String sql = mapping.getSql();

            // 拼接条件
            if (mapping.getEtlCondition() != null && params != null) {
                String etlCondition = mapping.getEtlCondition();
                int size = params.size();
                for (int i = 0; i < size; i++) {
                    etlCondition = etlCondition.replace("{" + i + "}", params.get(i));
                }

                sql += " " + etlCondition;
            }

            if (logger.isDebugEnabled()) {
                logger.debug("etl sql : {}", mapping.getSql());
            }

            if (bulk) {
                // 获取总数
                String countSql = "SELECT COUNT(1) FROM ( " + sql + ") _CNT ";
                long cnt = (Long) ESSyncUtil.sqlRS(dataSource, countSql, rs -> {
                    Long count = null;
                    try {
                        if (rs.next()) {
                            count = ((Number) rs.getObject(1)).longValue();
                        }
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                    return count == null ? 0L : count;
                });

                // 当大于1万条记录时开启多线程
                if (cnt >= 10000) {
                    int threadCount = 3; // TODO 从配置读取默认为3
                    long perThreadCnt = cnt / threadCount;
                    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
                    List<Future<Boolean>> futures = new ArrayList<>(threadCount);
                    for (int i = 0; i < threadCount; i++) {
                        long offset = i * perThreadCnt;
                        Long size = null;
                        if (i != threadCount - 1) {
                            size = perThreadCnt;
                        }
                        String sqlFinal;
                        if (size != null) {
                            sqlFinal = sql + " LIMIT " + offset + "," + size;
                        } else {
                            sqlFinal = sql + " LIMIT " + offset + "," + cnt;
                        }
                        Future<Boolean> future = executor
                                .submit(() -> executeSqlImport(dataSource, sqlFinal, mapping, impCount, errMsg));
                        futures.add(future);
                    }

                    for (Future<Boolean> future : futures) {
                        future.get();
                    }

                    executor.shutdown();
                } else {
                    executeSqlImport(dataSource, sql, mapping, impCount, errMsg);
                }
            } else {
                logger.info("自动ETL，无需统计记录总条数，直接进行ETL, index: {}", esIndex);
                executeSqlImport(dataSource, sql, mapping, impCount, errMsg);
            }

            logger.info("数据全量导入完成,一共导入 {} 条数据, 耗时: {}", impCount.get(), System.currentTimeMillis() - start);
            etlResult.setResultMessage("导入ES索引 " + esIndex + " 数据：" + impCount.get() + " 条");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            errMsg.add(esIndex + " etl failed! ==>" + e.getMessage());
        }
        if (errMsg.isEmpty()) {
            etlResult.setSucceeded(true);
        } else {
            etlResult.setErrorMessage(Joiner.on("\n").join(errMsg));
        }
        return etlResult;
    }

    private void processFailBulkResponse(BulkResponse bulkResponse, boolean hasParent) {
        for (BulkItemResponse response : bulkResponse.getItems()) {
            if (!response.isFailed()) {
                continue;
            }

            if (response.getFailure().getStatus() == RestStatus.NOT_FOUND) {
                logger.warn(response.getFailureMessage());
            } else {
                logger.error("全量导入数据有误 {}", response.getFailureMessage());
                throw new RuntimeException("全量数据 etl 异常: " + response.getFailureMessage());
            }
        }
    }

    private boolean executeSqlImport(DataSource ds, String sql, ESMapping mapping, AtomicLong impCount,
                                     List<String> errMsg) {
        try {
            ESSyncUtil.sqlRS(ds, sql, rs -> {
                int count = 0;
                try {
                    BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();

                    long batchBegin = System.currentTimeMillis();
                    while (rs.next()) {
                        Map<String, Object> esFieldData = new LinkedHashMap<>();
                        for (FieldItem fieldItem : mapping.getSchemaItem().getSelectFields().values()) {

                            // 如果是主键字段则不插入
                            if (fieldItem.getFieldName().equals(mapping.get_id())) {
                                continue;
                            }

                            String fieldName = fieldItem.getFieldName();
                            if (mapping.getSkips().contains(fieldName)) {
                                continue;
                            }

                            Object val = esTemplate.getValFromRS(mapping, rs, fieldName, fieldName);
                            esFieldData.put(fieldName, val);
                        }
                        Object idVal = null;
                        if (mapping.get_id() != null) {
                            //start-------2019.3.7  leizheng4修改--------
                            //idVal = rs.getObject(mapping.get_id());  idVal= BigInter 报错
                            idVal = esFieldData.get(mapping.get_id());
                            //end
                        }

                        if (idVal != null) {
                            if (mapping.getParent() == null) {
                                bulkRequestBuilder.add(transportClient
                                        .prepareIndex(mapping.get_index(), mapping.get_type(), idVal.toString())
                                        .setSource(esFieldData));
                            } else {
                                // ignore
                            }
                        } else {
                            //start-----2019.3.7 leizheng4修改
                            //idVal = rs.getObject(mapping.getPk());  idVal= BigInter 报错
                            idVal = esFieldData.get(mapping.getPk());
                            //end------------------------
                            if (mapping.getParent() == null) {
                                // 删除pk对应的数据
                                SearchResponse response = transportClient.prepareSearch(mapping.get_index())
                                        .setTypes(mapping.get_type())
                                        .setQuery(QueryBuilders.termQuery(mapping.getPk(), idVal))
                                        .get();
                                for (SearchHit hit : response.getHits()) {
                                    bulkRequestBuilder.add(transportClient
                                            .prepareDelete(mapping.get_index(), mapping.get_type(), hit.getId()));
                                }

                                bulkRequestBuilder
                                        .add(transportClient.prepareIndex(mapping.get_index(), mapping.get_type())
                                                .setSource(esFieldData));
                            } else {
                                // ignore
                            }
                        }

                        if (bulkRequestBuilder.numberOfActions() % mapping.getCommitBatch() == 0
                                && bulkRequestBuilder.numberOfActions() > 0) {
                            long esBatchBegin = System.currentTimeMillis();
                            BulkResponse rp = bulkRequestBuilder.execute().actionGet();
                            if (rp.hasFailures()) {
                                this.processFailBulkResponse(rp, Objects.nonNull(mapping.getParent()));
                            }

                            if (logger.isDebugEnabled()) {
                                logger.debug("全量数据批量导入批次耗时: {}, es执行时间: {}, 批次大小: {}, index; {}",
                                        (System.currentTimeMillis() - batchBegin),
                                        (System.currentTimeMillis() - esBatchBegin),
                                        bulkRequestBuilder.numberOfActions(),
                                        mapping.get_index());
                            }
                            batchBegin = System.currentTimeMillis();
                            bulkRequestBuilder = transportClient.prepareBulk();
                        }
                        count++;
                        impCount.incrementAndGet();
                    }

                    if (bulkRequestBuilder.numberOfActions() > 0) {
                        long esBatchBegin = System.currentTimeMillis();
                        BulkResponse rp = bulkRequestBuilder.execute().actionGet();
                        if (rp.hasFailures()) {
                            this.processFailBulkResponse(rp, Objects.nonNull(mapping.getParent()));
                        }
                        if (logger.isDebugEnabled()) {
                            logger.debug("全量数据批量导入最后批次耗时: {}, es执行时间: {}, 批次大小: {}, index; {}",
                                    (System.currentTimeMillis() - batchBegin),
                                    (System.currentTimeMillis() - esBatchBegin),
                                    bulkRequestBuilder.numberOfActions(),
                                    mapping.get_index());
                        }
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    errMsg.add(mapping.get_index() + " etl failed! ==>" + e.getMessage());
                    throw new RuntimeException(e);
                }
                return count;
            });

            return true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    //-----------2019.3.7 leizheng4修改-----------
    private boolean executeSqlImport(DataSource ds, String sql, ESMapping mapping, AtomicLong impCount,
                                     List<String> errMsg,boolean isNeedDelete) {
        try {
            ESSyncUtil.sqlRS(ds, sql, rs -> {
                int count = 0;
                try {
                    BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();

                    long batchBegin = System.currentTimeMillis();
                    while (rs.next()) {
                        Map<String, Object> esFieldData = new LinkedHashMap<>();
                        for (FieldItem fieldItem : mapping.getSchemaItem().getSelectFields().values()) {

                            String fieldName = fieldItem.getFieldName();
                            if (mapping.getSkips().contains(fieldName)) {
                                continue;
                            }

                            Object val = esTemplate.getValFromRS(mapping, rs, fieldName, fieldName);
                            esFieldData.put(fieldName, val);
                        }
                        Object idVal = null;
                        if (mapping.get_id() != null) {
                            //idVal = rs.getObject(mapping.get_id());  idVal= BigInter 报错
                            idVal = esFieldData.get(mapping.get_id());
                        }

                        if (idVal != null) {
                            if (mapping.getParent() == null) {
                                bulkRequestBuilder.add(transportClient
                                        .prepareIndex(mapping.get_index(), mapping.get_type(), idVal.toString())
                                        .setSource(esFieldData));
                            } else {
                                // ignore
                            }
                        } else {
                            //idVal = rs.getObject(mapping.getPk());  idVal= BigInter 报错
                            idVal = esFieldData.get(mapping.getPk());
                            if (mapping.getParent() == null) {
                                if(isNeedDelete){
                                    // 删除pk对应的数据
                                    SearchResponse response = transportClient.prepareSearch(mapping.get_index())
                                            .setTypes(mapping.get_type())
                                            .setQuery(QueryBuilders.termQuery(mapping.getPk(), idVal))
                                            .get();
                                    for (SearchHit hit : response.getHits()) {
                                        bulkRequestBuilder.add(transportClient
                                                .prepareDelete(mapping.get_index(), mapping.get_type(), hit.getId()));
                                    }
                                }

                                bulkRequestBuilder
                                        .add(transportClient.prepareIndex(mapping.get_index(), mapping.get_type())
                                                .setSource(esFieldData));
                            } else {
                                // ignore
                            }
                        }

                        if (bulkRequestBuilder.numberOfActions() % mapping.getCommitBatch() == 0
                                && bulkRequestBuilder.numberOfActions() > 0) {
                            long esBatchBegin = System.currentTimeMillis();
                            BulkResponse rp = bulkRequestBuilder.execute().actionGet();
                            if (rp.hasFailures()) {
                                this.processFailBulkResponse(rp, Objects.nonNull(mapping.getParent()));
                            }

                            if (logger.isDebugEnabled()) {
                                logger.debug("全量数据批量导入批次耗时: {}, es执行时间: {}, 批次大小: {}, index; {}",
                                        (System.currentTimeMillis() - batchBegin),
                                        (System.currentTimeMillis() - esBatchBegin),
                                        bulkRequestBuilder.numberOfActions(),
                                        mapping.get_index());
                            }
                            batchBegin = System.currentTimeMillis();
                            bulkRequestBuilder = transportClient.prepareBulk();
                        }
                        count++;
                        impCount.incrementAndGet();
                    }

                    if (bulkRequestBuilder.numberOfActions() > 0) {
                        long esBatchBegin = System.currentTimeMillis();
                        BulkResponse rp = bulkRequestBuilder.execute().actionGet();
                        if (rp.hasFailures()) {
                            this.processFailBulkResponse(rp, Objects.nonNull(mapping.getParent()));
                        }
                        if (logger.isDebugEnabled()) {
                            logger.debug("全量数据批量导入最后批次耗时: {}, es执行时间: {}, 批次大小: {}, index; {}",
                                    (System.currentTimeMillis() - batchBegin),
                                    (System.currentTimeMillis() - esBatchBegin),
                                    bulkRequestBuilder.numberOfActions(),
                                    mapping.get_index());
                        }
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                    errMsg.add(mapping.get_index() + " etl failed! ==>" + e.getMessage());
                    throw new RuntimeException(e);
                }
                return count;
            });

            return true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }

    //-----------2019.3.7 leizheng4修改-----------
    public EtlResult importDataSql(String sql,boolean isNeedDelete,boolean bulk) {
        EtlResult etlResult = new EtlResult();
        AtomicLong impCount = new AtomicLong();
        List<String> errMsg = new ArrayList<>();
        String esIndex = "";
        if (config == null) {
            logger.warn("esSycnCofnig is null, etl go end !");
            etlResult.setErrorMessage("esSycnCofnig is null, etl go end !");
            return etlResult;
        }

        ESMapping mapping = config.getEsMapping();

        esIndex = mapping.get_index();
        DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        Pattern pattern = Pattern.compile(".*:(.*)://.*/(.*)\\?.*$");
        Matcher matcher = pattern.matcher(dataSource.getUrl());
        if (!matcher.find()) {
            throw new RuntimeException("Not found the schema of jdbc-url: " + config.getDataSourceKey());
        }
        String schema = matcher.group(2);

        logger.info("etl from db: {},  to es index: {}", schema, esIndex);
        long start = System.currentTimeMillis();
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("etl sql : {}", sql);
            }

            if (bulk) {
                // 获取总数
                String countSql = "SELECT COUNT(1) FROM ( " + sql + ") _CNT ";
                long cnt = (Long) ESSyncUtil.sqlRS(dataSource, countSql, rs -> {
                    Long count = null;
                    try {
                        if (rs.next()) {
                            count = ((Number) rs.getObject(1)).longValue();
                        }
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                    return count == null ? 0L : count;
                });

                // 当大于1万条记录时开启多线程
                if (cnt >= 10000) {
//                    int threadCount = 3; // TODO 从配置读取默认为3
//                    long perThreadCnt = cnt / threadCount;
//                    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
//                    List<Future<Boolean>> futures = new ArrayList<>(threadCount);
//                    for (int i = 0; i < threadCount; i++) {
//                        long offset = i * perThreadCnt;
//                        Long size = null;
//                        if (i != threadCount - 1) {
//                            size = perThreadCnt;
//                        }
//                        String sqlFinal;
//                        if (size != null) {
//                            sqlFinal = sql + " LIMIT " + offset + "," + size;
//                        } else {
//                            sqlFinal = sql + " LIMIT " + offset + "," + cnt;
//                        }
//                        Future<Boolean> future = executor
//                            .submit(() -> executeSqlImport(dataSource, sqlFinal, mapping, impCount, errMsg));
//                        futures.add(future);
//                    }

                    //---------------修改后----------------
                    int pageCount =10000;
                    int threadCount = 10; // TODO 从配置读取
                    int perPageCnt = (int)(cnt / pageCount);//这里不会太大了  int 足矣
                    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
                    List<Future<Boolean>> futures = new ArrayList<>(perPageCnt);
                    for (int i = 0; i < perPageCnt; i++) {
                        long offset = i * pageCount;
                        Integer size = null;
                        if (i != perPageCnt - 1) {
                            size = pageCount;
                        }
                        String sqlFinal;
                        if (size != null) {
                            sqlFinal = sql + " LIMIT " + offset + "," + size;
                        } else {
                            sqlFinal = sql + " LIMIT " + offset + "," + cnt;
                        }
                        Future<Boolean> future = executor
                                .submit(() -> executeSqlImport(dataSource, sqlFinal, mapping, impCount, errMsg,isNeedDelete));
                        futures.add(future);
                    }

                    //---------------修改后----------------

                    for (Future<Boolean> future : futures) {
                        future.get();
                    }

                    executor.shutdown();
                } else {
                    executeSqlImport(dataSource, sql, mapping, impCount, errMsg,isNeedDelete);
                }
            } else {
                logger.info("自动ETL，无需统计记录总条数，直接进行ETL, index: {}", esIndex);
                executeSqlImport(dataSource, sql, mapping, impCount, errMsg,isNeedDelete);
            }

            logger.info("数据全量导入完成,一共导入 {} 条数据, 耗时: {}", impCount.get(), System.currentTimeMillis() - start);
            etlResult.setResultMessage("导入ES索引 " + esIndex + " 数据：" + impCount.get() + " 条");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            errMsg.add(esIndex + " etl failed! ==>" + e.getMessage());
        }
        if (errMsg.isEmpty()) {
            etlResult.setSucceeded(true);
        } else {
            etlResult.setErrorMessage(Joiner.on("\n").join(errMsg));
        }
        return etlResult;
    }



    //-----------2019.3.15 leizheng4修改-----------
    public EtlResult importDataSql(String sql,boolean isNeedDelete,String orderByParam) {
        EtlResult etlResult = new EtlResult();
        AtomicLong impCount = new AtomicLong();
        List<String> errMsg = new ArrayList<>();
        String esIndex = "";
        if (config == null) {
            logger.warn("esSycnCofnig is null, etl go end !");
            etlResult.setErrorMessage("esSycnCofnig is null, etl go end !");
            return etlResult;
        }

        ESMapping mapping = config.getEsMapping();

        esIndex = mapping.get_index();
        DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
        Pattern pattern = Pattern.compile(".*:(.*)://.*/(.*)\\?.*$");
        Matcher matcher = pattern.matcher(dataSource.getUrl());
        if (!matcher.find()) {
            throw new RuntimeException("Not found the schema of jdbc-url: " + config.getDataSourceKey());
        }
        String schema = matcher.group(2);

        logger.info("etl from db: {},  to es index: {}", schema, esIndex);
        long start = System.currentTimeMillis();
        ExecutorService executor = null;
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("etl sql : {}", sql);
            }

            // 直接开启多线程（数据量太大的情况，执行数目语句数据库开始）
            int fetchCount = 10000;
            boolean isFirst=true;
            long maxId = 0L;
            String readId = orderByParam;
            if(orderByParam.contains(".")) {
                readId=orderByParam.substring(orderByParam.indexOf(".")+1,orderByParam.length());
            }

            List<Future<Boolean>> futures = new ArrayList<>();
            int threadCount = 10; // TODO 从配置读取
            executor = Executors.newFixedThreadPool(threadCount);
            while(fetchCount>0)
            {
                String fetchSql ="";
                if(isFirst)
                {
                    fetchSql = sql + " order by "+orderByParam+" limit 0,10000";
                }else{
                    if(sql.toLowerCase().contains(" where "))
                    {
                        fetchSql = sql + " and " + orderByParam +">"+maxId +" order by "+orderByParam+" limit 0,10000";
                    }else{
                        fetchSql = sql + " where " + orderByParam +">"+maxId +" order by "+orderByParam+" limit 0,10000";
                    }
                }
                long startSql = System.currentTimeMillis();
                List<Map<String, Object>> listFileds = this.getListParamsByOrder(dataSource,fetchSql,mapping,errMsg);
                isFirst=false;
                fetchCount = listFileds.size();
                if(fetchCount!=0){
                    maxId = (Long)listFileds.get(fetchCount-1).get(readId);
                }

                logger.info("order by sql:{},maxId:{},耗时: {}", fetchSql,maxId, System.currentTimeMillis() - startSql);

                Future<Boolean> future = executor
                        .submit(() -> executeEsInsert( mapping,listFileds, impCount, errMsg,isNeedDelete));
                futures.add(future);
            }
            for (Future<Boolean> future : futures) {
                future.get();
            }
            //executor.shutdown();
            logger.info("数据全量导入完成,一共导入 {} 条数据, 耗时: {}", impCount.get(), System.currentTimeMillis() - start);
            etlResult.setResultMessage("导入ES索引 " + esIndex + " 数据：" + impCount.get() + " 条");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            errMsg.add(esIndex + " etl failed! ==>" + e.getMessage());
        }finally {
            if(executor!=null)
            {
                executor.shutdown();
            }
        }
        if (errMsg.isEmpty()) {
            etlResult.setSucceeded(true);
        } else {
            etlResult.setErrorMessage(Joiner.on("\n").join(errMsg));
        }
        return etlResult;
    }


    private List<Map<String, Object>> getListParamsByOrder(DruidDataSource dataSource,String sql,ESMapping mapping,List<String> errMsg)
    {
        return (List<Map<String, Object>>)ESSyncUtil.sqlRS(dataSource, sql, rs -> {
            List<Map<String, Object>> listFileds = new ArrayList<>();
            try {
                while (rs.next()) {
                    Map<String, Object> esFieldData = new LinkedHashMap<>();
                    for (FieldItem fieldItem : mapping.getSchemaItem().getSelectFields().values()) {

                        String fieldName = fieldItem.getFieldName();
                        if (mapping.getSkips().contains(fieldName)) {
                            continue;
                        }

                        Object val = esTemplate.getValFromRS(mapping, rs, fieldName, fieldName);
                        esFieldData.put(fieldName, val);
                    }
                    listFileds.add(esFieldData);
                }

            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                errMsg.add(mapping.get_index() + " etl failed! ==>" + e.getMessage());
                throw new RuntimeException(e);
            }
            return listFileds;
        });
    }


    //-----------2019.3.15 leizheng4修改-----------
    private boolean executeEsInsert(ESMapping mapping,List<Map<String,Object>> listFields ,AtomicLong impCount,
                                    List<String> errMsg,boolean isNeedDelete) {
        try {
            int count = 0;
            try {
                BulkRequestBuilder bulkRequestBuilder = transportClient.prepareBulk();

                long batchBegin = System.currentTimeMillis();
                for(Map<String,Object> esFieldData:listFields){
                    Object idVal = null;
                    if (mapping.get_id() != null) {
                        //idVal = rs.getObject(mapping.get_id());  idVal= BigInter 报错
                        idVal = esFieldData.get(mapping.get_id());
                    }

                    if (idVal != null) {
                        if (mapping.getParent() == null) {
                            bulkRequestBuilder.add(transportClient
                                    .prepareIndex(mapping.get_index(), mapping.get_type(), idVal.toString())
                                    .setSource(esFieldData));
                        } else {
                            // ignore
                        }
                    } else {
                        //idVal = rs.getObject(mapping.getPk());  idVal= BigInter 报错
                        idVal = esFieldData.get(mapping.getPk());
                        if (mapping.getParent() == null) {
                            if(isNeedDelete){
                                // 删除pk对应的数据
                                SearchResponse response = transportClient.prepareSearch(mapping.get_index())
                                        .setTypes(mapping.get_type())
                                        .setQuery(QueryBuilders.termQuery(mapping.getPk(), idVal))
                                        .get();
                                for (SearchHit hit : response.getHits()) {
                                    bulkRequestBuilder.add(transportClient
                                            .prepareDelete(mapping.get_index(), mapping.get_type(), hit.getId()));
                                }
                            }

                            bulkRequestBuilder
                                    .add(transportClient.prepareIndex(mapping.get_index(), mapping.get_type())
                                            .setSource(esFieldData));
                        } else {
                            // ignore
                        }
                    }

                    if (bulkRequestBuilder.numberOfActions() % mapping.getCommitBatch() == 0
                            && bulkRequestBuilder.numberOfActions() > 0) {
                        long esBatchBegin = System.currentTimeMillis();
                        BulkResponse rp = bulkRequestBuilder.execute().actionGet();
                        if (rp.hasFailures()) {
                            this.processFailBulkResponse(rp, Objects.nonNull(mapping.getParent()));
                        }

                        if (logger.isDebugEnabled()) {
                            logger.debug("全量数据批量导入批次耗时: {}, es执行时间: {}, 批次大小: {}, index; {}",
                                    (System.currentTimeMillis() - batchBegin),
                                    (System.currentTimeMillis() - esBatchBegin),
                                    bulkRequestBuilder.numberOfActions(),
                                    mapping.get_index());
                        }
                        batchBegin = System.currentTimeMillis();
                        bulkRequestBuilder = transportClient.prepareBulk();
                    }
                    count++;
                    impCount.incrementAndGet();
                }

                if (bulkRequestBuilder.numberOfActions() > 0) {
                    long esBatchBegin = System.currentTimeMillis();
                    BulkResponse rp = bulkRequestBuilder.execute().actionGet();
                    if (rp.hasFailures()) {
                        this.processFailBulkResponse(rp, Objects.nonNull(mapping.getParent()));
                    }
                    if (logger.isDebugEnabled()) {
                        logger.debug("全量数据批量导入最后批次耗时: {}, es执行时间: {}, 批次大小: {}, index; {}",
                                (System.currentTimeMillis() - batchBegin),
                                (System.currentTimeMillis() - esBatchBegin),
                                bulkRequestBuilder.numberOfActions(),
                                mapping.get_index());
                    }
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                errMsg.add(mapping.get_index() + " etl failed! ==>" + e.getMessage());
                throw new RuntimeException(e);
            }
            return true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return false;
        }
    }
}
