package com.alibaba.otter.canal.client.adapter.es;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import com.alibaba.otter.canal.client.adapter.es.config.*;
import com.alibaba.otter.canal.common.utils.NamedThreadFactory;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.es.config.ESSyncConfig.ESMapping;
import com.alibaba.otter.canal.client.adapter.es.monitor.ESConfigMonitor;
import com.alibaba.otter.canal.client.adapter.es.service.ESEtlService;
import com.alibaba.otter.canal.client.adapter.es.service.ESSyncService;
import com.alibaba.otter.canal.client.adapter.es.support.ESTemplate;
import com.alibaba.otter.canal.client.adapter.support.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ES外部适配器
 *
 * @author rewerma 2018-10-20
 * @version 1.0.0
 */
@SPI("es")
public class ESAdapter implements OuterAdapter {

    private static Logger logger = LoggerFactory.getLogger(ESAdapter.class);

    private Map<String, ESSyncConfig> esSyncConfig = new ConcurrentHashMap<>(); // 文件名对应配置
    private Map<String, Map<String, ESSyncConfig>> dbTableEsSyncConfig = new ConcurrentHashMap<>(); // schema-table对应配置

    private TransportClient transportClient;

    private ESSyncService esSyncService;

    private ESConfigMonitor esConfigMonitor;

    private int index;

    public TransportClient getTransportClient() {
        return transportClient;
    }

    public ESSyncService getEsSyncService() {
        return esSyncService;
    }

    public Map<String, ESSyncConfig> getEsSyncConfig() {
        return esSyncConfig;
    }

    public Map<String, Map<String, ESSyncConfig>> getDbTableEsSyncConfig() {
        return dbTableEsSyncConfig;
    }

    private int threadCount;                 //多线程处理的数目

    Map<Integer, ExecutorService> executorServiceMap;  //多线程处理的线程池

    private boolean mergeUpdate;   //是否合并update

    @Override
    public void init(OuterAdapterConfig configuration, Properties environment) {
        try {
            Map<String, ESSyncConfig> esSyncConfigTmp = ESSyncConfigLoader.load(environment);
            // 过滤不匹配的key的配置
            esSyncConfigTmp.forEach((key, config) -> {
                if ((config.getOuterAdapterKey() == null && configuration.getKey() == null)
                        || (config.getOuterAdapterKey() != null
                        && config.getOuterAdapterKey().equalsIgnoreCase(configuration.getKey()))) {
                    esSyncConfig.put(key, config);
                }
            });

            for (Map.Entry<String, ESSyncConfig> entry : esSyncConfig.entrySet()) {
                String configName = entry.getKey();
                ESSyncConfig config = entry.getValue();
                SchemaItem schemaItem = SqlParser.parse(config.getEsMapping().getSql());
                config.getEsMapping().setSchemaItem(schemaItem);

                DruidDataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
                if (dataSource == null || dataSource.getUrl() == null) {
                    throw new RuntimeException("No data source found: " + config.getDataSourceKey());
                }
                Pattern pattern = Pattern.compile(".*:(.*)://.*/(.*)\\?.*$");
                Matcher matcher = pattern.matcher(dataSource.getUrl());
                if (!matcher.find()) {
                    throw new RuntimeException("Not found the schema of jdbc-url: " + config.getDataSourceKey());
                }
                String schema = matcher.group(2);

                schemaItem.getAliasTableItems().values().forEach(tableItem -> {
                    Map<String, ESSyncConfig> esSyncConfigMap = dbTableEsSyncConfig
                            .computeIfAbsent(schema + "-" + tableItem.getTableName(), k -> new HashMap<>());
                    esSyncConfigMap.put(configName, config);
                });
            }

            Map<String, String> properties = configuration.getProperties();
            Settings.Builder settingBuilder = Settings.builder();
            properties.forEach(settingBuilder::put);
            Settings settings = settingBuilder.build();
            transportClient = new PreBuiltTransportClient(settings);
            String[] hostArray = configuration.getHosts().split(",");
            for (String host : hostArray) {
                int i = host.indexOf(":");
                transportClient.addTransportAddress(new TransportAddress(InetAddress.getByName(host.substring(0, i)),
                        Integer.parseInt(host.substring(i + 1))));
            }
            ESTemplate esTemplate = new ESTemplate(transportClient);
            esSyncService = new ESSyncService(esTemplate);

            esConfigMonitor = new ESConfigMonitor();
            esConfigMonitor.init(this);

            this.threadCount = configuration.getThreadCount() > 1 ? configuration.getThreadCount() : 1;
            executorServiceMap = new HashMap<>();
            //根据线程数，初始化线程池的Map
            for (int i = 0; i < threadCount; i++) {
                executorServiceMap.put(i, Executors.newFixedThreadPool(1, new NamedThreadFactory("ES-PROCESS-POOL" + i)));
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void sync(List<Dml> dmls) {
        int errorCount = 0;
        try {
            List<Future<Boolean>> futures = new ArrayList<>();
            this.index = 0;
            Map<RowKey, Dml> result = new LinkedHashMap<>();
            //对每个线程数进行归并
            for (Dml dml : dmls) {
                splitAndMergeUpdateDml(dml, result);
            }
            //遍历所有dml消息，处理同步逻辑
            for (Dml dml : result.values()) {
                sync(dml, futures);
            }


            for (Future<Boolean> future : futures) {
                Boolean res = future.get();
                if (res == false) {
                    errorCount++;
                }
            }
        } catch (Exception e) {
            logger.error("sync error.", e);
        }

        if (errorCount > 0) {
            throw new RuntimeException("sync error count:" + errorCount);
        }
    }

    public void sync(Dml dml, List<Future<Boolean>> futures) {
        String database = dml.getDatabase();
        String table = dml.getTable();
        if (dml.getData() == null || dml.getData().isEmpty()) {
            return;
        }

        Map<String, ESSyncConfig> configMap = dbTableEsSyncConfig.get(database + "-" + table);
        if (configMap != null) {
            for (ESSyncConfig esSyncConfig : configMap.values()) {
                ESMapping esMapping = esSyncConfig.getEsMapping();
                //如果有ThreadKey
                if (esMapping.getThreadKeys().size() > 0) {
                    String hashKey = esMapping.getThreadKeys().get(dml.getTable());
                    if (StringUtils.isNotEmpty(hashKey)) {
                        for (Map<String, Object> map : dml.getData()) {
                            Object value = map.get(hashKey);
                            if (value != null) {
                                //hash到对应线程里,hashCode可能为负值
                                int hash = (value.hashCode() & Integer.MAX_VALUE) % threadCount;
                                if (logger.isDebugEnabled()) {
                                    logger.debug("hash value:{} , hashKey：{},dml: {}", hash, hashKey, dml);
                                }
                                sendTaskToExecutor(hash, futures, esSyncConfig, dml);
                            } else {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("not find hashKey in dml,hashKey:{},dml:{} ", hashKey, dml);
                                }
                                sendTaskToExecutor(0, futures, esSyncConfig, dml);
                            }
                        }
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("not find hashKey in dml,hashKey:{},dml:{} ", hashKey, dml);
                        }
                        for (Map<String, Object> map : dml.getData()) {
                            sendTaskToExecutor(0, futures, esSyncConfig, dml);
                        }
                    }
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("not have threadKey,dml:{} ", dml);
                    }
                    for (Map<String, Object> map : dml.getData()) {
                        sendTaskToExecutor(0, futures, esSyncConfig, dml);
                    }
                }
            }
        }
    }


    /**
     * 拆分DML为单条DML，并且合并update
     *
     * @param dml
     * @param result
     */
    private void splitAndMergeUpdateDml(Dml dml, Map<RowKey, Dml> result) {
        String type = dml.getType();
        if (type != null && type.equalsIgnoreCase("INSERT")) {
            splitDmls(dml, false, result);
        } else if (type != null && type.equalsIgnoreCase("UPDATE")) {
            splitDmls(dml, true, result);
        } else if (type != null && type.equalsIgnoreCase("DELETE")) {
            splitDmls(dml, false, result);
        }
    }

    private void splitDmls(Dml dml, boolean isUpdate, Map<RowKey, Dml> result) {
        for (int i = 0; i < dml.getData().size(); i++) {
            this.index++;//index自增
            Dml dmlNew = new Dml();
            dmlNew.setDestination(dml.getDestination());
            dmlNew.setDatabase(dml.getDatabase());
            dmlNew.setTable(dml.getTable());
            dmlNew.setType(dml.getType());
            dmlNew.setEs(dml.getEs());
            dmlNew.setTs(dml.getTs());
            dmlNew.setSql(dml.getSql());

            List<Map<String, Object>> datas = new ArrayList<>();
            List<Map<String, Object>> oldDatas = new ArrayList<>();

            Map<String, Object> data = new HashMap<>();
            if (dml.getData() != null && !dml.getData().isEmpty()) {
                data = dml.getData().get(i);
                datas.add(data);
            }
            Object pkValue = null;
            if (dml.getPkNames() != null) {
                if (dml.getPkNames().size() == 1 && data.size() > 0) {
                    String pkName = dml.getPkNames().get(0);
                    pkValue = data.get(pkName);
                } else if (dml.getPkNames().size() > 1) {
                    logger.error("PkNames size >1 ." + dml.getPkNames().toString());
                }
            }

            if (isUpdate) {
                if (dml.getOld() != null && !dml.getOld().isEmpty()) {
                    oldDatas.add(dml.getOld().get(i));
                }
            }

            dmlNew.setData(datas);
            dmlNew.setOld(oldDatas);

            //是否合并update
            if(mergeUpdate)
            {
                if (isUpdate) {
                    RowKey rowKey = new RowKey(dmlNew.getTable(), dml.getType(), pkValue.toString(), 0);
                    if (!result.containsKey(rowKey)) {
                        result.put(rowKey, dmlNew);
                    } else {
                        Dml dmlOld = result.remove(rowKey);
                        //merge dml
                        replaceColumnValue(dmlNew, dmlOld);

                        result.put(rowKey, dmlNew);
                    }
                } else {
                    RowKey rowKey = new RowKey(dmlNew.getTable(), dml.getType(), pkValue.toString(), this.index);//增删不需要归并，但是key需要区分，所以用自增index区分
                    result.put(rowKey, dmlNew);
                }
            }else{
                RowKey rowKey = new RowKey(dmlNew.getTable(), dml.getType(), pkValue.toString(), this.index);//增删不需要归并，但是key需要区分，所以用自增index区分
                result.put(rowKey, dmlNew);
            }

        }
        if (logger.isDebugEnabled()) {
            logger.debug("dmlSize:{} merge--->dmlSize:{}", dml.getData().size(), result.size());
        }
    }

    /**
     *
     * @param dmlNew
     * @param dmlOld
     */
    private void replaceColumnValue(Dml dmlNew, Dml dmlOld) {
        // 把上一次变更的做合并，合并到最新的一条记录上
        if (dmlOld.getOld().size() > 0) {
            Map<String, Object> updateOld = dmlOld.getOld().get(0);
            if (dmlNew.getOld().size() > 0) {
                Map<String, Object> updateNew = dmlNew.getOld().get(0);
                for (Map.Entry<String, Object> entry : updateOld.entrySet()) {
                    String key = entry.getKey();
                    if (!updateNew.containsKey(key)) {
                        updateNew.put(key, entry.getValue());
                    }
                }
            } else {
                dmlNew.getOld().clear();
                dmlNew.getOld().add(updateOld);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("update merge.oldDml:{},newDml:{}", dmlOld, dmlNew);
            }
        }
    }

    /**
     * 根据hash结果 指定线程处理
     *
     * @param hash
     * @param futures
     * @param esSyncConfig
     * @param dml
     */
    private void sendTaskToExecutor(int hash, List<Future<Boolean>> futures, ESSyncConfig esSyncConfig, Dml dml) {
        futures.add(executorServiceMap.get(hash).submit(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return esSyncService.sync(esSyncConfig, dml);
            }
        }));
    }


    @Override
    public EtlResult etl(String task, List<String> params) {
        EtlResult etlResult = new EtlResult();
        ESSyncConfig config = esSyncConfig.get(task);
        if (config != null) {
            DataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
            ESEtlService esEtlService = new ESEtlService(transportClient, config);
            if (dataSource != null) {
                return esEtlService.importData(params, false);
            } else {
                etlResult.setSucceeded(false);
                etlResult.setErrorMessage("DataSource not found");
                return etlResult;
            }
        } else {
            StringBuilder resultMsg = new StringBuilder();
            boolean resSuccess = true;
            // ds不为空说明传入的是datasourceKey
            for (ESSyncConfig configTmp : esSyncConfig.values()) {
                // 取所有的destination为task的配置
                if (configTmp.getDestination().equals(task)) {
                    ESEtlService esEtlService = new ESEtlService(transportClient, configTmp);
                    EtlResult etlRes = esEtlService.importData(params, false);
                    if (!etlRes.getSucceeded()) {
                        resSuccess = false;
                        resultMsg.append(etlRes.getErrorMessage()).append("\n");
                    } else {
                        resultMsg.append(etlRes.getResultMessage()).append("\n");
                    }
                }
            }
            if (resultMsg.length() > 0) {
                etlResult.setSucceeded(resSuccess);
                if (resSuccess) {
                    etlResult.setResultMessage(resultMsg.toString());
                } else {
                    etlResult.setErrorMessage(resultMsg.toString());
                }
                return etlResult;
            }
        }
        etlResult.setSucceeded(false);
        etlResult.setErrorMessage("Task not found");
        return etlResult;
    }

    //-----------2019.3.7 leizheng4修改-----------
    @Override
    public EtlResult etlEx(String task, String sql, boolean isNeedDelete) {
        EtlResult etlResult = new EtlResult();
        ESSyncConfig config = esSyncConfig.get(task);
        if (config != null) {
            DataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
            ESEtlService esEtlService = new ESEtlService(transportClient, config);
            if (dataSource != null) {
                return esEtlService.importDataSql(sql, isNeedDelete, true);
                //------------add 逻辑
                // return esEtlService.importData(params, true);
                //----------------
            } else {
                etlResult.setSucceeded(false);
                etlResult.setErrorMessage("DataSource not found");
                return etlResult;
            }
        } else {
            StringBuilder resultMsg = new StringBuilder();
            boolean resSuccess = true;
            // ds不为空说明传入的是datasourceKey
            for (ESSyncConfig configTmp : esSyncConfig.values()) {
                // 取所有的destination为task的配置
                if (configTmp.getDestination().equals(task)) {
                    ESEtlService esEtlService = new ESEtlService(transportClient, configTmp);
                    EtlResult etlRes = esEtlService.importDataSql(sql, isNeedDelete, true);
                    if (!etlRes.getSucceeded()) {
                        resSuccess = false;
                        resultMsg.append(etlRes.getErrorMessage()).append("\n");
                    } else {
                        resultMsg.append(etlRes.getResultMessage()).append("\n");
                    }
                }
            }
            if (resultMsg.length() > 0) {
                etlResult.setSucceeded(resSuccess);
                if (resSuccess) {
                    etlResult.setResultMessage(resultMsg.toString());
                } else {
                    etlResult.setErrorMessage(resultMsg.toString());
                }
                return etlResult;
            }
        }
        etlResult.setSucceeded(false);
        etlResult.setErrorMessage("Task not found");
        return etlResult;
    }


    //-----------2019.3.18 leizheng4修改-----------
    @Override
    public EtlResult etlOrder(String task, String sql, boolean isNeedDelete, String orderByParam) {
        EtlResult etlResult = new EtlResult();
        ESSyncConfig config = esSyncConfig.get(task);
        if (config != null) {
            DataSource dataSource = DatasourceConfig.DATA_SOURCES.get(config.getDataSourceKey());
            ESEtlService esEtlService = new ESEtlService(transportClient, config);
            if (dataSource != null) {
                return esEtlService.importDataSql(sql, isNeedDelete, orderByParam);
                //------------add 逻辑
                // return esEtlService.importData(params, true);
                //----------------
            } else {
                etlResult.setSucceeded(false);
                etlResult.setErrorMessage("DataSource not found");
                return etlResult;
            }
        } else {
            StringBuilder resultMsg = new StringBuilder();
            boolean resSuccess = true;
            // ds不为空说明传入的是datasourceKey
            for (ESSyncConfig configTmp : esSyncConfig.values()) {
                // 取所有的destination为task的配置
                if (configTmp.getDestination().equals(task)) {
                    ESEtlService esEtlService = new ESEtlService(transportClient, configTmp);
                    EtlResult etlRes = esEtlService.importDataSql(sql, isNeedDelete, orderByParam);
                    if (!etlRes.getSucceeded()) {
                        resSuccess = false;
                        resultMsg.append(etlRes.getErrorMessage()).append("\n");
                    } else {
                        resultMsg.append(etlRes.getResultMessage()).append("\n");
                    }
                }
            }
            if (resultMsg.length() > 0) {
                etlResult.setSucceeded(resSuccess);
                if (resSuccess) {
                    etlResult.setResultMessage(resultMsg.toString());
                } else {
                    etlResult.setErrorMessage(resultMsg.toString());
                }
                return etlResult;
            }
        }
        etlResult.setSucceeded(false);
        etlResult.setErrorMessage("Task not found");
        return etlResult;
    }

    @Override
    public Map<String, Object> count(String task) {
        ESSyncConfig config = esSyncConfig.get(task);
        ESMapping mapping = config.getEsMapping();
        SearchResponse response = transportClient.prepareSearch(mapping.get_index())
                .setTypes(mapping.get_type())
                .setSize(0)
                .get();

        long rowCount = response.getHits().getTotalHits();
        Map<String, Object> res = new LinkedHashMap<>();
        res.put("esIndex", mapping.get_index());
        res.put("count", rowCount);
        return res;
    }

    @Override
    public void destroy() {
        for (ExecutorService executorService : executorServiceMap.values()) {
            executorService.shutdown();
        }
        if (transportClient != null) {
            transportClient.close();
        }
    }

    @Override
    public String getDestination(String task) {
        if (esConfigMonitor != null) {
            esConfigMonitor.destroy();
        }

        ESSyncConfig config = esSyncConfig.get(task);
        if (config != null) {
            return config.getDestination();
        }
        return null;
    }
}
