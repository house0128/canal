package com.alibaba.otter.canal.adapter.launcher.rest;

import java.util.*;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import com.alibaba.otter.canal.adapter.launcher.common.MetricsInfo;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import com.alibaba.otter.canal.adapter.launcher.common.EtlLock;
import com.alibaba.otter.canal.adapter.launcher.common.SyncSwitch;
import com.alibaba.otter.canal.adapter.launcher.config.AdapterCanalConfig;
import com.alibaba.otter.canal.client.adapter.OuterAdapter;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.ExtensionLoader;
import com.alibaba.otter.canal.client.adapter.support.Result;

/**
 * 适配器操作Rest
 *
 * @author rewerma @ 2018-10-20
 * @version 1.0.0
 */
@RestController
public class CommonRest {

    private static Logger                 logger           = LoggerFactory.getLogger(CommonRest.class);

    private static final String           ETL_LOCK_ZK_NODE = "/sync-etl/";

    private ExtensionLoader<OuterAdapter> loader;

    @Resource
    private SyncSwitch                    syncSwitch;
    @Resource
    private EtlLock                       etlLock;

    @Resource
    private AdapterCanalConfig            adapterCanalConfig;

    @PostConstruct
    public void init() {
        loader = ExtensionLoader.getExtensionLoader(OuterAdapter.class);
    }

    /**
     * ETL curl http://127.0.0.1:8081/etl/rdb/oracle1/mytest_user.yml -X POST
     *
     * @param type 类型 hbase, es
     * @param key adapter key
     * @param task 任务名对应配置文件名 mytest_user.yml
     * @param params etl where条件参数, 为空全部导入
     */
    @PostMapping("/etl/{type}/{key}/{task}")
    public EtlResult etl(@PathVariable String type, @PathVariable String key, @PathVariable String task,
                         @RequestParam(name = "params", required = false) String params) {
        OuterAdapter adapter = loader.getExtension(type, key);
        String destination = adapter.getDestination(task);
        String lockKey = destination == null ? task : destination;

        boolean locked = etlLock.tryLock(ETL_LOCK_ZK_NODE + type + "-" + lockKey);
        if (!locked) {
            EtlResult result = new EtlResult();
            result.setSucceeded(false);
            result.setErrorMessage(task + " 有其他进程正在导入中, 请稍后再试");
            return result;
        }
        try {

            Boolean oriSwitchStatus;
            if (destination != null) {
                oriSwitchStatus = syncSwitch.status(destination);
                if (oriSwitchStatus != null && oriSwitchStatus) {
                    syncSwitch.off(destination);
                }
            } else {
                // task可能为destination，直接锁task
                oriSwitchStatus = syncSwitch.status(task);
                if (oriSwitchStatus != null && oriSwitchStatus) {
                    syncSwitch.off(task);
                }
            }
            try {
                List<String> paramArray = null;
                if (params != null) {
                    paramArray = Arrays.asList(params.trim().split(";"));
                }
                return adapter.etl(task, paramArray);
            } finally {
                if (destination != null && oriSwitchStatus != null && oriSwitchStatus) {
                    syncSwitch.on(destination);
                } else if (destination == null && oriSwitchStatus != null && oriSwitchStatus) {
                    syncSwitch.on(task);
                }
            }
        } finally {
            etlLock.unlock(ETL_LOCK_ZK_NODE + type + "-" + lockKey);
        }
    }

    /**
     * ETL curl http://127.0.0.1:8081/etl/hbase/mytest_person2.yml -X POST
     * 
     * @param type 类型 hbase, es
     * @param task 任务名对应配置文件名 mytest_person2.yml
     * @param params etl where条件参数, 为空全部导入
     */
    @PostMapping("/etl/{type}/{task}")
    public EtlResult etl(@PathVariable String type, @PathVariable String task,
                         @RequestParam(name = "params", required = false) String params) {
        return etl(type, null, task, params);
    }

    /**
     * 统计总数 curl http://127.0.0.1:8081/count/rdb/oracle1/mytest_user.yml
     *
     * @param type 类型 hbase, es
     * @param key adapter key
     * @param task 任务名对应配置文件名 mytest_person2.yml
     * @return
     */
    @GetMapping("/count/{type}/{key}/{task}")
    public Map<String, Object> count(@PathVariable String type, @PathVariable String key, @PathVariable String task) {
        OuterAdapter adapter = loader.getExtension(type, key);
        return adapter.count(task);
    }

    /**
     * 统计总数 curl http://127.0.0.1:8081/count/hbase/mytest_person2.yml
     * 
     * @param type 类型 hbase, es
     * @param task 任务名对应配置文件名 mytest_person2.yml
     * @return
     */
    @GetMapping("/count/{type}/{task}")
    public Map<String, Object> count(@PathVariable String type, @PathVariable String task) {
        return count(type, null, task);
    }

    /**
     * 返回所有实例 curl http://127.0.0.1:8081/destinations
     */
    @GetMapping("/destinations")
    public List<Map<String, String>> destinations() {
        List<Map<String, String>> result = new ArrayList<>();
        Set<String> destinations = adapterCanalConfig.DESTINATIONS;
        for (String destination : destinations) {
            Map<String, String> resMap = new LinkedHashMap<>();
            Boolean status = syncSwitch.status(destination);
            String resStatus = "none";
            if (status != null && status) {
                resStatus = "on";
            } else if (status != null && !status) {
                resStatus = "off";
            }
            resMap.put("destination", destination);
            resMap.put("status", resStatus);
            result.add(resMap);
        }
        return result;
    }

    /**
     * 实例同步开关 curl http://127.0.0.1:8081/syncSwitch/example/off -X PUT
     * 
     * @param destination 实例名称
     * @param status 开关状态: off on
     * @return
     */
    @PutMapping("/syncSwitch/{destination}/{status}")
    public Result etl(@PathVariable String destination, @PathVariable String status) {
        if (status.equals("on")) {
            syncSwitch.on(destination);
            logger.info("#Destination: {} sync on", destination);
            return Result.createSuccess("实例: " + destination + " 开启同步成功");
        } else if (status.equals("off")) {
            syncSwitch.off(destination);
            logger.info("#Destination: {} sync off", destination);
            return Result.createSuccess("实例: " + destination + " 关闭同步成功");
        } else {
            Result result = new Result();
            result.setCode(50000);
            result.setMessage("实例: " + destination + " 操作失败");
            return result;
        }
    }

    /**
     * 获取实例开关状态 curl http://127.0.0.1:8081/syncSwitch/example
     * 
     * @param destination 实例名称
     * @return
     */
    @GetMapping("/syncSwitch/{destination}")
    public Map<String, String> etl(@PathVariable String destination) {
        Boolean status = syncSwitch.status(destination);
        String resStatus = "none";
        if (status != null && status) {
            resStatus = "on";
        } else if (status != null && !status) {
            resStatus = "off";
        }
        Map<String, String> res = new LinkedHashMap<>();
        res.put("stauts", resStatus);
        return res;
    }

    //-----------2019.3.7 leizheng4修改-----------
    /**
     * ETL curl http://127.0.0.1:8081/etlEx/es/mytest_user.yml?sql=select%20id,name,age%20from%20user&isNeedDelete=false -X POST
     *
     * @param type 类型 hbase, es
     * @param key adapter key
     * @param task 任务名对应配置文件名 mytest_user.yml
     * @param sql 要执行ELT的语句
     * @param isNeedDelete 在插入es是否需要判断执行删除es逻辑
     */
    @PostMapping("/etlEx/{type}/{task}")
//    public EtlResult etlEx(@PathVariable String type, @PathVariable String task,
//                         @RequestParam(name = "sql", required = true) String sql,@RequestParam(name = "isDelete", required = true) boolean isNeedDelete) {
    public EtlResult etlEx(@PathVariable String type, @PathVariable String task,
                           @RequestParam(name = "sql", required = true) String sql,@RequestParam(name = "isNeedDelete", required = true) boolean isNeedDelete ) {
        OuterAdapter adapter = loader.getExtension(type, null);
        String destination = adapter.getDestination(task);
        String lockKey = destination == null ? task : destination;

        boolean locked = etlLock.tryLock(ETL_LOCK_ZK_NODE + type + "-" + lockKey);
        if (!locked) {
            EtlResult result = new EtlResult();
            result.setSucceeded(false);
            result.setErrorMessage(task + " 有其他进程正在导入中, 请稍后再试");
            return result;
        }
        try {

            Boolean oriSwitchStatus;
            if (destination != null) {
                oriSwitchStatus = syncSwitch.status(destination);
                if (oriSwitchStatus != null && oriSwitchStatus) {
                    syncSwitch.off(destination);
                }
            } else {
                // task可能为destination，直接锁task
                oriSwitchStatus = syncSwitch.status(task);
                if (oriSwitchStatus != null && oriSwitchStatus) {
                    syncSwitch.off(task);
                }
            }
            try {
                if (StringUtils.isNotBlank(sql)) {
                    return  adapter.etlEx(task,sql,isNeedDelete);
                }

                EtlResult etlResult = new EtlResult();
                etlResult.setSucceeded(false);
                etlResult.setErrorMessage("need send sql");
                return etlResult;
            } finally {
                if (destination != null && oriSwitchStatus != null && oriSwitchStatus) {
                    syncSwitch.on(destination);
                } else if (destination == null && oriSwitchStatus != null && oriSwitchStatus) {
                    syncSwitch.on(task);
                }
            }
        } finally {
            etlLock.unlock(ETL_LOCK_ZK_NODE + type + "-" + lockKey);
        }
    }



    //-----------2019.3.15 leizheng4修改-----------
    /**
     * ETL curl http://127.0.0.1:8081/etlOrder/es/mytest_user.yml?sql=select%20id,name,age%20from%20user&isNeedDelete=false -X POST
     *
     * @param type 类型 hbase, es
     * @param key adapter key
     * @param task 任务名对应配置文件名 mytest_user.yml
     * @param sql 要执行ELT的语句
     * @param isNeedDelete 在插入es是否需要判断执行删除es逻辑
     * @param orderByParam 插入的排序字段
     */
    @PostMapping("/etlOrder/{type}/{task}")
//    public EtlResult etlEx(@PathVariable String type, @PathVariable String task,
//                         @RequestParam(name = "sql", required = true) String sql,@RequestParam(name = "isDelete", required = true) boolean isNeedDelete) {
    public EtlResult etlOrder(@PathVariable String type, @PathVariable String task,
                              @RequestParam(name = "sql", required = true) String sql,@RequestParam(name = "isNeedDelete", required = true) boolean isNeedDelete,@RequestParam(name = "orderBy", required = true) String orderByParam) {
        OuterAdapter adapter = loader.getExtension(type, null);
        String destination = adapter.getDestination(task);
        String lockKey = destination == null ? task : destination;

        boolean locked = etlLock.tryLock(ETL_LOCK_ZK_NODE + type + "-" + lockKey);
        if (!locked) {
            EtlResult result = new EtlResult();
            result.setSucceeded(false);
            result.setErrorMessage(task + " 有其他进程正在导入中, 请稍后再试");
            return result;
        }
        try {

            Boolean oriSwitchStatus;
            if (destination != null) {
                oriSwitchStatus = syncSwitch.status(destination);
                if (oriSwitchStatus != null && oriSwitchStatus) {
                    syncSwitch.off(destination);
                }
            } else {
                // task可能为destination，直接锁task
                oriSwitchStatus = syncSwitch.status(task);
                if (oriSwitchStatus != null && oriSwitchStatus) {
                    syncSwitch.off(task);
                }
            }
            try {
                if (StringUtils.isNotBlank(sql)) {
                    return  adapter.etlOrder(task,sql,isNeedDelete,orderByParam);
                }

                EtlResult etlResult = new EtlResult();
                etlResult.setSucceeded(false);
                etlResult.setErrorMessage("need send sql");
                return etlResult;
            } finally {
                if (destination != null && oriSwitchStatus != null && oriSwitchStatus) {
                    syncSwitch.on(destination);
                } else if (destination == null && oriSwitchStatus != null && oriSwitchStatus) {
                    syncSwitch.on(task);
                }
            }
        } finally {
            etlLock.unlock(ETL_LOCK_ZK_NODE + type + "-" + lockKey);
        }
    }


    //-----------2019.4.4 leizheng4修改-----------
    /**
     * ETL curl http://127.0.0.1:8081/metrics -X POST
     * 返回mertics统计信息
     */
    @PostMapping("/metrics")
    public String metrics() {
        return MetricsInfo.getInstance().toString();
    }
}
