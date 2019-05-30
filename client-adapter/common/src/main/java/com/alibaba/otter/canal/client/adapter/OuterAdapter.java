package com.alibaba.otter.canal.client.adapter;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.alibaba.otter.canal.client.adapter.support.Dml;
import com.alibaba.otter.canal.client.adapter.support.EtlResult;
import com.alibaba.otter.canal.client.adapter.support.OuterAdapterConfig;
import com.alibaba.otter.canal.client.adapter.support.SPI;
import org.omg.CORBA.Environment;

/**
 * 外部适配器接口
 *
 * @author reweerma 2018-8-18 下午10:14:02
 * @version 1.0.0
 */
@SPI("logger")
public interface OuterAdapter {

    /**
     * 外部适配器初始化接口
     *
     * @param configuration 外部适配器配置信息
     */
    void init(OuterAdapterConfig configuration, Properties environment);

    /**
     * 往适配器中同步数据
     *
     * @param dmls 数据包
     */
    void sync(List<Dml> dmls);

    /**
     * 外部适配器销毁接口
     */
    void destroy();

    /**
     * Etl操作
     *
     * @param task 任务名, 对应配置名
     * @param params etl筛选条件
     */
    default EtlResult etl(String task, List<String> params) {
        throw new UnsupportedOperationException("unsupported operation");
    }

    //---------2019.3.7  leizheng4修改-------------
    /**
     * Etl操作
     *
     * @param task 任务名, 对应配置名
     * @param sql sql执行的语句
     *@param isNeedDelete 是否要反查es删除
     */
    default EtlResult etlEx(String task, String sql,boolean isNeedDelete) {
        throw new UnsupportedOperationException("unsupported operation");
    }

    //---------2019.3.15  leizheng4修改-------------
    /**
     *
     * @param task 任务名, 对应配置名
     * @param sql sql执行的语句
     * @param isNeedDelete 是否要反查es删除
     * @param orderByParam 插入的排序字段
     * @return
     */
    default EtlResult etlOrder(String task, String sql,boolean isNeedDelete,String orderByParam) {
        throw new UnsupportedOperationException("unsupported operation");
    }

    /**
     * 计算总数
     *
     * @param task 任务名, 对应配置名
     * @return 总数
     */
    default Map<String, Object> count(String task) {
        throw new UnsupportedOperationException("unsupported operation");
    }

    /**
     * 通过task获取对应的destination
     *
     * @param task 任务名, 对应配置名
     * @return destination
     */
    default String getDestination(String task) {
        return null;
    }
}
