package com.alibaba.otter.canal.adapter.launcher.common;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * TODO: 类描述
 *
 * @author leizheng4
 * @date 2019/5/23 16:42
 */
public class MetricsInfo {
    public static MetricsInfo metricsInfo = null;

    private int qps = 0;
    private long syncFailCount = 0;
    private String lastSyncFailTime;

    public static MetricsInfo getInstance() {

        if (metricsInfo == null) {

            metricsInfo = new MetricsInfo();

        }

        return metricsInfo;
    }

    public int getQps() {
        return qps;
    }

    public void setQps(int qps) {
        this.qps = qps;
    }


    public void addSyncFailCount(){
        syncFailCount = syncFailCount +1;
        lastSyncFailTime = getCurrentTime();
    }

    public long getSyncFailCount() {
        return syncFailCount;
    }

    public void setSyncFailCount(long syncFailCount) {
        this.syncFailCount = syncFailCount;
    }

    public String getLastSyncFailTime() {
        return lastSyncFailTime;
    }

    public void setLastSyncFailTime(String lastSyncFailTime) {
        this.lastSyncFailTime = lastSyncFailTime;
    }

    public String getCurrentTime(){
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置日期格式
        return df.format(new Date());// new Date()为获取当前系统时间，也可使用当前时间戳
    }

    @Override
    public String toString() {
        return "{" +
                "qps=" + qps +
                ", syncFailCount=" + syncFailCount +
                ", lastSyncFailTime='" + lastSyncFailTime + '\'' +
                '}';
    }
}
