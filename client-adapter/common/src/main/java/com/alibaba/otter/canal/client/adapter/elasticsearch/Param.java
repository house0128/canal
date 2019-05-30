package com.alibaba.otter.canal.client.adapter.elasticsearch;

import java.io.Serializable;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * TODO: 类描述
 *
 * @author leizheng4
 * @date 2019/5/28 11:36
 */
public class Param implements Serializable {

    /**
     * 分页起始位置，默认为1
     */
    private static final int PAGE_INDEX_MIN =1;

    /**
     * 分页最大返回参数 500
     */
    private static final int LIMIT_MAX = 500;

    /**
     * 默认最大分页返回 500
     */
    public static final int DEFAULT_MAX_LIMIT_BY_PAGE = 500;

    /**
     * 分页起始位置，默认为1
     *
     * <p>
     * 注意
     * 前端分页显示是从1开始
     * </p>
     */
    private int pageIndex = PAGE_INDEX_MIN;

    /**
     * 校验 分页起始位置
     * @param pageIndex
     */
    private void checkPageIndex(int pageIndex) {
        checkArgument(pageIndex >= PAGE_INDEX_MIN,"分页参数起始位置从1开始");
    }

    /**
     * limit  可选，分页最大返回数目，默认为20，当limit超过500时最多只返回500条
     */
    private int limit = 20;

    /**
     * 校验 分页最大返回数目
     * @param limit
     */
    private void checkLimit(int limit) {
        checkArgument(1 <= limit && limit <= LIMIT_MAX,"分页参数分页最大数量从1至500");
    }

    /**
     * 排序字段。
     */
    private String orderBy;



    /**
     * 构造函数
     */
    public Param() {

    }

    /**
     * 构造函数。
     *
     * @param pageIndex  当前页码，从1开始。
     * @param limit 每页记录数。
     */
    public Param(int pageIndex, int limit) {
        checkPageIndex(pageIndex);
        checkLimit(limit);
        this.pageIndex = pageIndex;
        this.limit = limit;
    }

    /**
     * 构造函数。
     *
     * @param pageIndex           当前页码，从1开始。
     * @param limit          每页记录数。
     * @param orderBy        排序字段
     * @param orderDirection 排序方向。
     */
    public Param(int pageIndex, int limit, String orderBy) {
        checkPageIndex(pageIndex);
        checkLimit(limit);
        this.pageIndex = pageIndex;
        this.limit = limit;
        this.orderBy = orderBy;
    }


    public int getSkip() {
        return (this.pageIndex - 1) * this.limit;
    }


    public int getPageIndex() {
        return pageIndex;
    }

    public void setPageIndex(int pageIndex) {
        checkPageIndex(pageIndex);
        this.pageIndex = pageIndex;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        checkLimit(limit);
        this.limit = limit;
    }

    public String getOrderBy() {
        return orderBy;
    }

    public void setOrderBy(String orderBy) {
        this.orderBy = orderBy;
    }


    private static final long serialVersionUID = 2112537108587791494L;

    @Override
    public String toString() {
        return "Param{" +
                "pageIndex=" + pageIndex +
                ", limit=" + limit +
                ", orderBy='" + orderBy + '\'' +
                '}';
    }
}
