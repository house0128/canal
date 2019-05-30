package com.alibaba.otter.canal.client.adapter.elasticsearch.support;

/**
 * TODO: 类描述
 *
 * @author leizheng4
 * @date 2019/5/28 11:18
 */
public enum QueryFieldType {
    EQUALS("EQUALS"),
    LIKE("LIKE"),
    //模糊查询
    NGRAM("NGRAM"),
    //右模糊查询
    RLIKE("RLIKE"),
    //左模糊查询
    LLIKE("LLIKE"),
    //存在查询
    EXIST("EXIST"),
    //In查询
    IN("IN"),
    //IkSMART 分词查询
    IKSMART("IKSMART");
    private String fieldType;

    private QueryFieldType(String fieldType) {
        this.fieldType = fieldType;
    }

    public String getFieldType() {
        return fieldType;
    }

    public void setFieldType(String fieldType) {
        this.fieldType = fieldType;
    }
}
