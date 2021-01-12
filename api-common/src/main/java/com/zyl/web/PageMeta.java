package com.zyl.web;

/**
 * @author zyl
 */
public class PageMeta<T> implements Meta {

    private long total;

    public long getTotal() {
        return total;
    }

    public PageMeta<T> setTotal(long total) {
        this.total = total;
        return this;
    }
}
