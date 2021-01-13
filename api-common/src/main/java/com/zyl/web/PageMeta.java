package com.zyl.web;

import java.io.Serializable;

/**
 * 带分页的meta对象
 *
 * @author zyl
 */
public final class PageMeta extends EmptyMeta implements Serializable {
    private Page pagination;

    public PageMeta() {

    }

    public PageMeta(Page page) {
        if (!Page.class.equals(page.getClass())) {
            page = new Page(page);
        }
        this.pagination = page;
    }

    public Page getPagination() {
        return pagination;
    }

    public void setPagination(Page pagination) {
        this.pagination = pagination;
    }

    @Override
    public String toString() {
        return "PageMeta{" +
                "pagination=" + pagination +
                super.toString() +
                '}';
    }
}
