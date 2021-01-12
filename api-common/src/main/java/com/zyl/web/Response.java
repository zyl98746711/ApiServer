package com.zyl.web;

import java.util.List;

/**
 * controller返回值
 *
 * @author zyl
 */
public class Response<T, K extends Meta> {

    private T data;

    private K meta;

    private Integer statusCode;

    private String msg;

    private List<Error> errors;

    private static final Integer SUCCESS_CODE = 200;

    private static final String SUCCESS_MESSAGE = "success";

    private static final String FAIL_MESSAGE = "fail";

    public Response(T data, K meta, Integer statusCode, String msg, List<Error> errors) {
        this.data = data;
        this.meta = meta;
        this.statusCode = statusCode;
        this.msg = msg;
        this.errors = errors;
    }

    public static <T, K extends Meta> Response<T, K> success(T t, K k) {
        return new Response<>(t, k, SUCCESS_CODE, SUCCESS_MESSAGE, null);
    }

    public static <T, K extends Meta> Response<T, K> success(T t) {
        return new Response<>(t, null, SUCCESS_CODE, SUCCESS_MESSAGE, null);
    }

    public static <T, K extends Meta> Response<T, K> fail(Integer code, String msg) {
        return new Response<>(null, null, code, msg, null);
    }

    public static <T, K extends Meta> Response<T, K> fail(Integer code, List<Error> errors) {
        return new Response<>(null, null, code, FAIL_MESSAGE, errors);
    }

    public static <T, K extends Meta> Response<T, K> result(Response<T, K> response) {
        if (SUCCESS_CODE.equals(response.getStatusCode())) {
            return new Response<>(response.getData(), response.getMeta(), SUCCESS_CODE, response.getMsg(), null);
        } else {
            return new Response<>(response.getData(), response.getMeta(), response.getStatusCode(), response.getMsg(), response.getErrors());
        }
    }

    public List<Error> getErrors() {
        return errors;
    }

    public Response<T, K> setErrors(List<Error> errors) {
        this.errors = errors;
        return this;
    }

    public T getData() {
        return data;
    }

    public Response<T, K> setData(T data) {
        this.data = data;
        return this;
    }

    public Integer getStatusCode() {
        return statusCode;
    }

    public Response<T, K> setStatusCode(Integer statusCode) {
        this.statusCode = statusCode;
        return this;
    }

    public String getMsg() {
        return msg;
    }

    public Response<T, K> setMsg(String msg) {
        this.msg = msg;
        return this;
    }

    public K getMeta() {
        return meta;
    }

    public Response<T, K> setMeta(K meta) {
        this.meta = meta;
        return this;
    }


    public boolean isSuccess() {
        return 200 == this.statusCode;
    }

}
