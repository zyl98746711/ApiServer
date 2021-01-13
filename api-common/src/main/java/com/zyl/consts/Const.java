package com.zyl.consts.consts;

/**
 * @author ZhZ
 * @description 常量值
 * @time 2019-05-03 10:55
 */
public class Const {

    private Const() {
    }

    /**
     * 错误码
     */
    public static final int STATUS_EXIST_DB = 502;
    public static final int STATUS_NOT_EXIST_DB = 503;
    public static final int STATUS_UNKNOWN_ERROR = 500;
    public static final int STATUS_WRONG_REQUEST = 501;
    public static final int WITHOUT_AUTH = 401;
    public static final int STATUS_CREDIT_LOW = 504;
    public static final int STATUS_SERVICE_FAIL = 505;

    public static final String IGNORE_METHOD = "OPTIONS";
    public static final int NOT_EXIST_SERVICE = 506;
    public static final int NOT_FOUND = 404;
}
