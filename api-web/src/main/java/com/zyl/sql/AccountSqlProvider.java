package com.zyl.sql;

/**
 * @author zyl
 */
public class AccountSqlProvider {

    public String findAllSql() {
        return new SQL()
                .select("uid,user_name,mobile")
                .from("t_account")
                .build();
    }
}