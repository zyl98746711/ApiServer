package com.zyl.mapper;

import com.zyl.domain.Account;
import com.zyl.sql.AccountSqlProvider;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.SelectProvider;

import java.util.List;

/**
 * @author zyl
 * @date 2021/1/8 10:08 下午
 */
@Mapper
public interface AccountMapper {

    /**
     * 查找所有用户
     *
     * @return 用户信息
     */
    @SelectProvider(type = AccountSqlProvider.class, method = "findAllSql")
    List<Account> findAll();
}
