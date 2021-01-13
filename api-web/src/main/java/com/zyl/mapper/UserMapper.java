package com.zyl.mapper;

import com.zyl.domain.User;

import org.apache.ibatis.annotations.Mapper;

import java.util.List;

/**
 * @author zyl
 * @date 2021/1/8 10:08 下午
 */
@Mapper
public interface UserMapper {
    
    List<User> findAll();
}
