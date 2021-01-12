package com.zyl.controller;

import com.zyl.domain.User;
import com.zyl.mapper.UserMapper;
import com.zyl.web.EmptyMeta;
import com.zyl.web.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * @author zyl
 * @date 2021/1/8 9:39 下午
 */
@RestController
@RequestMapping("user")
public class UserController {

    @Autowired
    private UserMapper userMapper;

    /**
     * 保存用户
     *
     * @return 是否成功
     */
    @GetMapping("/")
    public Response<Void, EmptyMeta> saveUser() {
        List<User> users = userMapper.findAll();
        System.out.println(users);
        return Response.success(null);
    }
}
