package com.zyl.controller.error;

import com.zyl.consts.consts.Const;
import com.zyl.web.EmptyMeta;
import com.zyl.web.Response;

import org.springframework.boot.autoconfigure.web.ErrorProperties;
import org.springframework.boot.autoconfigure.web.ServerProperties;
import org.springframework.boot.autoconfigure.web.servlet.error.AbstractErrorController;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorViewResolver;
import org.springframework.boot.web.servlet.error.ErrorAttributes;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Collections;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

/**
 * @author zyl
 */
@Controller
@RequestMapping("/error")
public class RewriteErrorController extends AbstractErrorController {

    private ErrorProperties errorProperties;

    public RewriteErrorController(ErrorAttributes errorAttributes, ServerProperties serverProperties) {
        this(errorAttributes, serverProperties.getError(), Collections.emptyList());
    }

    private RewriteErrorController(ErrorAttributes errorAttributes, ErrorProperties errorProperties, List<ErrorViewResolver> errorViewResolvers) {
        super(errorAttributes, errorViewResolvers);
        Assert.notNull(errorProperties, "ErrorProperties must not be null");
        this.errorProperties = errorProperties;
    }


    @RequestMapping
    @ResponseBody
    public Response<Void, EmptyMeta> error(HttpServletRequest request) {
        return Response.fail(Const.NOT_FOUND, Collections.emptyList());
    }


    @Override
    public String getErrorPath() {
        return null;
    }
}
