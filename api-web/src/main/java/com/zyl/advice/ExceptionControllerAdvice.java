package com.zyl.advice;


import com.zyl.consts.consts.Const;
import com.zyl.web.EmptyMeta;
import com.zyl.web.Error;
import com.zyl.web.Response;

import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 统一异常拦截
 *
 * @author zyl
 */
@ControllerAdvice
public class ExceptionControllerAdvice {

    @ExceptionHandler(value = MethodArgumentNotValidException.class)
    public Response<Void, EmptyMeta> methodArgumentHandler(MethodArgumentNotValidException ex) {
        List<FieldError> allErrors = ex.getBindingResult().getFieldErrors();
        List<Error> errors = new ArrayList<>(allErrors.size());
        allErrors.forEach(objectError -> errors
                .add(new Error(objectError.getField(), objectError.getDefaultMessage())));
        return Response.fail(com.zyl.consts.consts.Const.STATUS_UNKNOWN_ERROR, errors);
    }

    @ResponseBody
    @ExceptionHandler(value = Exception.class)
    public Response<Void, EmptyMeta> catchException(Exception e) {
        return Response.fail(Const.STATUS_WRONG_REQUEST, Collections.emptyList());
    }

}
