package com.zyl.advice;


import com.zyl.Const;
import com.zyl.web.EmptyMeta;
import com.zyl.web.Error;
import com.zyl.web.Response;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.ArrayList;
import java.util.List;

/**
 * 统一异常拦截
 *
 * @author zyl
 */
@RestControllerAdvice
public class ExceptionControllerAdvice {

    @ExceptionHandler(value = MethodArgumentNotValidException.class)
    public Response<Void, EmptyMeta> methodArgumentHandler(MethodArgumentNotValidException ex) {
        List<FieldError> allErrors = ex.getBindingResult().getFieldErrors();
        List<Error> errors = new ArrayList<>(allErrors.size());
        allErrors.forEach(objectError -> errors
                .add(new Error(objectError.getField(), objectError.getDefaultMessage())));
        return Response.fail(Const.STATUS_UNKNOWN_ERROR, errors);
    }

}
