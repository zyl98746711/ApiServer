package com.zyl.consts;

import java.util.regex.Pattern;

import static java.util.regex.Pattern.compile;

/**
 * 正则表达式常量类
 *
 * @author ty
 */
public final class RegexConst {
    /**
     * 多个id使用逗号拼接的字符串正则字符串
     */
    public static final String IDS_COMMA_STR_REGEX = "\\d+(,\\d+)*";
    /**
     * 多个id使用逗号拼接的字符串
     */
    public static final Pattern IDS_COMMA_STR = Pattern.compile(IDS_COMMA_STR_REGEX);
    /**
     * 手机正则字符串
     */
    public static final String PHONE_REGEX = "^[1][3-9][0-9]{9}$";
    /**
     * 手机正则
     */
    public static final Pattern PHONE = Pattern.compile(PHONE_REGEX);
    /**
     * 邮箱正则字符串
     */
    public static final String EMAIL_REGEX = "[a-zA-Z0-9_-]+@\\w+\\.[a-z]+(\\.[a-z]+)?";
    /**
     * 邮箱正则
     */
    public static final Pattern EMAIL = Pattern.compile(EMAIL_REGEX, Pattern.CASE_INSENSITIVE);

    /**
     * emoji正则字符串
     */
    public static final String EMOJI_REGEX = "[\ud83c\udc00-\ud83c\udfff]|[\ud83d\udc00-\ud83d\udfff]|[\u2600-\u27ff]";

    /**
     * emoji正则
     */
    public static final Pattern EMOJI = Pattern.compile(EMAIL_REGEX, Pattern.UNICODE_CASE | Pattern.CASE_INSENSITIVE);
    /**
     * 特殊字符 空格 回车 换行 tab
     */
    public static final Pattern SPECIAL_REGEX = compile("\\s*|\r|\n|\t");
}
