package com.zyl.sql;


import com.zyl.util.StringUtil;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

/**
 * sql where条件对象
 *
 * @author zyl
 */
public final class Where {
    private final StringBuilder whereSql;
    WhereEnum whereEnum;
    /**
     * 是否为复合where条件
     */
    private boolean composite = false;

    public Where() {
        this("", WhereEnum.AND);
    }

    public Where(String where) {
        this(where, WhereEnum.AND);
    }

    Where(String where, WhereEnum whereEnum) {
        Validate.notNull(where, "where can't be null");
        whereSql = new StringBuilder(where);
        this.whereEnum = whereEnum;
    }

    public Where and(String and) {
        Validate.validState(StringUtils.isNotBlank(and), "and can't be empty neither blank");
        composite = true;
        whereSql.append(whereSql.length() > 0 ? " AND " : "").append(and);
        return this;
    }

    public Where and(boolean condition, String and) {
        if (condition) {
            and(and);
        }
        return this;
    }

    public Where and(Where and) {
        Validate.notNull(and, "where can't be null");
        composite = true;
        whereSql.append(whereSql.length() > 0 ? " AND " : "").append(and.build());
        return this;
    }

    public Where and(boolean condition, Where and) {
        if (condition) {
            and(and);
        }
        return this;
    }

    public Where or(String or) {
        Validate.validState(StringUtils.isNotBlank(or), "or can't be empty neither blank");
        composite = true;
        whereSql.append(whereSql.length() > 0 ? " OR " : "").append(or);
        return this;
    }

    public Where or(boolean condition, String or) {
        if (condition) {
            or(or);
        }
        return this;
    }

    public Where or(Where or) {
        Validate.notNull(or, "where can't be null");
        composite = true;
        whereSql.append(whereSql.length() > 0 ? " OR " : "").append(or.build());
        return this;
    }

    public Where or(boolean condition, Where or) {
        if (condition) {
            or(or);
        }
        return this;
    }

    public Where andIn(String column, String[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.notEmpty(in, "in must not be empty");
        return and(column + " IN (" + StringUtil.withSingleQuoteJoin(in, ",") + ")");
    }

    public Where andIn(boolean condition, String column, String[] in) {
        if (condition) {
            andIn(column, in);
        }
        return this;
    }

    public Where andIn(String column, byte[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return and(column + " IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where andIn(boolean condition, String column, byte[] in) {
        if (condition) {
            andIn(column, in);
        }
        return this;
    }

    public Where andIn(String column, short[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return and(column + " IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where andIn(boolean condition, String column, short[] in) {
        if (condition) {
            andIn(column, in);
        }
        return this;
    }

    public Where andIn(String column, int[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return and(column + " IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where andIn(boolean condition, String column, int[] in) {
        if (condition) {
            andIn(column, in);
        }
        return this;
    }

    public Where andIn(String column, float[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return and(column + " IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where andIn(boolean condition, String column, float[] in) {
        if (condition) {
            andIn(column, in);
        }
        return this;
    }

    public Where andIn(String column, double[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return and(column + " IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where andIn(boolean condition, String column, double[] in) {
        if (condition) {
            andIn(column, in);
        }
        return this;
    }

    public Where andIn(String column, long[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return and(column + " IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where andIn(boolean condition, String column, long[] in) {
        if (condition) {
            andIn(column, in);
        }
        return this;
    }

    public Where andNotIn(String column, String[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.notEmpty(in, "in must not be empty");
        return and(column + " NOT IN (" + StringUtil.withSingleQuoteJoin(in, ",") + ")");
    }

    public Where andNotIn(boolean condition, String column, String[] in) {
        if (condition) {
            andNotIn(column, in);
        }
        return this;
    }

    public Where andNotIn(String column, byte[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return and(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where andNotIn(boolean condition, String column, byte[] in) {
        if (condition) {
            andNotIn(column, in);
        }
        return this;
    }

    public Where andNotIn(String column, short[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return and(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where andNotIn(boolean condition, String column, short[] in) {
        if (condition) {
            andNotIn(column, in);
        }
        return this;
    }

    public Where andNotIn(String column, int[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return and(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where andNotIn(boolean condition, String column, int[] in) {
        if (condition) {
            andNotIn(column, in);
        }
        return this;
    }

    public Where andNotIn(String column, float[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return and(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where andNotIn(boolean condition, String column, float[] in) {
        if (condition) {
            andNotIn(column, in);
        }
        return this;
    }

    public Where andNotIn(String column, double[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return and(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where andNotIn(boolean condition, String column, double[] in) {
        if (condition) {
            andNotIn(column, in);
        }
        return this;
    }

    public Where andNotIn(String column, long[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return and(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where andNotIn(boolean condition, String column, long[] in) {
        if (condition) {
            andNotIn(column, in);
        }
        return this;
    }

    //
    public Where orIn(String column, String[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.notEmpty(in, "in must not be empty");
        return or(column + " IN (" + StringUtil.withSingleQuoteJoin(in, ",") + ")");
    }

    public Where orIn(boolean condition, String column, String[] in) {
        if (condition) {
            orIn(column, in);
        }
        return this;
    }

    public Where orIn(String column, byte[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return or(column + " IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where orIn(boolean condition, String column, byte[] in) {
        if (condition) {
            orIn(column, in);
        }
        return this;
    }

    public Where orIn(String column, short[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return or(column + " IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where orIn(boolean condition, String column, short[] in) {
        if (condition) {
            orIn(column, in);
        }
        return this;
    }

    public Where orIn(String column, int[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return or(column + " IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where orIn(boolean condition, String column, int[] in) {
        if (condition) {
            orIn(column, in);
        }
        return this;
    }

    public Where orIn(String column, float[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return or(column + " IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where orIn(boolean condition, String column, float[] in) {
        if (condition) {
            orIn(column, in);
        }
        return this;
    }

    public Where orIn(String column, double[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return or(column + " IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where orIn(boolean condition, String column, double[] in) {
        if (condition) {
            orIn(column, in);
        }
        return this;
    }

    public Where orIn(String column, long[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return or(column + " IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where orIn(boolean condition, String column, long[] in) {
        if (condition) {
            orIn(column, in);
        }
        return this;
    }

    public Where orNotIn(String column, String[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.notEmpty(in, "in must not be empty");
        return or(column + " NOT IN (" + StringUtil.withSingleQuoteJoin(in, ",") + ")");
    }

    public Where orNotIn(boolean condition, String column, String[] in) {
        if (condition) {
            orNotIn(column, in);
        }
        return this;
    }

    public Where orNotIn(String column, byte[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return or(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where orNotIn(boolean condition, String column, byte[] in) {
        if (condition) {
            orNotIn(column, in);
        }
        return this;
    }

    public Where orNotIn(String column, short[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return or(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where orNotIn(boolean condition, String column, short[] in) {
        if (condition) {
            orNotIn(column, in);
        }
        return this;
    }

    public Where orNotIn(String column, int[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return or(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where orNotIn(boolean condition, String column, int[] in) {
        if (condition) {
            orNotIn(column, in);
        }
        return this;
    }

    public Where orNotIn(String column, float[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return or(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where orNotIn(boolean condition, String column, float[] in) {
        if (condition) {
            orNotIn(column, in);
        }
        return this;
    }

    public Where orNotIn(String column, double[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return or(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where orNotIn(boolean condition, String column, double[] in) {
        if (condition) {
            orNotIn(column, in);
        }
        return this;
    }

    public Where orNotIn(String column, long[] in) {
        Validate.notBlank(column, "column must have text");
        Validate.isTrue(ArrayUtils.isNotEmpty(in), "in must not be empty");
        return or(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
    }

    public Where orNotIn(boolean condition, String column, long[] in) {
        if (condition) {
            orNotIn(column, in);
        }
        return this;
    }


    String build() {
        if (composite) {
            return "(" + whereSql.toString() + ")";
        } else {
            return whereSql.toString();
        }
    }

    enum WhereEnum {
        /**
         * and
         */
        AND,
        /**
         * or
         */
        OR
    }
}
