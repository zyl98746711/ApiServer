package com.zyl.sql;


import com.zyl.util.StringUtil;
import com.zyl.web.Page;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.zyl.sql.Where.WhereEnum.AND;
import static com.zyl.sql.Where.WhereEnum.OR;


/**
 * 动态sql类
 *
 * @author zyl
 */
public final class SQL {
    private final List<String> select = new ArrayList<>();
    private final List<String> from = new ArrayList<>();
    private final List<JoinSql> join = new ArrayList<>();
    private final List<Where> where = new ArrayList<>();
    private final List<String> groupBy = new ArrayList<>();
    private final List<String> having = new ArrayList<>();
    private final List<String> orderBy = new ArrayList<>();
    private final List<String[]> values = new ArrayList<>();
    private final List<String[]> set = new ArrayList<>();
    private final List<String[]> duplicateUpdate = new ArrayList<>();
    private String insert;
    private String insertIgnore;
    private String replace;
    private String update;
    private String delete;
    private int[] limit;
    private final String ITERATOR_SEPARATOR = ";\n";
    private final int ITERATOR_MAX_LENGTH = 200;
    private final String UNION_SEPARATOR = " UNION ";
    private final String UNION_ALL_SEPARATOR = " UNION ALL ";
    private boolean deleteCommand = false;
    private Lock lock;
    /**
     * 是否允许update/delete没有where条件,默认不允许
     */
    private boolean permitNoWhere = false;
    private List<SQL> union = new ArrayList<>();
    private List<SQL> unionAll = new ArrayList<>();
    /**
     * 为适应foreach生成字符串型结果,后续再拼接sql
     */
    private String head;
    private final String DUPLICATE_UPDATE_VALUES = "VALUES";

    private String createSql(ActionType actionType) {
        StringBuilder sql = new StringBuilder();
        switch (actionType) {
            case SELECT: {
                if (select.size() > 0) {
                    sql.append("SELECT ").append(com.sun.deploy.util.StringUtils.join(select, ","));
                }
                break;
            }
            case FROM: {
                if (from.size() > 0) {
                    sql.append("FROM ").append(com.sun.deploy.util.StringUtils.join(from, ","));
                }
                break;
            }
            case JOIN: {
                if (join.size() > 0) {
                    List<String> joinSqls = new ArrayList<>(join.size());
                    for (JoinSql joinSql : join) {
                        // 左连接
                        if (joinSql.getActionType() == ActionType.LEFT_JOIN) {
                            joinSqls.add("LEFT JOIN " + joinSql.getSql());
                        }
                        // 全连接
                        else if (joinSql.getActionType() == ActionType.INNER_JOIN) {
                            joinSqls.add("INNER JOIN " + joinSql.getSql());
                        }
                    }
                    sql.append(com.sun.deploy.util.StringUtils.join(joinSqls, " "));
                }
                break;
            }
            case WHERE: {
                // 预防虽然where size不为空,但是where是blank
                boolean noBlankWhere = false;
                if (where.size() > 0) {
                    // 标记当前where是否是第一个,第一个前面添加"WHERE"关键字(为解决到第一个where为止,前面where为空的情况)
                    boolean firstWhere = true;
                    for (int i = 0; i < where.size(); i++) {
                        Where whereItem = where.get(i);
                        final String whereSql = whereItem.build();
                        if (StringUtils.isBlank(whereSql)) {
                            continue;
                        }
                        // 只要有非blank where即可
                        noBlankWhere = true;
                        if (i == 0 || firstWhere) {
                            // 一旦出现第一次非空的where,状态要设置为false
                            firstWhere = false;
                            sql.append("WHERE ").append(whereSql);
                        } else {
                            Where.WhereEnum whereEnum = whereItem.whereEnum;
                            Validate.notNull(whereEnum, "where's whereEnum can't be null");
                            if (whereEnum == AND) {
                                sql.append(" AND ").append(whereSql);
                            } else if (whereEnum == OR) {
                                sql.append(" OR ").append(whereSql);
                            }
                        }
                    }
                }
                // delete比较特殊，不能用delete != null来判断
                if (update != null || deleteCommand) {
                    Validate.isTrue(permitNoWhere || noBlankWhere, "UPDATE和DELETE不允许全表操作,特殊处理请执行noWhere()方法");
                }
                break;
            }
            case GROUP_BY: {
                if (groupBy.size() > 0) {
                    sql.append("GROUP BY ").append(com.sun.deploy.util.StringUtils.join(groupBy, ","));
                }
                break;
            }
            case HAVING: {
                if (having.size() > 0) {
                    sql.append("HAVING ").append(com.sun.deploy.util.StringUtils.join(having, " AND "));
                }
                break;
            }
            case ORDER_BY: {
                if (orderBy.size() > 0) {
                    sql.append("ORDER BY ").append(com.sun.deploy.util.StringUtils.join(orderBy, ","));
                }
                break;
            }
            case LIMIT: {
                if (limit != null && limit.length == 2) {
                    sql.append(String.format("LIMIT %d,%d", limit[0], limit[1]));
                }
                break;
            }
            case INSERT: {
                if (StringUtils.isNotBlank(insert)) {
                    sql.append("INSERT INTO ").append(insert);
                }
                break;
            }
            case INSERT_IGNORE: {
                if (StringUtils.isNotBlank(insertIgnore)) {
                    sql.append("INSERT IGNORE INTO ").append(insertIgnore);
                }
                break;
            }
            case REPLACE: {
                if (StringUtils.isNotBlank(replace)) {
                    sql.append("REPLACE INTO ").append(replace);
                }
                break;
            }
            case VALUES: {
                if (values.size() > 0) {
                    String[] columnArr = new String[values.size()];
                    String[] valueArr = new String[values.size()];
                    for (int i = 0; i < values.size(); i++) {
                        String[] value = values.get(i);
                        columnArr[i] = value[0];
                        valueArr[i] = value[1];
                    }
                    sql.append(String.format("(%s) VALUES (%s)", StringUtils.join(columnArr, ","),
                            StringUtils.join(valueArr, ",")));
                }
                break;
            }
            case DUPLICATE_UPDATE: {
                if (duplicateUpdate.size() > 0) {
                    String[] duplicateUpdateArr = new String[duplicateUpdate.size()];
                    for (int i = 0; i < duplicateUpdateArr.length; i++) {
                        String[] duplicateUpdateTmp = duplicateUpdate.get(i);
                        duplicateUpdateArr[i] = duplicateUpdateTmp[0] + "=" + duplicateUpdateTmp[1];
                    }
                    sql.append("ON DUPLICATE KEY UPDATE ").append(StringUtils.join(duplicateUpdateArr, ","));
                }
                break;
            }
            case UPDATE: {
                if (StringUtils.isNotBlank(update)) {
                    sql.append("UPDATE ").append(update);
                }
                break;
            }
            case SET: {
                if (set.size() > 0) {
                    String[] setArr = new String[set.size()];
                    for (int i = 0; i < setArr.length; i++) {
                        String[] setTmp = set.get(i);
                        setArr[i] = setTmp[0] + "=" + setTmp[1];
                    }
                    sql.append("SET ").append(StringUtils.join(setArr, ","));
                }
                break;
            }
            case DELETE: {
                if (StringUtils.isNotBlank(delete)) {
                    sql.append("DELETE ").append(delete);
                } else if (deleteCommand) {
                    sql.append("DELETE ");
                }
                break;
            }
            case LOCK: {
                if (lock != null) {
                    sql.append(lock.getLock());
                }
                break;
            }
            case UNION: {
                if (CollectionUtils.isNotEmpty(union)) {
                    for (SQL unionSql : union) {
                        sql.append(UNION_SEPARATOR + unionSql.build());
                    }
                }
                break;
            }
            case UNION_ALL: {
                if (CollectionUtils.isNotEmpty(unionAll)) {
                    for (SQL unionAllSql : unionAll) {
                        sql.append(UNION_ALL_SEPARATOR + unionAllSql.build());
                    }
                }
                break;
            }
            default:
                break;
        }
        return sql.toString();
    }

    public SQL select(String select) {
        if (StringUtils.isNotBlank(select)) {
            this.select.add(select);
        }
        return this;
    }

    public SQL select(boolean condition, String select) {
        return condition ? select(select) : this;
    }

    public SQL select(boolean condition, String select, String alternate) {
        return select(condition ? select : alternate);
    }

    public SQL select(SqlAssert selectAssert) {
        boolean condition = selectAssert.check();
        String select = selectAssert.create();
        return select(condition, select);
    }

    public SQL from(String table) {
        if (StringUtils.isNotBlank(table)) {
            this.from.add(table);
        }
        return this;
    }

    public SQL from(boolean condition, String table) {
        return condition ? from(table) : this;
    }

    public SQL from(boolean condition, String table, String alternate) {
        return from(condition ? table : alternate);
    }

    public SQL from(SqlAssert fromAssert) {
        boolean condition = fromAssert.check();
        String table = fromAssert.create();
        return from(condition, table);
    }

    public SQL from(SQL subSql, String alias) {
        return from("(" + subSql.build() + ") " + alias);
    }

    public SQL leftJoin(String leftJoin) {
        if (StringUtils.isNotBlank(leftJoin)) {
            this.join.add(new JoinSql(ActionType.LEFT_JOIN, leftJoin));
        }
        return this;
    }

    public SQL leftJoin(String leftJoin, String... ands) {
        if (StringUtils.isNotBlank(leftJoin)) {
            StringBuilder sb = new StringBuilder(leftJoin);
            if (ands != null && ands.length > 0) {
                for (String and : ands) {
                    if (StringUtils.isNotBlank(and)) {
                        sb.append(" AND ").append(and);
                    }
                }
            }
            this.join.add(new JoinSql(ActionType.LEFT_JOIN, sb.toString()));
        }
        return this;
    }

    public SQL leftJoin(boolean condition, String leftJoin) {
        return condition ? leftJoin(leftJoin) : this;
    }

    public SQL leftJoin(boolean condition, String leftJoin, String alternate) {
        return leftJoin(condition ? leftJoin : alternate);
    }

    public SQL leftJoin(SqlAssert leftJoinAssert) {
        boolean condition = leftJoinAssert.check();
        String leftJoin = leftJoinAssert.create();
        return leftJoin(condition, leftJoin);
    }

    public SQL leftJoin(SQL subSql, String alias, String on) {
        return leftJoin("(" + subSql.build() + ") " + alias + " ON " + on);
    }

    public SQL leftJoin(boolean condition, SQL subSql, String alias, String on) {
        return condition ? leftJoin("(" + subSql.build() + ") " + alias + " ON " + on) : this;
    }

    public SQL innerJoin(String innerJoin) {
        if (StringUtils.isNotBlank(innerJoin)) {
            this.join.add(new JoinSql(ActionType.INNER_JOIN, innerJoin));
        }
        return this;
    }

    public SQL innerJoin(String innerJoin, String... ands) {
        if (StringUtils.isNotBlank(innerJoin)) {
            StringBuilder sb = new StringBuilder(innerJoin);
            if (ands != null && ands.length > 0) {
                for (String and : ands) {
                    if (StringUtils.isNotBlank(and)) {
                        sb.append(" AND ").append(and);
                    }
                }
            }
            this.join.add(new JoinSql(ActionType.INNER_JOIN, sb.toString()));
        }
        return this;
    }

    public SQL innerJoin(boolean condition, String innerJoin) {
        return condition ? innerJoin(innerJoin) : this;
    }

    public SQL innerJoin(boolean condition, String innerJoin, String alternate) {
        return innerJoin(condition ? innerJoin : alternate);
    }

    public SQL innerJoin(SqlAssert innerJoinAssert) {
        boolean condition = innerJoinAssert.check();
        String innerJoin = innerJoinAssert.create();
        return innerJoin(condition, innerJoin);
    }

    public SQL innerJoin(SQL subSql, String alias, String on) {
        return innerJoin("(" + subSql.build() + ") " + alias + " ON " + on);
    }

    public SQL innerJoin(boolean condition, SQL subSql, String alias, String on) {
        return condition ? innerJoin("(" + subSql.build() + ") " + alias + " ON " + on) : this;
    }

    public SQL where(String where) {
        if (StringUtils.isBlank(where)) {
            throw new SqlPreFlightException("where", "where must have text");
        }
        where(new Where(where, AND));
        return this;
    }

    public SQL where(boolean condition, String where) {
        return condition ? where(where) : this;
    }

    public SQL where(boolean condition, String where, String alternate) {
        return where(condition ? where : alternate);
    }

    public SQL where(SqlAssert whereAssert) {
        boolean condition = whereAssert.check();
        String where = whereAssert.create();
        return where(condition, where);
    }

    public SQL where(Where where) {
        if (null == where) {
            throw new SqlPreFlightException("where", "where can't be null");
        }
        this.where.add(where);
        return this;
    }

    public SQL where(boolean condition, Where where) {
        return condition ? where(where) : this;
    }

    public SQL whereIn(String column, short[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("whereIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("whereIn", "in must have value");
        }
        where(column + " IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL whereIn(boolean condition, String column, short[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("whereIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("whereIn", "in must have value");
        }
        return condition ? where(column + " IN (" + StringUtil.join(in, ",") + ")") : this;
    }

    public SQL whereIn(String column, int[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("whereIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("whereIn", "in must have value");
        }
        where(column + " IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL whereIn(boolean condition, String column, int[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("whereIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("whereIn", "in must have value");
        }
        return condition ? where(column + " IN (" + StringUtil.join(in, ",") + ")") : this;
    }

    public SQL whereIn(String column, long[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("whereIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("whereIn", "in must have value");
        }
        where(column + " IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL whereIn(boolean condition, String column, long[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("whereIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("whereIn", "in must have value");
        }
        return condition ? where(column + " IN (" + StringUtil.join(in, ",") + ")") : this;
    }

    public SQL whereIn(String column, String[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("whereIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("whereIn", "in must have value");
        }
        where(column + " IN (" + StringUtil.withSingleQuoteJoin(in, ",") + ")");
        return this;
    }

    public SQL whereIn(boolean condition, String column, String[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("whereIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("whereIn", "in must have value");
        }
        return condition ? where(column + " IN (" + StringUtil.withSingleQuoteJoin(in, ",") + ")") : this;
    }

    public SQL whereNotIn(String column, short[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("whereNotIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("whereNotIn", "in must have value");
        }
        where(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL whereNotIn(boolean condition, String column, short[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("whereNotIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("whereNotIn", "in must have value");
        }
        return condition ? where(column + " NOT IN (" + StringUtil.join(in, ",") + ")") : this;
    }

    public SQL whereNotIn(String column, int[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("whereNotIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("whereNotIn", "in must have value");
        }
        where(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL whereNotIn(boolean condition, String column, int[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("whereNotIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("whereNotIn", "in must have value");
        }
        return condition ? where(column + " NOT IN (" + StringUtil.join(in, ",") + ")") : this;
    }

    public SQL whereNotIn(String column, long[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("whereNotIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("whereNotIn", "in must have value");
        }
        where(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL whereNotIn(boolean condition, String column, long[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("whereNotIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("whereNotIn", "in must have value");
        }
        return condition ? where(column + " NOT IN (" + StringUtil.join(in, ",") + ")") : this;
    }

    public SQL whereNotIn(String column, String[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("whereNotIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("whereNotIn", "in must have value");
        }
        where(column + " NOT IN (" + StringUtil.withSingleQuoteJoin(in, ",") + ")");
        return this;
    }

    public SQL whereNotIn(boolean condition, String column, String[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("whereNotIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("whereNotIn", "in must have value");
        }
        return condition ? where(column + " NOT IN (" + StringUtil.withSingleQuoteJoin(in, ",") + ")") : this;
    }

    public SQL and(String and) {
        if (StringUtils.isBlank(and)) {
            throw new SqlPreFlightException("and", "and must have text");
        }
        and(new Where(and, AND));
        return this;
    }

    public SQL and(boolean condition, String and) {
        return condition ? and(and) : this;
    }

    public SQL and(boolean condition, String and, String alternate) {
        return and(condition ? and : alternate);
    }

    public SQL andIn(String column, byte[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("andIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("andIn", "in must have value");
        }
        and(column + " IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL andIn(boolean condition, String column, byte[] in) {
        return condition ? andIn(column, in) : this;
    }

    public SQL andIn(String column, short[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("andIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("andIn", "in must have value");
        }
        and(column + " IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL andIn(boolean condition, String column, short[] in) {
        return condition ? andIn(column, in) : this;
    }

    public SQL andIn(String column, int[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("andIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("andIn", "in must have value");
        }
        and(column + " IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL andIn(boolean condition, String column, int[] in) {
        return condition ? andIn(column, in) : this;
    }

    public SQL andIn(String column, float[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("andIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("andIn", "in must have value");
        }
        and(column + " IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL andIn(boolean condition, String column, float[] in) {
        return condition ? andIn(column, in) : this;
    }

    public SQL andIn(String column, long[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("andIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("andIn", "in must have value");
        }
        and(column + " IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL andIn(boolean condition, String column, long[] in) {
        return condition ? andIn(column, in) : this;
    }

    public SQL andIn(String column, double[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("andIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("andIn", "in must have value");
        }
        and(column + " IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL andIn(boolean condition, String column, double[] in) {
        return condition ? andIn(column, in) : this;
    }

    public SQL andIn(String column, String[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("andIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("andIn", "in must have value");
        }
        and(column + " IN (" + StringUtil.withSingleQuoteJoin(in, ",") + ")");
        return this;
    }

    public SQL andIn(boolean condition, String column, String[] in) {
        return condition ? andIn(column, in) : this;
    }

    public SQL andNotIn(String column, byte[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("andNotIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("andNotIn", "in must have value");
        }
        and(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL andNotIn(boolean condition, String column, byte[] in) {
        return condition ? andNotIn(column, in) : this;
    }

    public SQL andNotIn(String column, short[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("andNotIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("andNotIn", "in must have value");
        }
        and(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL andNotIn(boolean condition, String column, short[] in) {
        return condition ? andNotIn(column, in) : this;
    }

    public SQL andNotIn(String column, int[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("andNotIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("andNotIn", "in must have value");
        }
        and(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL andNotIn(boolean condition, String column, int[] in) {
        return condition ? andNotIn(column, in) : this;
    }

    public SQL andNotIn(String column, float[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("andNotIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("andNotIn", "in must have value");
        }
        and(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL andNotIn(boolean condition, String column, float[] in) {
        return condition ? andNotIn(column, in) : this;
    }

    public SQL andNotIn(String column, long[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("andNotIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("andNotIn", "in must have value");
        }
        and(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL andNotIn(boolean condition, String column, long[] in) {
        return condition ? andNotIn(column, in) : this;
    }

    public SQL andNotIn(String column, double[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("andNotIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("andNotIn", "in must have value");
        }
        and(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL andNotIn(boolean condition, String column, double[] in) {
        return condition ? andNotIn(column, in) : this;
    }

    public SQL andNotIn(String column, String[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("andNotIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("andNotIn", "in must have value");
        }
        and(column + " NOT IN (" + StringUtil.withSingleQuoteJoin(in, ",") + ")");
        return this;
    }

    public SQL andNotIn(boolean condition, String column, String[] in) {
        return condition ? andNotIn(column, in) : this;
    }

    public SQL and(Where where) {
        if (null == where) {
            throw new SqlPreFlightException("and", "where can't be null");
        }
        where.whereEnum = AND;
        this.where.add(where);
        return this;
    }

    public SQL and(boolean condition, Where where) {
        return condition ? and(where) : this;
    }

    public SQL or(String or) {
        if (StringUtils.isBlank(or)) {
            throw new SqlPreFlightException("or", "or must have text");
        }
        or(new Where(or, OR));
        return this;
    }

    public SQL or(boolean condition, String or) {
        return condition ? or(or) : this;
    }

    public SQL or(boolean condition, String or, String alternate) {
        return or(condition ? or : alternate);
    }

    public SQL orIn(String column, byte[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("orIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("orIn", "in must have value");
        }
        or(column + " IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL orIn(boolean condition, String column, byte[] in) {
        return condition ? orIn(column, in) : this;
    }

    public SQL orIn(String column, short[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("orIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("orIn", "in must have value");
        }
        or(column + " IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL orIn(boolean condition, String column, short[] in) {
        return condition ? orIn(column, in) : this;
    }

    public SQL orIn(String column, int[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("orIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("orIn", "in must have value");
        }
        or(column + " IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL orIn(boolean condition, String column, int[] in) {
        return condition ? orIn(column, in) : this;
    }

    public SQL orIn(String column, float[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("orIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("orIn", "in must have value");
        }
        or(column + " IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL orIn(boolean condition, String column, float[] in) {
        return condition ? orIn(column, in) : this;
    }

    public SQL orIn(String column, long[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("orIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("orIn", "in must have value");
        }
        or(column + " IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL orIn(boolean condition, String column, long[] in) {
        return condition ? orIn(column, in) : this;
    }

    public SQL orIn(String column, double[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("orIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("orIn", "in must have value");
        }
        or(column + " IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL orIn(boolean condition, String column, double[] in) {
        return condition ? orIn(column, in) : this;
    }

    public SQL orIn(String column, String[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("orIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("orIn", "in must have value");
        }
        or(column + " IN (" + StringUtil.withSingleQuoteJoin(in, ",") + ")");
        return this;
    }

    public SQL orIn(boolean condition, String column, String[] in) {
        return condition ? orIn(column, in) : this;
    }

    public SQL orNotIn(String column, byte[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("orNotIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("orNotIn", "in must have value");
        }
        or(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL orNotIn(boolean condition, String column, byte[] in) {
        return condition ? orNotIn(column, in) : this;
    }

    public SQL orNotIn(String column, short[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("orNotIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("orNotIn", "in must have value");
        }
        or(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL orNotIn(boolean condition, String column, short[] in) {
        return condition ? orNotIn(column, in) : this;
    }

    public SQL orNotIn(String column, int[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("orNotIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("orNotIn", "in must have value");
        }
        or(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL orNotIn(boolean condition, String column, int[] in) {
        return condition ? orNotIn(column, in) : this;
    }

    public SQL orNotIn(String column, float[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("orNotIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("orNotIn", "in must have value");
        }
        or(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL orNotIn(boolean condition, String column, float[] in) {
        return condition ? orNotIn(column, in) : this;
    }

    public SQL orNotIn(String column, long[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("orNotIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("orNotIn", "in must have value");
        }
        or(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL orNotIn(boolean condition, String column, long[] in) {
        return condition ? orNotIn(column, in) : this;
    }

    public SQL orNotIn(String column, double[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("orNotIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("orNotIn", "in must have value");
        }
        or(column + " NOT IN (" + StringUtil.join(in, ",") + ")");
        return this;
    }

    public SQL orNotIn(boolean condition, String column, double[] in) {
        return condition ? orNotIn(column, in) : this;
    }

    public SQL orNotIn(String column, String[] in) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("orNotIn", "column must have text");
        }
        if (ArrayUtils.isEmpty(in)) {
            throw new SqlPreFlightException("orNotIn", "in must have value");
        }
        or(column + " NOT IN (" + StringUtil.withSingleQuoteJoin(in, ",") + ")");
        return this;
    }

    public SQL orNotIn(boolean condition, String column, String[] in) {
        return condition ? orNotIn(column, in) : this;
    }

    public SQL or(Where or) {
        if (or == null) {
            throw new SqlPreFlightException("or", "or can't be null");
        }
        or.whereEnum = OR;
        this.where.add(or);
        return this;
    }

    public SQL or(boolean condition, Where or) {
        return condition ? or(or) : this;
    }

    public SQL groupBy(String groupBy) {
        if (StringUtils.isNotBlank(groupBy)) {
            this.groupBy.add(groupBy);
        }
        return this;
    }

    public SQL groupBy(boolean condition, String groupBy) {
        return condition ? groupBy(groupBy) : this;
    }

    public SQL groupBy(boolean condition, String groupBy, String alternate) {
        return groupBy(condition ? groupBy : alternate);
    }

    public SQL groupBy(SqlAssert groupByAssert) {
        boolean condition = groupByAssert.check();
        String groupBy = groupByAssert.create();
        return groupBy(condition, groupBy);
    }

    public SQL having(String having) {
        if (StringUtils.isNotBlank(having)) {
            this.having.add(having);
        }
        return this;
    }

    public SQL having(boolean condition, String having) {
        return condition ? having(having) : this;
    }

    public SQL having(boolean condition, String having, String alternate) {
        return having(condition ? having : alternate);
    }

    public SQL having(SqlAssert havingAssert) {
        boolean condition = havingAssert.check();
        String having = havingAssert.create();
        return having(condition, having);
    }

    public SQL orderBy(String orderBy) {
        if (StringUtils.isNotBlank(orderBy)) {
            this.orderBy.add(orderBy);
        }
        return this;
    }

    public SQL orderBy(boolean condition, String orderBy) {
        return condition ? orderBy(orderBy) : this;
    }

    public SQL orderBy(boolean condition, String orderBy, String alternate) {
        return orderBy(condition ? orderBy : alternate);
    }

    public SQL orderBy(SqlAssert orderByAssert) {
        boolean condition = orderByAssert.check();
        String orderBy = orderByAssert.create();
        return orderBy(condition, orderBy);
    }

    public SQL limit(int position, int offSet) {
        if (position < 0 || offSet < 0) {
            throw new IllegalArgumentException("LIMIT起始位置和偏移量不能为空");
        }
        this.limit = new int[]{position, offSet};
        return this;
    }

    public SQL limit(boolean condition, int position, int offSet) {
        return condition ? limit(position, offSet) : this;
    }

    public SQL limit(SqlAssert limitAssert) {
        boolean condition = limitAssert.check();
        String[] limit = limitAssert.createArr();
        try {
            int position = Integer.valueOf(limit[0]);
            int offSet = Integer.valueOf(limit[1]);
            limit(condition, position, offSet);
        } catch (Exception ex) {
            throw new IllegalArgumentException("分页数据错误", ex);
        }
        return this;
    }

    public SQL limit(Page page) {
        if (page == null) {
            throw new IllegalArgumentException("分页对象不能为空");
        }
        this.limit = page.paginate();
        if (limit[0] >= page.getTotal()) {
            throw new IllegalStateException("超出查询范围");
        }
        return this;
    }

    public SQL limit(boolean condition, Page page) {
        return condition ? limit(page) : this;
    }

    public SQL insert(String table) {
        if (StringUtils.isNotBlank(table)) {
            this.insert = table;
        }
        return this;
    }

    public SQL insert(boolean condition, String insert) {
        return condition ? insert(insert) : this;
    }

    public SQL insert(boolean condition, String insert, String alternate) {
        return insert(condition ? insert : alternate);
    }

    public SQL insert(SqlAssert insertAssert) {
        boolean condition = insertAssert.check();
        String insert = insertAssert.create();
        return insert(condition, insert);
    }

    /**
     * Attention: 1:新增不冲突的数据,成功返回1;2:新增冲突的数据,成功返回0
     */
    public SQL insertIgnore(String table) {
        if (StringUtils.isNotBlank(table)) {
            this.insertIgnore = table;
        }
        return this;
    }

    /**
     * Attention: 1:新增不冲突的数据,成功返回1;2:新增冲突的数据,成功返回0
     */
    public SQL insertIgnore(boolean condition, String insert) {
        return condition ? insertIgnore(insert) : this;
    }

    /**
     * Attention: 1:新增不冲突的数据,成功返回1;2:新增冲突的数据,成功返回0
     */
    public SQL insertIgnore(boolean condition, String insert, String alternate) {
        return insertIgnore(condition ? insert : alternate);
    }

    /**
     * Attention: 1:新增不冲突的数据,成功返回1;2:新增冲突的数据,成功返回0
     */
    public SQL insertIgnore(SqlAssert insertAssert) {
        boolean condition = insertAssert.check();
        String insert = insertAssert.create();
        return insertIgnore(condition, insert);
    }

    /**
     * Attention: 1:新增不冲突的数据,成功返回1;2:新增冲突的数据,但前后数据不一致,成功返回2;3:新增冲突的数据,数据完全一致,成功返回1
     */
    public SQL replace(String table) {
        if (StringUtils.isNotBlank(table)) {
            this.replace = table;
        }
        return this;
    }

    /**
     * Attention: 1:新增不冲突的数据,成功返回1;2:新增冲突的数据,但前后数据不一致,成功返回2;3:新增冲突的数据,数据完全一致,成功返回1
     */
    public SQL replace(boolean condition, String replace) {
        return condition ? replace(replace) : this;
    }

    /**
     * Attention: 1:新增不冲突的数据,成功返回1;2:新增冲突的数据,但前后数据不一致,成功返回2;3:新增冲突的数据,数据完全一致,成功返回1
     */
    public SQL replace(boolean condition, String replace, String alternate) {
        return replace(condition ? replace : alternate);
    }

    /**
     * Attention: 1:新增不冲突的数据,成功返回1;2:新增冲突的数据,但前后数据不一致,成功返回2;3:新增冲突的数据,数据完全一致,成功返回1
     */
    public SQL replace(SqlAssert replaceAssert) {
        boolean condition = replaceAssert.check();
        String replace = replaceAssert.create();
        return replace(condition, replace);
    }

    public SQL values(String column, String value) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("values", "column must have text");
        }
        if (StringUtils.isBlank(value)) {
            throw new SqlPreFlightException("values", "value must have text");
        }
        if (StringUtils.isNotBlank(column) && StringUtils.isNotBlank(value)) {
            this.values.add(new String[]{column, value});
        }
        return this;
    }

    public SQL values(boolean condition, String column, String value) {
        return condition ? values(column, value) : this;
    }

    public SQL values(SqlAssert valuesAssert) {
        boolean condition = valuesAssert.check();
        String[] values = valuesAssert.createArr();
        try {
            String column = values[0];
            String value = values[1];
            values(condition, column, value);
        } catch (Exception ex) {
            throw new IllegalArgumentException("insert数据错误", ex);
        }
        return this;
    }

    /**
     * Attention: 1:新增不冲突的数据,成功返回1;2:新增冲突的数据,成功返回2
     */
    public SQL duplicateUpdate(String column, String value) {
        if (StringUtils.isNotBlank(column) && StringUtils.isNotBlank(value)) {
            this.duplicateUpdate.add(new String[]{column, value});
        }
        return this;
    }

    /**
     * Attention: 1:新增不冲突的数据,成功返回1;2:新增冲突的数据,成功返回2
     */
    public SQL duplicateUpdate(boolean condition, String column, String value) {
        return condition ? duplicateUpdate(column, value) : this;
    }

    /**
     * Attention: 1:新增不冲突的数据,成功返回1;2:新增冲突的数据,成功返回2
     */
    public SQL duplicateUpdateValues(String column, String value) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("duplicateUpdateValues", "column must have text");
        }
        if (StringUtils.isBlank(value)) {
            throw new SqlPreFlightException("duplicateUpdateValues", "value must have text");
        }
        if (StringUtils.isNotBlank(column) && StringUtils.isNotBlank(value)) {
            this.duplicateUpdate.add(new String[]{column, DUPLICATE_UPDATE_VALUES + "(" + value + ")"});
        }
        return this;
    }

    /**
     * Attention: 1:新增不冲突的数据,成功返回1;2:新增冲突的数据,成功返回2
     */
    public SQL duplicateUpdateValues(boolean condition, String column, String value) {
        return condition ? duplicateUpdateValues(column, value) : this;
    }

    public SQL update(String table) {
        if (StringUtils.isBlank(table)) {
            throw new SqlPreFlightException("update", "table must have text");
        }
        if (StringUtils.isNotBlank(table)) {
            this.update = table;
        }
        return this;
    }

    public SQL update(boolean condition, String table) {
        return condition ? update(table) : this;
    }

    public SQL update(boolean condition, String table, String alternate) {
        return update(condition ? table : alternate);
    }

    public SQL update(SqlAssert updateAssert) {
        boolean condition = updateAssert.check();
        String table = updateAssert.create();
        return update(condition, table);
    }

    public SQL set(String column, String value) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("set", "column must have text");
        }
        if (StringUtils.isBlank(value)) {
            throw new SqlPreFlightException("set", "value must have text");
        }
        if (StringUtils.isNotBlank(column) && StringUtils.isNotBlank(value)) {
            this.set.add(new String[]{column, value});
        }
        return this;
    }

    public SQL set(boolean condition, String column, String value) {
        return condition ? set(column, value) : this;
    }

    public SQL set(SqlAssert setAssert) {
        boolean condition = setAssert.check();
        String[] set = setAssert.createArr();
        try {
            String column = set[0];
            String value = set[1];
            set(condition, column, value);
        } catch (Exception ex) {
            throw new IllegalArgumentException("set数据错误", ex);
        }
        return this;
    }

    public SQL delete() {
        return delete("");
    }

    public SQL delete(String table) {
        if (StringUtils.isBlank(table)) {
            throw new SqlPreFlightException("delete", "table must have text");
        }
        deleteCommand = true;
        if (StringUtils.isNotBlank(table)) {
            this.delete = table;
        }
        return this;
    }

    public SQL delete(boolean condition, String table) {
        return condition ? delete(table) : this;
    }

    public SQL delete(boolean condition, String table, String alternate) {
        return delete(condition ? table : alternate);
    }

    public SQL delete(SqlAssert deleteAssert) {
        boolean condition = deleteAssert.check();
        String table = deleteAssert.create();
        return delete(condition, table);
    }

    /**
     * sql处理方法,把当前sql对象调用权限转移给调用者，可以直接操作当前sql对象
     *
     * @param sqlProcessor sql处理类
     * @return 当前sql对象
     */
    public SQL sqlProcess(SqlProcessor sqlProcessor) {
        sqlProcessor.process(this);
        return this;
    }

    /**
     * 生成sql字符串
     */
    public String build() {
        boolean hasHead = StringUtils.isNotBlank(head);
        ActionType[] actionTypes = ActionType.values();
        List<String> sqls = new ArrayList<>(ActionType.values().length + (hasHead ? 1 : 0));
        if (hasHead) {
            sqls.add(head);
        }
        for (ActionType actionType : actionTypes) {
            sqls.add(createSql(actionType));
        }
        sqls.removeIf(StringUtils::isBlank);
        return com.sun.deploy.util.StringUtils.join(sqls, " ");
    }

    public <T> String foreach(List<T> params, SqlIterator<T> sqlIterator) {
        if (CollectionUtils.isEmpty(params)) {
            throw new SqlPreFlightException("foreach", "params can't be empty");
        }
        String[] sqls = collectionIterate(params, sqlIterator);
        return StringUtils.join(sqls, ITERATOR_SEPARATOR);
    }

    public <T> String foreach(T[] params, SqlIterator<T> sqlIterator) {
        if (ArrayUtils.isEmpty(params)) {
            throw new SqlPreFlightException("foreach", "params can't be empty");
        }
        int length = params.length;
        //Assert.state(length <= ITERATOR_MAX_LENGTH, "sql太长,请使用批量操作类:" + BatchDao.class.getName());
        String[] sqls = new String[length];
        for (int i = 0; i < length; i++) {
            T param = params[i];
            SQL sql = sqlIterator.iterate(param, i);
            sqls[i] = sql.build();
        }
        return StringUtils.join(sqls, ITERATOR_SEPARATOR);
    }

    public <T> String foreach(List<T> params, ValuesIterator<T> valuesIterator) {
        if (CollectionUtils.isEmpty(params)) {
            throw new SqlPreFlightException("foreach", "params can't be empty");
        }
        int size = params.size();
        //Assert.state(size <= ITERATOR_MAX_LENGTH, "sql太长,请使用批量操作类:" + BatchDao.class.getName());
        String name = "";
        String[] values = new String[size];
        for (int i = 0; i < size; i++) {
            T param = params.get(i);
            Map<String, String> valuesMap = valuesIterator.values(param, i);
            if (i == 0) {
                name = mapIterate(valuesMap);
            }
            StringBuilder valueBuilder = new StringBuilder();
            Collection<String> valueCollection = valuesMap.values();
            for (String value : valueCollection) {
                valueBuilder.append(value).append(",");
            }
            values[i] = "(" + valueBuilder.substring(0, valueBuilder.lastIndexOf(",")) + ")";
        }
        String sql = build() + "(" + name + ")" + " VALUES " + StringUtils.join(values, ",");
        return sql;
    }

    public <T> String foreach(T[] params, ValuesIterator<T> valuesIterator) {
        if (ArrayUtils.isEmpty(params)) {
            throw new SqlPreFlightException("foreach", "params can't be empty");
        }
        int size = params.length;
        //Assert.state(size <= ITERATOR_MAX_LENGTH, "sql太长,请使用批量操作类:" + BatchDao.class.getName());
        String name = "";
        String[] values = new String[size];
        for (int i = 0; i < size; i++) {
            T param = params[i];
            Map<String, String> valuesMap = valuesIterator.values(param, i);
            if (i == 0) {
                name = mapIterate(valuesMap);
            }
            StringBuilder valueBuilder = new StringBuilder();
            Collection<String> valueCollection = valuesMap.values();
            for (String value : valueCollection) {
                valueBuilder.append(value).append(",");
            }
            values[i] = "(" + valueBuilder.substring(0, valueBuilder.lastIndexOf(",")) + ")";
        }
        String sql = build() + "(" + name + ")" + " VALUES " + StringUtils.join(values, ",");
        return sql;
    }

    public <T> String foreachUnion(List<T> params, SqlIterator<T> unionIterator) {
        if (CollectionUtils.isEmpty(params)) {
            throw new SqlPreFlightException("foreachUnion", "params can't be empty");
        }
        String[] sqls = collectionIterate(params, unionIterator);
        return StringUtils.join(sqls, UNION_SEPARATOR);
    }

    public SQL shareLock() {
        this.lock = Lock.SHARE_LOCK;
        return this;
    }

    public SQL excludeLock() {
        this.lock = Lock.EXCLUDE_LOCK;
        return this;
    }

    /**
     * 显示调用此方法,关闭update/delete不允许没有where条件的限制
     */
    public SQL noWhere() {
        this.permitNoWhere = true;
        return this;
    }

    public SQL union(SQL union) {
        if (union != null) {
            this.union.add(union);
        }
        return this;
    }

    public SQL unionAll(SQL union) {
        if (union != null) {
            this.unionAll.add(union);
        }
        return this;
    }

    public SQL selectCount() {
        return select("COUNT(*)");
    }

    public SQL selectCount(String countColumn) {
        if (StringUtils.isBlank(countColumn)) {
            throw new SqlPreFlightException("selectCount", "countColumn must have text");
        }
        return select("COUNT(" + countColumn + ")");
    }

    public SQL selectCount(String countColumn, String alias) {
        if (StringUtils.isBlank(countColumn)) {
            throw new SqlPreFlightException("selectCount", "countColumn must have text");
        }
        if (StringUtils.isBlank(alias)) {
            throw new SqlPreFlightException("selectCount", "alias must have text");
        }
        return select("COUNT(" + countColumn + ") AS " + alias);
    }

    public SQL selectCountDistinct(String countColumn) {
        if (StringUtils.isBlank(countColumn)) {
            throw new SqlPreFlightException("selectCountDistinct", "countColumn must have text");
        }
        return select("COUNT( DISTINCT " + countColumn + ")");
    }

    public SQL selectCount(boolean condition, String countColumn) {
        if (StringUtils.isBlank(countColumn)) {
            throw new SqlPreFlightException("selectCount", "countColumn must have text");
        }
        return condition ? selectCount(countColumn) : this;
    }

    public SQL selectCount(boolean condition, String countColumn, String alias) {
        if (StringUtils.isBlank(countColumn)) {
            throw new SqlPreFlightException("selectCount", "countColumn must have text");
        }
        if (StringUtils.isBlank(alias)) {
            throw new SqlPreFlightException("selectCount", "alias must have text");
        }
        return condition ? selectCount(countColumn, alias) : this;
    }

    public SQL selectCountDistinct(boolean condition, String countColumns) {
        return condition ? selectCountDistinct(countColumns) : this;
    }

    public SQL selectIf(String condition, String firstReturn, String elseReturn, String alias) {
        if (StringUtils.isBlank(condition)) {
            throw new SqlPreFlightException("selectIf", "condition must have text");
        }
        if (StringUtils.isBlank(firstReturn)) {
            throw new SqlPreFlightException("selectIf", "firstReturn must have text");
        }
        if (StringUtils.isBlank(elseReturn)) {
            throw new SqlPreFlightException("selectIf", "elseReturn must have text");
        }
        if (StringUtils.isBlank(alias)) {
            throw new SqlPreFlightException("selectIf", "alias must have text");
        }
        return select("IF(" + condition + "," + firstReturn + "," + elseReturn + ") AS " + alias);
    }

    public SQL orderByAsc(String orderBy) {
        if (StringUtils.isBlank(orderBy)) {
            throw new SqlPreFlightException("orderByAsc", "orderBy must have text");
        }
        return orderBy(orderBy + " ASC");
    }

    public SQL orderByAsc(boolean condition, String orderBy) {
        return condition ? orderByAsc(orderBy) : this;
    }

    public SQL orderByDesc(String orderBy) {
        if (StringUtils.isBlank(orderBy)) {
            throw new SqlPreFlightException("orderByDesc", "orderBy must have text");
        }
        return orderBy(orderBy + " DESC");
    }

    public SQL orderByDesc(boolean condition, String orderBy) {
        return condition ? orderByDesc(orderBy) : this;
    }

    public SQL whereLike(String column, String like) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("whereLike", "column must have text");
        }
        if (StringUtils.isBlank(like)) {
            throw new SqlPreFlightException("whereLike", "like must have text");
        }
        return where(column + " LIKE CONCAT('" + like + "%')");
    }

    public SQL whereLike(boolean condition, String column, String like) {
        return condition ? whereLike(column, like) : this;
    }

    public SQL andLike(String column, String like) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("andLike", "column must have text");
        }
        if (StringUtils.isBlank(like)) {
            throw new SqlPreFlightException("andLike", "like must have text");
        }
        return and(column + " LIKE CONCAT('" + like + "%')");
    }

    public SQL orLike(boolean condition, String column, String like) {
        return condition ? orLike(column, like) : this;
    }

    public SQL orLike(String column, String like) {
        if (StringUtils.isBlank(column)) {
            throw new SqlPreFlightException("orLike", "column must have text");
        }
        if (StringUtils.isBlank(like)) {
            throw new SqlPreFlightException("orLike", "like must have text");
        }
        return or(column + " LIKE CONCAT('" + like + "%')");
    }

    public SQL andLike(boolean condition, String column, String like) {
        return condition ? andLike(column, like) : this;
    }

    /**
     * 前置sql
     *
     * @param sql sql
     */
    public SQL head(String sql) {
        this.head = sql;
        return this;
    }

    private <T> String[] collectionIterate(List<T> params, SqlIterator<T> sqlIterator) {
        int size = params.size();
        //Assert.state(size <= ITERATOR_MAX_LENGTH, "sql太长,请使用批量操作类:" + BatchDao.class.getName());
        String[] sqls = new String[size];
        for (int i = 0; i < size; i++) {
            T param = params.get(i);
            SQL sql = sqlIterator.iterate(param, i);
            sqls[i] = sql.build();
        }
        return sqls;
    }

    private String mapIterate(Map<String, String> valuesMap) {
        StringBuilder nameBuilder = new StringBuilder();
        Set<String> keySet = valuesMap.keySet();
        for (String key : keySet) {
            nameBuilder.append(key).append(",");
        }
        return nameBuilder.substring(0, nameBuilder.lastIndexOf(","));
    }


    enum ActionType {
        INSERT, INSERT_IGNORE, REPLACE, SELECT, VALUES, DUPLICATE_UPDATE, UPDATE, DELETE, FROM, LEFT_JOIN, INNER_JOIN, JOIN, SET, WHERE, GROUP_BY, HAVING, ORDER_BY, LIMIT, LOCK, UNION, UNION_ALL
    }

    private class JoinSql {
        private final ActionType actionType;
        private final String sql;

        private JoinSql(ActionType actionType, String sql) {
            this.actionType = actionType;
            this.sql = sql;
        }

        private ActionType getActionType() {
            return actionType;
        }

        private String getSql() {
            return sql;
        }
    }

    enum Lock {
        /**
         * 共享锁
         */
        SHARE_LOCK("LOCK IN SHARE MODE"),
        /**
         * 排他锁
         */
        EXCLUDE_LOCK("FOR UPDATE");
        private final String lock;

        Lock(String lock) {
            this.lock = lock;
        }

        public String getLock() {
            return lock;
        }
    }

}
