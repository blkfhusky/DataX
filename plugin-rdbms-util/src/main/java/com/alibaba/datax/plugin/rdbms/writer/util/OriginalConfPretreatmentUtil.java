package com.alibaba.datax.plugin.rdbms.writer.util;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.util.ListUtil;
import com.alibaba.datax.plugin.rdbms.util.*;
import com.alibaba.datax.plugin.rdbms.writer.Constant;
import com.alibaba.datax.plugin.rdbms.writer.Key;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public final class OriginalConfPretreatmentUtil {
    private static final Logger LOG = LoggerFactory
            .getLogger(OriginalConfPretreatmentUtil.class);

    public static DataBaseType DATABASE_TYPE;

//    public static void doPretreatment(Configuration originalConfig) {
//        doPretreatment(originalConfig,null);
//    }

    public static void doPretreatment(Configuration originalConfig, DataBaseType dataBaseType) {
        // 检查 username/password 配置（必填）
        originalConfig.getNecessaryValue(Key.USERNAME, DBUtilErrorCode.REQUIRED_VALUE);
        originalConfig.getNecessaryValue(Key.PASSWORD, DBUtilErrorCode.REQUIRED_VALUE);

        doCheckBatchSize(originalConfig);

        simplifyConf(originalConfig);

        /*
        为兼容在没有表时创建表的操作，需校验传入的table和preSql是否匹配，如果匹配则说明数据库中尚无该表，可跳过dealColumnConf
        预处理配置文件列信息，会和数据库中的表做对比，表不存在时会报错,包含下列操作
        1. 将*替换为从数据库中查出的所有列名，并log.warn
        2. 校验传入列长度是否大于数据库表列长度
        3. 判断传入的列是否有重复的，有重复的报错
        4. 判断传入的所有列是否在表中都存在，有不存在的报错
         */
        if (!matchTableAndCreateSql(originalConfig)) {
            dealColumnConf(originalConfig);
        }
        dealWriteMode(originalConfig, dataBaseType);
    }

    public static void doCheckBatchSize(Configuration originalConfig) {
        // 检查batchSize 配置（选填，如果未填写，则设置为默认值）
        int batchSize = originalConfig.getInt(Key.BATCH_SIZE, Constant.DEFAULT_BATCH_SIZE);
        if (batchSize < 1) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE, String.format(
                    "您的batchSize配置有误. 您所配置的写入数据库表的 batchSize:%s 不能小于1. 推荐配置范围为：[100-1000], 该值越大, 内存溢出可能性越大. 请检查您的配置并作出修改.",
                    batchSize));
        }

        originalConfig.set(Key.BATCH_SIZE, batchSize);
    }

    public static void simplifyConf(Configuration originalConfig) {
        List<Object> connections = originalConfig.getList(Constant.CONN_MARK,
                Object.class);

        int tableNum = 0;

        for (int i = 0, len = connections.size(); i < len; i++) {
            Configuration connConf = Configuration.from(connections.get(i).toString());

            String jdbcUrl = connConf.getString(Key.JDBC_URL);
            if (StringUtils.isBlank(jdbcUrl)) {
                throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE, "您未配置的写入数据库表的 jdbcUrl.");
            }

            jdbcUrl = DATABASE_TYPE.appendJDBCSuffixForReader(jdbcUrl);
            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK, i, Key.JDBC_URL),
                    jdbcUrl);

            List<String> tables = connConf.getList(Key.TABLE, String.class);

            if (null == tables || tables.isEmpty()) {
                throw DataXException.asDataXException(DBUtilErrorCode.REQUIRED_VALUE,
                        "您未配置写入数据库表的表名称. 根据配置DataX找不到您配置的表. 请检查您的配置并作出修改.");
            }

            // 对每一个connection 上配置的table 项进行解析
            List<String> expandedTables = TableExpandUtil
                    .expandTableConf(DATABASE_TYPE, tables);

            if (null == expandedTables || expandedTables.isEmpty()) {
                throw DataXException.asDataXException(DBUtilErrorCode.CONF_ERROR,
                        "您配置的写入数据库表名称错误. DataX找不到您配置的表，请检查您的配置并作出修改.");
            }

            tableNum += expandedTables.size();

            originalConfig.set(String.format("%s[%d].%s", Constant.CONN_MARK,
                    i, Key.TABLE), expandedTables);
        }

        originalConfig.set(Constant.TABLE_NUMBER_MARK, tableNum);
    }

    public static void dealColumnConf(Configuration originalConfig, ConnectionFactory connectionFactory, String oneTable) {
        List<String> userConfiguredColumns = originalConfig.getList(Key.COLUMN, String.class);
        if (null == userConfiguredColumns || userConfiguredColumns.isEmpty()) {
            throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                    "您的配置文件中的列配置信息有误. 因为您未配置写入数据库表的列名称，DataX获取不到列信息. 请检查您的配置并作出修改.");
        } else {
            boolean isPreCheck = originalConfig.getBool(Key.DRYRUN, false);
            List<String> allColumns;
            if (isPreCheck) {
                allColumns = DBUtil.getTableColumnsByConn(DATABASE_TYPE, connectionFactory.getConnecttionWithoutRetry(), oneTable, connectionFactory.getConnectionInfo());
            } else {
                allColumns = DBUtil.getTableColumnsByConn(DATABASE_TYPE, connectionFactory.getConnecttion(), oneTable, connectionFactory.getConnectionInfo());
            }

            LOG.info("table:[{}] all columns:[\n{}\n].", oneTable,
                    StringUtils.join(allColumns, ","));

            if (1 == userConfiguredColumns.size() && "*".equals(userConfiguredColumns.get(0))) {
                LOG.warn("您的配置文件中的列配置信息存在风险. 因为您配置的写入数据库表的列为*，当您的表字段个数、类型有变动时，可能影响任务正确性甚至会运行出错。请检查您的配置并作出修改.");

                // 回填其值，需要以 String 的方式转交后续处理
                originalConfig.set(Key.COLUMN, allColumns);
            } else if (userConfiguredColumns.size() > allColumns.size()) {
                throw DataXException.asDataXException(DBUtilErrorCode.ILLEGAL_VALUE,
                        String.format("您的配置文件中的列配置信息有误. 因为您所配置的写入数据库表的字段个数:%s 大于目的表的总字段总个数:%s. 请检查您的配置并作出修改.",
                                userConfiguredColumns.size(), allColumns.size()));
            } else {
                // 确保用户配置的 column 不重复
                ListUtil.makeSureNoValueDuplicate(userConfiguredColumns, false);

                // 检查列是否都为数据库表中正确的列（通过执行一次 select column from table 进行判断）
                DBUtil.getColumnMetaData(connectionFactory.getConnecttion(), oneTable, StringUtils.join(userConfiguredColumns, ","));
            }
        }
    }

    public static void dealColumnConf(Configuration originalConfig) {
        String jdbcUrl = originalConfig.getString(String.format("%s[0].%s",
                Constant.CONN_MARK, Key.JDBC_URL));

        String username = originalConfig.getString(Key.USERNAME);
        String password = originalConfig.getString(Key.PASSWORD);
        String oneTable = originalConfig.getString(String.format(
                "%s[0].%s[0]", Constant.CONN_MARK, Key.TABLE));

        JdbcConnectionFactory jdbcConnectionFactory = new JdbcConnectionFactory(DATABASE_TYPE, jdbcUrl, username, password);
        dealColumnConf(originalConfig, jdbcConnectionFactory, oneTable);
    }

    public static void dealWriteMode(Configuration originalConfig, DataBaseType dataBaseType) {
        List<String> columns = originalConfig.getList(Key.COLUMN, String.class);

        String jdbcUrl = originalConfig.getString(String.format("%s[0].%s",
                Constant.CONN_MARK, Key.JDBC_URL, String.class));

        // 默认为：insert 方式
        String writeMode = originalConfig.getString(Key.WRITE_MODE, "INSERT");

        List<String> valueHolders = new ArrayList<String>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            valueHolders.add("?");
        }

        boolean forceUseUpdate = false;
        //ob10的处理
        if (dataBaseType == DataBaseType.MySql && isOB10(jdbcUrl)) {
            forceUseUpdate = true;
        }

        String writeDataSqlTemplate = WriterUtil.getWriteTemplate(columns, valueHolders, writeMode, dataBaseType, forceUseUpdate);

        LOG.info("Write data [\n{}\n], which jdbcUrl like:[{}]", writeDataSqlTemplate, jdbcUrl);

        originalConfig.set(Constant.INSERT_OR_REPLACE_TEMPLATE_MARK, writeDataSqlTemplate);
    }

    public static boolean isOB10(String jdbcUrl) {
        //ob10的处理
        if (jdbcUrl.startsWith(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING)) {
            String[] ss = jdbcUrl.split(com.alibaba.datax.plugin.rdbms.writer.Constant.OB10_SPLIT_STRING_PATTERN);
            if (ss.length != 3) {
                throw DataXException
                        .asDataXException(
                                DBUtilErrorCode.JDBC_OB10_ADDRESS_ERROR, "JDBC OB10格式错误，请联系askdatax");
            }
            return true;
        }
        return false;
    }

    /**
     * 判断传入的table和建表语句是否符合，如果符合，可跳过列校验
     *
     * @param originalConfig 源配置
     * @return table是否存在
     */
    public static boolean matchTableAndCreateSql(Configuration originalConfig) {
        List<String> tables = originalConfig.getList(String.format(
                "%s[0].%s", Constant.CONN_MARK, Key.TABLE), String.class);
        List<String> preSqls = originalConfig.getList(Key.PRE_SQL, String.class);

        List<String> matchTables = new ArrayList<String>();
        for (String table : tables) {
            String reg = "(?i)create\\s+(?i)table([\\s\\S]*)" + table + "([\\s\\S]*)";
            for (String preSql : preSqls) {
                if (Pattern.matches(reg, preSql)) {
                    matchTables.add(table);
                    break;
                }
            }
        }
        LOG.info("table:{}, preSql:{}, matchSize:{}", tables, preSqls, matchTables.size());

        return tables.size() == matchTables.size();
    }


}
