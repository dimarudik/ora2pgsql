package org.example.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.model.Chunk;
import org.example.model.ColumnRule;
import org.example.model.SQLStatement;

import java.sql.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.example.util.SQLUtil.buildStartEndRowIdOfChunkStatement;

public class ColumnUtil {
    private static final Logger logger = LogManager.getLogger(ColumnUtil.class);

    public static List<String> readSourceColumnsFromDB(Connection connection, SQLStatement sqlStatement) {
        List<String> columnList = new ArrayList<>();
        ResultSet resultSet;
        try {
            resultSet = connection.getMetaData().getColumns(
                    null,
                    sqlStatement.getFromSchemaName(),
                    sqlStatement.getFromTableName().replaceAll("^\"|\"$", ""),
                    null
            );
            while (resultSet.next()) {
                columnList.add(resultSet.getString(4));
            }
            resultSet.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return columnList;
    }

    public static List<String> readTargetColumnsFromDB(Connection connection, SQLStatement sqlStatement) {
        List<String> typeList = new ArrayList<>();
        List<String> columnList = new ArrayList<>();
        ResultSet resultSet;
        try {
            resultSet = connection.getMetaData().getColumns(
                    null,
                    sqlStatement.getToSchemaName().toLowerCase(),
                    sqlStatement.getToTableName().toLowerCase(),
                    null
            );
            while (resultSet.next()) {
                typeList.add(resultSet.getString(6));
                columnList.add(resultSet.getString(4));
            }
            resultSet.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        sqlStatement.setTargetColumnTypes(typeList);
        return columnList;
    }

    public static Map<Integer, ColumnRule> getColumn2RuleMap(SQLStatement sqlStatement) {
        Map<Integer, ColumnRule> ruleMap = new HashMap<>();
        int i = 1;
        for (String fieldName: sqlStatement.getNeededSourceColumns()) {
            Optional<ColumnRule> columnRule = Optional.empty();
            if (sqlStatement.getTransformToRule() != null) {
                columnRule =
                        sqlStatement.getTransformToRule()
                                .stream()
                                .filter(rule -> rule.getColumnName().equalsIgnoreCase(fieldName))
                                .findFirst();
            }
            assert Objects.requireNonNull(columnRule).isPresent();
            ruleMap.put(i, columnRule.orElse(null));
            i++;
        }
        return ruleMap;
    }

    static HashMap<Integer, Chunk> getStartEndRowIdMap(Connection connection, SQLStatement sqlStatement) {
        HashMap<Integer, Chunk> map = new HashMap<>();
        try {
            PreparedStatement statement = connection.prepareStatement(buildStartEndRowIdOfChunkStatement());
            statement.setString(1, sqlStatement.getFromTaskName());
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                map.put(resultSet.getInt(1),
                        new Chunk(
                                resultSet.getInt(1),
                                resultSet.getString(2),
                                resultSet.getString(3),
                                resultSet.getLong(4),
                                resultSet.getLong(5)
                        )
                );
            }
            resultSet.close();
            statement.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return map;
    }

    public static byte[] convertBlobToBytes(ResultSet resultSet, int i) throws SQLException {
        Blob blob = resultSet.getBlob(i);
        int blobLength = (int) blob.length();
        return blob.getBytes(1, blobLength);
    }

    public static String convertClobToBytes(ResultSet resultSet, int i) throws SQLException {
        Clob clob = resultSet.getClob(i);
        int clobLength = (int) clob.length();
        return clob.getSubString(1L, clobLength);
    }
}
