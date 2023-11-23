package org.example.util;

import com.sun.source.tree.SynchronizedTree;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.example.model.Chunk;
import org.example.model.LogMessage;
import org.example.model.RunnerResult;
import org.example.model.SQLStatement;
import org.example.task.Runner;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.example.util.ColumnUtil.*;
import static org.example.util.SQLUtil.buildInsertStatement;

public class ProcessUtil {
    private static final Logger logger = LogManager.getLogger(ProcessUtil.class);

    public static void initiateProcessFromDatabase(Properties fromProperties,
                                                   Properties toProperties,
                                                   SQLStatement sqlStatement,
                                                   int threads) {
        // Надо переделать на Callable
        ExecutorService executorService = Executors.newFixedThreadPool(threads);
        try {
            Connection connection = DatabaseUtil.getConnection(fromProperties);
//            !!!!!!!!!!!!!!!!!!!!!!!!!!
//            connection.unwrap(oracle.jdbc.OracleConnection.class).setSchema(sqlStatement.getFromSchemaName());
//            !!!!!!!!!!!!!!!!!!!!!!!!!!
            sqlStatement.setSourceColumns(readSourceColumnsFromDB(connection, sqlStatement));
            sqlStatement.setColumn2Rule(getColumn2RuleMap(sqlStatement));
            TreeMap<Integer, Chunk> map = new TreeMap<>(getStartEndRowIdMap(connection, sqlStatement));
            logger.info(sqlStatement.getFromTableName() + " "  +
                    map.keySet().stream().min(Integer::compareTo).orElse(0) + " " +
                    map.keySet().stream().max(Integer::compareTo).orElse(0));
            // тут должен быть submit
            map.forEach((key, chunk) ->
                executorService.execute(new Runner(
                        fromProperties,
                        toProperties,
                        sqlStatement,
                        chunk)));
            executorService.shutdown();
            DatabaseUtil.closeConnection(connection);
        } catch (SQLException e) {
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    public static RunnerResult initiateProcessToDatabase(Properties toProperties, ResultSet fetchResultSet,
                                                         SQLStatement sqlStatement, Chunk chunk) {
        LogMessage logMessage = null;
        SQLException sqlException = null;
        SQLStatement threadSafeStatement = new SQLStatement(sqlStatement);
        try {
            if (fetchResultSet.next()) {
                Connection connection = DatabaseUtil.getConnection(toProperties);
                connection.setAutoCommit(false);
                threadSafeStatement.setTargetColumns(readTargetColumnsFromDB(connection, threadSafeStatement));
                PreparedStatement statement = connection.prepareStatement(buildInsertStatement(threadSafeStatement));
                long start = System.currentTimeMillis();
                int rowCount = 0;

                do {
                    for (int i = 1; i <= fetchResultSet.getMetaData().getColumnCount(); i++) {

                        if (threadSafeStatement.getColumn2Rule().get(i) == null) {

                            switch (fetchResultSet.getMetaData().getColumnType(i)) {
                                case 2004:
                                    if (fetchResultSet.getObject(i) != null) {
                                        statement.setBytes(i, convertBlobToBytes(fetchResultSet, i));
                                    } else {
                                        statement.setObject(i, null);
                                    }
                                    break;
                                case 2005:
                                    if (fetchResultSet.getObject(i) != null) {
                                        statement.setBytes(i, convertClobToBytes(fetchResultSet, i).getBytes());
                                    } else {
                                        statement.setObject(i, null);
                                    }
                                    break;
                                case 93:
                                    Date localDate = fetchResultSet.getTimestamp(i);
                                    statement.setObject(i, localDate);
                                    break;
                                default:
                                    statement.setObject(i, fetchResultSet.getObject(i));
                                    break;
                            }
                        } else {
                            switch (threadSafeStatement.getColumn2Rule().get(i).getColumnType()) {
                                case "boolean":
                                    if (fetchResultSet.getObject(i) != null) {
                                        statement.setBoolean(i, getStateByDefinition(
                                                threadSafeStatement.getColumn2Rule().get(i).getRuleDefinition(),
                                                fetchResultSet.getObject(i)));
                                    } else {
                                        statement.setObject(i, null);
                                    }
                                    break;
                                default:
                                    statement.setObject(i, null);
                                    break;
                            }
                        }


                    }
                    rowCount++;
                    statement.addBatch();
                } while (fetchResultSet.next());

                logMessage = new LogMessage(threadSafeStatement.getFromTaskName(), threadSafeStatement.getFromTableName(), rowCount,
                        chunk.getStartRowId(), chunk.getEndRowId(), chunk.getChunkId());

                logger.info(" {} :\t\tFETCH {}\t {}ms", logMessage.fromTableName(), logMessage,
                        (System.currentTimeMillis() - start));

                start = System.currentTimeMillis();

//                logger.info("{}", statement);

                statement.executeBatch();
                connection.commit();

                logger.info(" {} :\t\tINSERT {}\t {}ms", logMessage.fromTableName(), logMessage,
                        (System.currentTimeMillis() - start));

                statement.close();
                DatabaseUtil.closeConnection(connection);
            } else {
                logMessage = new LogMessage(threadSafeStatement.getFromTaskName(), threadSafeStatement.getFromTableName(), 0,
                        chunk.getStartRowId(), chunk.getEndRowId(), chunk.getChunkId());
                logger.info(" {} :\t\tFETCH {}\t", logMessage.fromTableName(), logMessage);
            }
        } catch (SQLException e) {
//            logger.error("\t" + sqlStatement.getFromTableName() + " : " + e.getMessage());
            logger.error("{} \t {}", threadSafeStatement.getFromTableName(), e);
            e.printStackTrace();
            sqlException = e;
        }
        return new RunnerResult(logMessage, sqlException);
    }

    private static boolean getStateByDefinition(String ruleDefinition, Object o) {
        String[] strings = ruleDefinition.split(",");
        return !o.toString().equals(strings[0]);
    }
}
