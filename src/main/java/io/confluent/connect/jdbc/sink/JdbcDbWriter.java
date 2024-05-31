/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcDbWriter {
  private static final Logger log = LoggerFactory.getLogger(JdbcDbWriter.class);

  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  final CachedConnectionProvider cachedConnectionProvider;
  private String masterQueryToGetBalance;


  JdbcDbWriter(final JdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure) {
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;

    this.cachedConnectionProvider = connectionProvider(
        config.connectionAttempts,
        config.connectionBackoffMs
    );
  }

  protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
    return new CachedConnectionProvider(this.dbDialect, maxConnAttempts, retryBackoff) {
      @Override
      protected void onConnect(final Connection connection) throws SQLException {
        log.info("JdbcDbWriter Connected");
        connection.setAutoCommit(false);
      }
    };
  }

  void write(final Collection<SinkRecord> records)
      throws SQLException, TableAlterOrCreateException {
    log.debug("Initializing vanilla writer");
    final Connection connection = cachedConnectionProvider.getConnection();
    try {
      final Map<TableId, BufferedRecords> bufferByTable = new HashMap<>();
      for (SinkRecord record : records) {
        log.debug("Buffering sink record: {}", record);
        final TableId tableId = destinationTable(record.topic());
        BufferedRecords buffer = bufferByTable.get(tableId);
        if (buffer == null) {
          buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
          bufferByTable.put(tableId, buffer);
        }
        buffer.add(record);
      }
      for (Map.Entry<TableId, BufferedRecords> entry : bufferByTable.entrySet()) {
        TableId tableId = entry.getKey();
        BufferedRecords buffer = entry.getValue();
        log.debug("Flushing records in JDBC Writer for table ID: {}", tableId);
        buffer.flush();
        buffer.close();
      }
      connection.commit();
    } catch (SQLException | TableAlterOrCreateException e) {
      try {
        connection.rollback();
      } catch (SQLException sqle) {
        e.addSuppressed(sqle);
      } finally {
        throw e;
      }
    }
  }

  /**
   * Write records consistently and atomically to a target sink database.
   * @param records sink records coming from Kafka topic
   * @throws SQLException if things go wrong
   * @throws TableAlterOrCreateException if things go wrong in other ways ;)
   */
  void writeConsistently(final Collection<SinkRecord> records)
      throws SQLException, TableAlterOrCreateException {
    final Connection connection = cachedConnectionProvider.getConnection();
    boolean transactionInProgress = false;
    try {
      final Map<TableId, BufferedRecords> bufferedRecords = new HashMap<>();
      for (SinkRecord record : records) {
        log.debug("Buffering sink record: {}", record);
        Struct s = (Struct) record.value();
        boolean isTxnRecord = record.value() != null
            && s.schema().fields().stream().map(Field::name)
                 .collect(Collectors.toSet()).contains("status");
        if (isTxnRecord) {
          if (s.getString("status").equals("BEGIN")) {
            // Do nothing, indicate a connection start.
            log.debug("Received a BEGIN record with transaction id {}, "
                        + "starting to buffer the records", s.getString("id"));
          } else {
            // Commit the connection assuming that we have flushed all the records already.
            log.debug("Received a END record, committing the transaction with id {} with "
                        + "total record size {}", s.getString("id"), s.getInt64("event_count"));
            connection.commit();
            final Struct dataCollections = (Struct) s.getArray("data_collections").get(0);
            if (config.logTableBalance) {
              logTotalBalanceAfterTxnCommit(connection, s.getString("id"));
            }
          }
        } else {
          final TableId tableId = destinationTable(record.topic());
          BufferedRecords buffer = bufferedRecords.get(tableId);
          if (buffer == null) {
            buffer = new BufferedRecords(config, tableId, dbDialect, dbStructure, connection);
            bufferedRecords.put(tableId, buffer);
          }

          buffer.add(removeTableIdentifierField(record));
          buffer.flush();
        }
      }
    } catch (SQLException | TableAlterOrCreateException e) {
      try {
        log.warn("Rolling back transaction because of exception", e);
        connection.rollback();
      } catch (SQLException sqle) {
        e.addSuppressed(sqle);
      } finally {
        throw e;
      }
    }
  }

   void logTotalBalanceAfterTxnCommit(
          Connection connection, String txnId
  ) throws SQLException {
     String warningMessage = "Unable to query sink database for total balance";
     String query = getBalanceQuery();
     try (ResultSet rs = connection.createStatement().executeQuery(query)) {
       if (rs.next()) {
         long balance = rs.getLong(1);
         if (balance != config.expectedTableBalance) {
           log.debug("Total balance did not match. Actual balance: {}, txn_id: {}", balance, txnId);
         } else {
           log.debug("Total balance matched. Actual balance: {}, txn_id: {}", balance, txnId);
         }
       } else {
         log.warn(warningMessage);
       }
     } catch (SQLException e) {
       log.warn(warningMessage, e);
     }
  }

  public String getBalanceQuery() {
    if (config.tablesForBalance.isEmpty()) {
      String errorMessage = "Requested table balance when tables.for.balance was empty, "
                              + "provide a list in the configuration";
      log.error(errorMessage);
      throw new RuntimeException(errorMessage);
    }

    // Form a master query to get balance from all the tables.
    if (masterQueryToGetBalance == null || masterQueryToGetBalance.isEmpty()) {
      String[] tableNames = config.tablesForBalance.split(",");

      List<String> queriesForTables = new ArrayList<>();
      Arrays.stream(tableNames)
        .forEach(tableName -> queriesForTables.add("SELECT balance FROM " + tableName));

      masterQueryToGetBalance = String.join(" UNION ALL ", queriesForTables);
    }

    return "SELECT SUM((SUBSTRING(balance FROM ':(.*?)(:|$)'))::bigint) AS sum "
             + "FROM (" + masterQueryToGetBalance + ") AS subquery;";
  }

  /**
   * Create a sink record after removing the extra field added by a transformer
   * {@code ExtractTopic} while using consistent writes and when all the records are routed to
   * a common topic.
   * @param record the {@link SinkRecord} to remove the field from
   * @return a converted record without the field {@link JdbcSinkConfig#tableIdentifierField}
   */
  SinkRecord removeTableIdentifierField(SinkRecord record) {
    // If consistent writes are not enabled then we do not need to apply transformation or remove
    // any identifier field.
    if (!config.consistentWrites || !config.removeTableIdentifierField) {
      return record;
    }

    Schema updatedKeySchema = makeUpdatedSchema(record.keySchema());
    Schema updatedValueSchema = makeUpdatedSchema(record.valueSchema());

    // We need to update both the structs for key and value.
    return record.newRecord(record.topic(), record.kafkaPartition(),
        updatedKeySchema, makeUpdatedStruct(updatedKeySchema, (Struct) record.key()),
        updatedValueSchema, makeUpdatedStruct(updatedValueSchema, (Struct) record.value()),
        record.timestamp());
  }

  /**
   * Modify the schema for the sink record to not include the field for
   * {@link JdbcSinkConfig#tableIdentifierField}
   * @param schema the schema for which the field needs to be removed
   * @return updated schema with the field removed
   */
  Schema makeUpdatedSchema(Schema schema) {
    if (schema == null) {
      // YB Note: This is to handle tombstone records which will have a null schema.
      return null;
    }

    SchemaBuilder builder = SchemaBuilder.struct();

    for (Field field : schema.fields()) {
      if (!field.name().equals(config.tableIdentifierField)) {
        builder.field(field.name(), field.schema());
      }
    }

    return builder.build();
  }

  /**
   * Modify the struct for the given value to remove the value pertaining to
   * {@link JdbcSinkConfig#tableIdentifierField}
   * @param schema the schema which does not contain the field for
   * {@link JdbcSinkConfig#tableIdentifierField}
   * @param value the value which needs the value of the field to be removed
   * @return a modified struct with the removed value
   */
  Struct makeUpdatedStruct(Schema schema, Struct value) {
    if (schema == null || value == null) {
      // YB Note: This is to handle tombstone records which will have a null schema.
      return null;
    }

    Struct updated = new Struct(schema);

    for (Field field : value.schema().fields()) {
      if (!field.name().equals(config.tableIdentifierField)) {
        updated.put(field, value.get(field));
      }
    }

    return updated;
  }

  void closeQuietly() {
    cachedConnectionProvider.close();
  }

  TableId destinationTable(String topic) {
    final String tableName = config.tableNameFormat.replace("${topic}", topic);
    if (tableName.isEmpty()) {
      throw new ConnectException(String.format(
          "Destination table name for topic '%s' is empty using the format string '%s'",
          topic,
          config.tableNameFormat
      ));
    }
    return dbDialect.parseTableIdentifier(tableName);
  }
}
