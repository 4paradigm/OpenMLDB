/*
 * Copyright 2020 Confluent Inc.
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

import io.confluent.connect.jdbc.util.StringUtils;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

@FunctionalInterface
public interface RecordValidator {

  RecordValidator NO_OP = (record) -> { };

  void validate(SinkRecord record);

  default RecordValidator and(RecordValidator other) {
    if (other == null || other == NO_OP || other == this) {
      return this;
    }
    if (this == NO_OP) {
      return other;
    }
    RecordValidator thisValidator = this;
    return (record) -> {
      thisValidator.validate(record);
      other.validate(record);
    };
  }

  static RecordValidator create(JdbcSinkConfig config) {
    RecordValidator requiresKey = requiresKey(config);
    RecordValidator requiresValue = requiresValue(config);

    RecordValidator keyValidator = NO_OP;
    RecordValidator valueValidator = NO_OP;
    switch (config.pkMode) {
      case RECORD_KEY:
        keyValidator = keyValidator.and(requiresKey);
        break;
      case RECORD_VALUE:
      case NONE:
        valueValidator = valueValidator.and(requiresValue);
        break;
      case KAFKA:
      default:
        // no primary key is required
        break;
    }

    if (config.deleteEnabled) {
      // When delete is enabled, we need a key
      keyValidator = keyValidator.and(requiresKey);
    } else {
      // When delete is disabled, we need non-tombstone values
      valueValidator = valueValidator.and(requiresValue);
    }

    // Compose the validator that may or may be NO_OP
    return keyValidator.and(valueValidator);
  }

  static RecordValidator requiresValue(JdbcSinkConfig config) {
    return record -> {
      Schema valueSchema = record.valueSchema();
      if (record.value() != null
          && valueSchema != null
          && valueSchema.type() == Schema.Type.STRUCT) {
        return;
      }
      throw new ConnectException(
          String.format(
              "Sink connector '%s' is configured with '%s=%s' and '%s=%s' and therefore requires "
              + "records with a non-null Struct value and non-null Struct schema, "
              + "but found record at (topic='%s',partition=%d,offset=%d,timestamp=%d) "
              + "with a %s value and %s value schema.",
              config.connectorName(),
              JdbcSinkConfig.DELETE_ENABLED,
              config.deleteEnabled,
              JdbcSinkConfig.PK_MODE,
              config.pkMode.toString().toLowerCase(),
              record.topic(),
              record.kafkaPartition(),
              record.kafkaOffset(),
              record.timestamp(),
              StringUtils.valueTypeOrNull(record.value()),
              StringUtils.schemaTypeOrNull(record.valueSchema())
          )
      );
    };
  }

  static RecordValidator requiresKey(JdbcSinkConfig config) {
    return record -> {
      Schema keySchema = record.keySchema();
      if (record.key() != null
          && keySchema != null
          && (keySchema.type() == Schema.Type.STRUCT || keySchema.type().isPrimitive())) {
        return;
      }
      throw new ConnectException(
          String.format(
              "Sink connector '%s' is configured with '%s=%s' and '%s=%s' and therefore requires "
              + "records with a non-null key and non-null Struct or primitive key schema, "
              + "but found record at (topic='%s',partition=%d,offset=%d,timestamp=%d) "
              + "with a %s key and %s key schema.",
              config.connectorName(),
              JdbcSinkConfig.DELETE_ENABLED,
              config.deleteEnabled,
              JdbcSinkConfig.PK_MODE,
              config.pkMode.toString().toLowerCase(),
              record.topic(),
              record.kafkaPartition(),
              record.kafkaOffset(),
              record.timestamp(),
              StringUtils.valueTypeOrNull(record.key()),
              StringUtils.schemaTypeOrNull(record.keySchema())
          )
      );
    };
  }
}
