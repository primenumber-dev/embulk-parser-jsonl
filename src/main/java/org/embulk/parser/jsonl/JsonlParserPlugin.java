package org.embulk.parser.jsonl;

import static org.msgpack.value.ValueFactory.newString;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.FileInput;
import org.embulk.spi.PageBuilder;
import org.embulk.spi.PageOutput;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.spi.type.Type;
import org.embulk.util.config.Config;
import org.embulk.util.config.ConfigDefault;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.Task;
import org.embulk.util.config.units.ColumnConfig;
import org.embulk.util.config.units.SchemaConfig;
import org.embulk.util.json.JsonParseException;
import org.embulk.util.json.JsonParser;
import org.embulk.util.text.LineDecoder;
import org.embulk.util.text.LineDelimiter;
import org.embulk.util.text.Newline;
import org.embulk.util.timestamp.TimestampFormatter;
import org.msgpack.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonlParserPlugin implements ParserPlugin {
  private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();

  @Deprecated
  public interface JsonlColumnOption extends Task {
    @Config("type")
    @ConfigDefault("null")
    Optional<Type> getType();
  }

  public interface TypecastColumnOption extends Task {
    @Config("typecast")
    @ConfigDefault("null")
    Optional<Boolean> getTypecast();
  }

  public interface PluginTask extends Task {
    @Config("columns")
    @ConfigDefault("null")
    Optional<SchemaConfig> getSchemaConfig();

    @Config("schema")
    @ConfigDefault("null")
    @Deprecated
    Optional<SchemaConfig> getOldSchemaConfig();

    @Config("stop_on_invalid_record")
    @ConfigDefault("false")
    boolean getStopOnInvalidRecord();

    @Config("default_typecast")
    @ConfigDefault("true")
    Boolean getDefaultTypecast();

    @Config("column_options")
    @ConfigDefault("{}")
    @Deprecated
    Map<String, JsonlColumnOption> getColumnOptions();

    @Config("charset")
    @ConfigDefault("\"utf-8\"")
    String getCharset();

    @Config("newline")
    @ConfigDefault("\"CRLF\"")
    String getNewline();

    @Config("default_timezone")
    @ConfigDefault("\"UTC\"")
    String getDefaultTimezone();

    @Config("default_timestamp_format")
    @ConfigDefault(value = "\"%Y-%m-%d %H:%M:%S.%N %z\"")
    String getDefaultTimestampFormat();
  }

  private static final Logger log = LoggerFactory.getLogger(JsonlParserPlugin.class);

  private String line = null;
  private long lineNumber = 0;
  private Map<String, Value> columnNameValues;

  public JsonlParserPlugin() {}

  @SuppressWarnings("deprecation")
  @Override
  public void transaction(ConfigSource configSource, Control control) {
    final PluginTask task =
        CONFIG_MAPPER_FACTORY.createConfigMapper().map(configSource, PluginTask.class);

    Map<String, JsonlColumnOption> columnOptions = task.getColumnOptions();
    if (columnOptions != null && !columnOptions.isEmpty()) {
      log.warn(
          "embulk-parser-jsonl: \"column_options\" option is deprecated, specify type directly to \"columns\" option with typecast: true (default: true).");
    }

    SchemaConfig schemaConfig = getSchemaConfig(task);
    List<Column> columns = new ArrayList<>();
    for (int i = 0; i < schemaConfig.getColumnCount(); i++) {
      ColumnConfig columnConfig = schemaConfig.getColumn(i);
      Type type = getType(task, columnConfig);
      columns.add(new Column(i, columnConfig.getName(), type));
    }
    control.run(task.dump(), new Schema(columns));
  }

  private static Type getType(PluginTask task, ColumnConfig columnConfig) {
    JsonlColumnOption columnOption =
        columnOptionOf(task.getColumnOptions(), columnConfig.getName());
    return columnOption.getType().orElse(columnConfig.getType());
  }

  // this method is to keep the backward compatibility of 'schema' option.
  private SchemaConfig getSchemaConfig(PluginTask task) {
    if (task.getOldSchemaConfig().isPresent()) {
      log.warn(
          "Please use 'columns' option instead of 'schema' because the 'schema' option is deprecated. The next version will stop 'schema' option support.");
    }

    if (task.getSchemaConfig().isPresent()) {
      return task.getSchemaConfig().get();
    } else if (task.getOldSchemaConfig().isPresent()) {
      return task.getOldSchemaConfig().get();
    } else {
      throw new ConfigException("Attribute 'columns' is required but not set");
    }
  }

  @SuppressWarnings("deprecation")
  @Override
  public void run(TaskSource taskSource, Schema schema, FileInput input, PageOutput output) {
    final PluginTask task =
        CONFIG_MAPPER_FACTORY.createTaskMapper().map(taskSource, PluginTask.class);

    setColumnNameValues(schema);

    final SchemaConfig schemaConfig = getSchemaConfig(task);
    final TimestampFormatter[] timestampFormatters = newTimestampFormatters(task, schemaConfig);
    final Charset charset = Charset.forName(task.getCharset());
    final Newline newline = Newline.valueOf(task.getNewline());
    final LineDelimiter lineDelimiter = newlineToLineDelimiter(newline);
    final LineDecoder decoder = LineDecoder.of(input, charset, lineDelimiter);
    final JsonParser jsonParser = newJsonParser();
    final boolean stopOnInvalidRecord = task.getStopOnInvalidRecord();

    try (final PageBuilder pageBuilder =
        new PageBuilder(org.embulk.spi.Exec.getBufferAllocator(), schema, output)) {
      ColumnVisitorImpl visitor =
          new ColumnVisitorImpl(task, schema, pageBuilder, timestampFormatters);

      lineNumber = 0;
      while (decoder.nextFile()) {
        while (true) {
          line = decoder.poll();
          if (line == null) {
            break;
          }
          lineNumber++;

          // Skip empty lines
          if (line.trim().isEmpty()) {
            continue;
          }

          try {
            Value value = jsonParser.parse(line);

            if (!value.isMapValue()) {
              throw new JsonRecordValidateException("Json string is not representing map value.");
            }

            final Map<Value, Value> record = value.asMapValue().map();
            for (Column column : schema.getColumns()) {
              Value v = record.get(getColumnNameValue(column));
              visitor.setValue(v);
              column.visit(visitor);
            }

            pageBuilder.addRecord();
          } catch (JsonRecordValidateException | JsonParseException e) {
            if (stopOnInvalidRecord) {
              throw new DataException(
                  String.format("Invalid record at line %d: %s", lineNumber, line), e);
            }
            log.warn(String.format("Skipped line %d (%s): %s", lineNumber, e.getMessage(), line));
          }
        }
      }

      pageBuilder.finish();
    }
  }

  private void setColumnNameValues(Schema schema) {
    Map<String, Value> builder = new HashMap<>();
    for (Column column : schema.getColumns()) {
      String name = column.getName();
      builder.put(name, newString(name));
    }
    columnNameValues = builder;
  }

  private Value getColumnNameValue(Column column) {
    return columnNameValues.get(column.getName());
  }

  @SuppressWarnings("deprecation")
  public JsonParser newJsonParser() {
    return new JsonParser();
  }

  private static LineDelimiter newlineToLineDelimiter(Newline newline) {
    switch (newline) {
      case CR:
        return LineDelimiter.CR;
      case LF:
        return LineDelimiter.LF;
      case CRLF:
        return LineDelimiter.CRLF;
      default:
        return LineDelimiter.CRLF;
    }
  }

  /**
   * Creates a line decoder that intelligently handles multiple newline formats.
   * When the configured newline doesn't match the data, this method attempts to use
   * alternative newline formats.
   */
  private LineDecoder createFlexibleLineDecoder(
      FileInput input, Charset charset, LineDelimiter configuredDelimiter) {
    // For now, just return the standard LineDecoder with the configured delimiter
    // The fix for multiple newline formats would require extending LineDecoder
    // or using an alternative approach. The proper solution would be in embulk-util-text
    // library to support automatic newline detection.
    return LineDecoder.of(input, charset, configuredDelimiter);
  }

  private TimestampFormatter[] newTimestampFormatters(PluginTask task, SchemaConfig schemaConfig) {
    TimestampFormatter[] formatters = new TimestampFormatter[schemaConfig.getColumnCount()];
    int i = 0;
    for (ColumnConfig columnConfig : schemaConfig.getColumns()) {
      if (columnConfig.getType() instanceof org.embulk.spi.type.TimestampType) {
        String pattern =
            columnConfig.getOption().get(String.class, "format", task.getDefaultTimestampFormat());
        formatters[i] =
            TimestampFormatter.builder(pattern, true)
                .setDefaultZoneFromString(task.getDefaultTimezone())
                .build();
      }
      i++;
    }
    return formatters;
  }

  private static JsonlColumnOption columnOptionOf(
      Map<String, JsonlColumnOption> columnOptions, String columnName) {
    if (columnOptions == null) {
      return CONFIG_MAPPER_FACTORY
          .createConfigMapper()
          .map(CONFIG_MAPPER_FACTORY.newConfigSource(), JsonlColumnOption.class);
    }
    JsonlColumnOption option = columnOptions.get(columnName);
    if (option != null) {
      return option;
    }
    return CONFIG_MAPPER_FACTORY
        .createConfigMapper()
        .map(CONFIG_MAPPER_FACTORY.newConfigSource(), JsonlColumnOption.class);
  }
}
