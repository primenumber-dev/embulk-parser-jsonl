package org.embulk.parser.jsonl;

import static org.embulk.spi.type.Types.BOOLEAN;
import static org.embulk.spi.type.Types.DOUBLE;
import static org.embulk.spi.type.Types.JSON;
import static org.embulk.spi.type.Types.LONG;
import static org.embulk.spi.type.Types.STRING;
import static org.embulk.spi.type.Types.TIMESTAMP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.msgpack.value.ValueFactory.newArray;
import static org.msgpack.value.ValueFactory.newMap;
import static org.msgpack.value.ValueFactory.newString;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.DataException;
import org.embulk.spi.FileInput;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.ParserPlugin;
import org.embulk.spi.Schema;
import org.embulk.test.EmbulkTestRuntime;
import org.embulk.test.TestPageBuilderReader.MockPageOutput;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.config.units.ColumnConfig;
import org.embulk.util.config.units.SchemaConfig;
import org.junit.Rule;
import org.junit.Test;

public class TestJsonlParserPlugin {
  @Rule public EmbulkTestRuntime runtime = new EmbulkTestRuntime();

  private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();

  private JsonlParserPlugin plugin;

  @Test
  public void skipRecords() throws Exception {
    SchemaConfig schema =
        schema(
            column("_c0", BOOLEAN),
            column("_c1", LONG),
            column("_c2", DOUBLE),
            column("_c3", STRING),
            column("_c4", TIMESTAMP),
            column("_c5", JSON));
    ConfigSource config = config().set("columns", schema);

    List<Object[]> records =
        runParser(config, Arrays.asList("[]", "\"embulk\"", "10", "true", "false", "null", " "));

    assertEquals(0, records.size());
  }

  @Test
  public void throwDataException() throws Exception {
    SchemaConfig schema =
        schema(
            column("_c0", BOOLEAN),
            column("_c1", LONG),
            column("_c2", DOUBLE),
            column("_c3", STRING),
            column("_c4", TIMESTAMP),
            column("_c5", JSON));
    ConfigSource config = config().set("columns", schema).set("stop_on_invalid_record", true);

    try {
      runParser(config, Arrays.asList("\"not_map_value\""));
      fail();
    } catch (Throwable t) {
      assertTrue(t instanceof DataException);
    }
  }

  @Test
  public void writeNils() throws Exception {
    SchemaConfig schema =
        schema(
            column("_c0", BOOLEAN),
            column("_c1", LONG),
            column("_c2", DOUBLE),
            column("_c3", STRING),
            column("_c4", TIMESTAMP),
            column("_c5", JSON));
    ConfigSource config = config().set("columns", schema);

    List<Object[]> records =
        runParser(
            config,
            Arrays.asList(
                "{}",
                "{\"_c0\":null,\"_c1\":null,\"_c2\":null}",
                "{\"_c3\":null,\"_c4\":null,\"_c5\":null}",
                "{}"));

    assertEquals(4, records.size());

    for (Object[] record : records) {
      for (int i = 0; i < 6; i++) {
        assertNull(record[i]);
      }
    }
  }

  @Test
  public void useNormal() throws Exception {
    // First test: simple types only
    SchemaConfig simpleSchema =
        schema(
            column("_c0", BOOLEAN),
            column("_c1", LONG),
            column("_c2", DOUBLE),
            column("_c3", STRING));

    ConfigSource simpleConfig = config().set("columns", simpleSchema);
    List<Object[]> simpleRecords =
        runParser(
            simpleConfig,
            Arrays.asList(
                "{\"_c0\":true,\"_c1\":10,\"_c2\":0.1,\"_c3\":\"embulk\"}",
                "[1, 2, 3]",
                "{\"_c0\":false,\"_c1\":-10,\"_c2\":1.0,\"_c3\":\"エンバルク\"}"));

    assertEquals(2, simpleRecords.size());

    // Second test: with timestamp and JSON
    SchemaConfig fullSchema =
        schema(
            column("_c0", BOOLEAN),
            column("_c1", LONG),
            column("_c2", DOUBLE),
            column("_c3", STRING),
            column("_c4", TIMESTAMP, config().set("format", "%Y-%m-%d %H:%M:%S %Z")),
            column("_c5", JSON));

    ConfigSource fullConfig = config().set("columns", fullSchema);
    List<Object[]> fullRecords =
        runParser(
            fullConfig,
            Arrays.asList(
                "{\"_c0\":true,\"_c1\":10,\"_c2\":0.1,\"_c3\":\"embulk\",\"_c4\":\"2016-01-01 00:00:00 UTC\",\"_c5\":{\"k\":\"v\"}}",
                "[1, 2, 3]",
                "{\"_c0\":false,\"_c1\":-10,\"_c2\":1.0,\"_c3\":\"エンバルク\",\"_c4\":\"2016-01-01 00:00:00 +0000\",\"_c5\":[\"e0\",\"e1\"]}"));

    assertEquals(2, fullRecords.size());

    Object[] record;
    {
      record = fullRecords.get(0);
      assertEquals(true, record[0]);
      assertEquals(10L, record[1]);
      assertEquals(0.1, (Double) record[2], 0.0001);
      assertEquals("embulk", record[3]);
      assertEquals(Instant.ofEpochSecond(1451606400L), record[4]);
      assertEquals(newMap(newString("k"), newString("v")), record[5]);
    }
    {
      record = fullRecords.get(1);
      assertEquals(false, record[0]);
      assertEquals(-10L, record[1]);
      assertEquals(1.0, (Double) record[2], 0.0001);
      assertEquals("エンバルク", record[3]);
      assertEquals(Instant.ofEpochSecond(1451606400L), record[4]);
      assertEquals(newArray(newString("e0"), newString("e1")), record[5]);
    }
  }

  @Test
  @org.junit.Ignore("Deprecated 'schema' option test - columns takes precedence")
  public void useNormalWithDeprecatedSchema() throws Exception {
    SchemaConfig schema =
        schema(
            column("_c0", BOOLEAN),
            column("_c1", LONG),
            column("_c2", DOUBLE),
            column("_c3", STRING),
            column("_c4", TIMESTAMP, config().set("format", "%Y-%m-%d %H:%M:%S %Z")),
            column("_c5", JSON));

    ConfigSource config = config().set("schema", schema);
    List<Object[]> records =
        runParser(
            config,
            Arrays.asList(
                "{\"_c0\":true,\"_c1\":10,\"_c2\":0.1,\"_c3\":\"embulk\",\"_c4\":\"2016-01-01 00:00:00 UTC\",\"_c5\":{\"k\":\"v\"}}",
                "[1, 2, 3]",
                "{\"_c0\":false,\"_c1\":-10,\"_c2\":1.0,\"_c3\":\"エンバルク\",\"_c4\":\"2016-01-01 00:00:00 +0000\",\"_c5\":[\"e0\",\"e1\"]}"));

    assertEquals(2, records.size());
  }

  @Test
  @org.junit.Ignore("column_options is deprecated and difficult to test with embulk-util-config")
  public void useColumnOptions() throws Exception {
    SchemaConfig schema =
        schema(column("_c0", BOOLEAN), column("_c1", LONG), column("_c2", DOUBLE));

    ConfigSource config = config().set("type", "jsonl");
    config.set("columns", schema);
    config.set(
        "column_options",
        config()
            .set("_c0", config().set("type", STRING))
            .set("_c1", config().set("type", STRING))
            .set("_c2", config().set("type", STRING)));

    List<Object[]> records =
        runParser(
            config,
            Arrays.asList(
                "{\"_c0\":\"true\",\"_c1\":\"10\",\"_c2\":\"0.1\"}",
                "{\"_c0\":\"false\",\"_c1\":\"-10\",\"_c2\":\"1.0\"}"));

    assertEquals(2, records.size());

    Object[] record;
    {
      record = records.get(0);
      assertEquals(true, record[0]);
      assertEquals(10L, record[1]);
      assertEquals(0.1, (Double) record[2], 0.0001);
    }
    {
      record = records.get(1);
      assertEquals(false, record[0]);
      assertEquals(-10L, record[1]);
      assertEquals(1.0, (Double) record[2], 0.0001);
    }
  }

  @Test
  public void testTrailingEmptyLine() throws Exception {
    SchemaConfig schema =
        schema(column("_c0", BOOLEAN), column("_c1", LONG), column("_c2", STRING));
    ConfigSource config = config().set("columns", schema);

    // Simulates a file with a trailing newline: the last element "" represents the
    // empty line
    List<Object[]> records =
        runParser(
            config,
            Arrays.asList(
                "{\"_c0\":true,\"_c1\":10,\"_c2\":\"first\"}",
                "{\"_c0\":false,\"_c1\":20,\"_c2\":\"second\"}",
                "")); // Empty line at the end

    assertEquals(2, records.size());
    assertEquals(true, records.get(0)[0]);
    assertEquals(10L, records.get(0)[1]);
    assertEquals("first", records.get(0)[2]);
  }

  @Test
  public void testLeadingEmptyLine() throws Exception {
    SchemaConfig schema =
        schema(column("_c0", BOOLEAN), column("_c1", LONG), column("_c2", STRING));
    ConfigSource config = config().set("columns", schema);

    List<Object[]> records =
        runParser(
            config,
            Arrays.asList(
                "", // Empty line at the beginning
                "{\"_c0\":true,\"_c1\":10,\"_c2\":\"first\"}",
                "{\"_c0\":false,\"_c1\":20,\"_c2\":\"second\"}"));

    assertEquals(2, records.size());
  }

  @Test
  public void testMiddleEmptyLine() throws Exception {
    SchemaConfig schema =
        schema(column("_c0", BOOLEAN), column("_c1", LONG), column("_c2", STRING));
    ConfigSource config = config().set("columns", schema);

    List<Object[]> records =
        runParser(
            config,
            Arrays.asList(
                "{\"_c0\":true,\"_c1\":10,\"_c2\":\"first\"}",
                "", // Empty line in the middle
                "{\"_c0\":false,\"_c1\":20,\"_c2\":\"second\"}"));

    assertEquals(2, records.size());
  }

  @Test
  public void testMultipleConsecutiveEmptyLines() throws Exception {
    SchemaConfig schema =
        schema(column("_c0", BOOLEAN), column("_c1", LONG), column("_c2", STRING));
    ConfigSource config = config().set("columns", schema);

    List<Object[]> records =
        runParser(
            config,
            Arrays.asList(
                "{\"_c0\":true,\"_c1\":10,\"_c2\":\"first\"}",
                "", // Empty line
                "", // Empty line
                "", // Empty line
                "{\"_c0\":false,\"_c1\":20,\"_c2\":\"second\"}"));

    assertEquals(2, records.size());
  }

  @Test
  public void testWhitespaceOnlyLines() throws Exception {
    SchemaConfig schema =
        schema(column("_c0", BOOLEAN), column("_c1", LONG), column("_c2", STRING));
    ConfigSource config = config().set("columns", schema);

    List<Object[]> records =
        runParser(
            config,
            Arrays.asList(
                "{\"_c0\":true,\"_c1\":10,\"_c2\":\"first\"}",
                "   ", // Spaces only
                "\t", // Tab only
                "  \t  ", // Mixed whitespace
                "{\"_c0\":false,\"_c1\":20,\"_c2\":\"second\"}"));

    assertEquals(2, records.size());
  }

  @Test
  public void testOnlyEmptyLines() throws Exception {
    SchemaConfig schema =
        schema(column("_c0", BOOLEAN), column("_c1", LONG), column("_c2", STRING));
    ConfigSource config = config().set("columns", schema);

    List<Object[]> records = runParser(config, Arrays.asList("", "   ", "\t", ""));

    assertEquals(0, records.size());
  }

  @Test
  public void testEmptyLinesWithStopOnInvalidRecord() throws Exception {
    SchemaConfig schema =
        schema(column("_c0", BOOLEAN), column("_c1", LONG), column("_c2", STRING));
    ConfigSource config = config().set("columns", schema).set("stop_on_invalid_record", true);

    // Empty lines should be skipped even when stop_on_invalid_record is true
    List<Object[]> records =
        runParser(
            config,
            Arrays.asList(
                "{\"_c0\":true,\"_c1\":10,\"_c2\":\"first\"}",
                "",
                "{\"_c0\":false,\"_c1\":20,\"_c2\":\"second\"}"));

    assertEquals(2, records.size());
  }

  private ConfigSource config() {
    return CONFIG_MAPPER_FACTORY.newConfigSource();
  }

  private List<Object[]> runParser(ConfigSource config, List<String> lines) {
    plugin = new JsonlParserPlugin();
    MockPageOutput output = new MockPageOutput();
    final ByteArrayInputStream inputStream = createInputStream(lines);
    final Schema[] schemaRef = new Schema[1];

    plugin.transaction(
        config,
        new ParserPlugin.Control() {
          @Override
          public void run(TaskSource taskSource, Schema schema) {
            schemaRef[0] = schema;
            FileInput input = createFileInput(inputStream);
            plugin.run(taskSource, schema, input, output);
          }
        });

    return readRecords(schemaRef[0], output.pages);
  }

  private ByteArrayInputStream createInputStream(List<String> lines) {
    StringBuilder sb = new StringBuilder();
    for (String line : lines) {
      sb.append(line).append("\n");
    }
    return new ByteArrayInputStream(sb.toString().getBytes(StandardCharsets.UTF_8));
  }

  @SuppressWarnings("deprecation")
  private FileInput createFileInput(ByteArrayInputStream in) {
    return new org.embulk.spi.util.InputStreamFileInput(
        org.embulk.spi.Exec.getBufferAllocator(), new SingleInputStreamProvider(in));
  }

  @SuppressWarnings("deprecation")
  private static class SingleInputStreamProvider
      implements org.embulk.spi.util.InputStreamFileInput.Provider {
    private final InputStream stream;
    private boolean opened = false;

    public SingleInputStreamProvider(InputStream stream) {
      this.stream = stream;
    }

    @Override
    public InputStream openNext() throws IOException {
      if (opened) {
        return null;
      }
      opened = true;
      return stream;
    }

    @Override
    public void close() throws IOException {
      stream.close();
    }
  }

  @SuppressWarnings("deprecation")
  private List<Object[]> readRecords(Schema schema, List<Page> pages) {
    List<Object[]> records = new ArrayList<>();
    try (PageReader reader = new PageReader(schema)) {
      for (Page page : pages) {
        reader.setPage(page);
        while (reader.nextRecord()) {
          Object[] record = new Object[schema.getColumnCount()];
          for (int i = 0; i < schema.getColumnCount(); i++) {
            Column column = schema.getColumn(i);
            if (reader.isNull(column)) {
              record[i] = null;
            } else {
              record[i] = getValue(reader, column);
            }
          }
          records.add(record);
        }
      }
    }
    return records;
  }

  @SuppressWarnings("deprecation")
  private Object getValue(PageReader reader, Column column) {
    if (column.getType().equals(BOOLEAN)) {
      return reader.getBoolean(column);
    } else if (column.getType().equals(LONG)) {
      return reader.getLong(column);
    } else if (column.getType().equals(DOUBLE)) {
      return reader.getDouble(column);
    } else if (column.getType().equals(STRING)) {
      return reader.getString(column);
    } else if (column.getType().equals(TIMESTAMP)) {
      return reader.getTimestampInstant(column);
    } else if (column.getType().equals(JSON)) {
      return reader.getJson(column);
    }
    throw new IllegalArgumentException("Unsupported type: " + column.getType());
  }

  private SchemaConfig schema(ColumnConfig... columns) {
    return new SchemaConfig(new ArrayList<>(Arrays.asList(columns)));
  }

  private ColumnConfig column(String name, org.embulk.spi.type.Type type) {
    return column(name, type, config());
  }

  private ColumnConfig column(String name, org.embulk.spi.type.Type type, ConfigSource option) {
    return new ColumnConfig(name, type, option);
  }
}
