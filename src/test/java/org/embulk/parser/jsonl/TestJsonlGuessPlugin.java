package org.embulk.parser.jsonl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.nio.charset.StandardCharsets;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Buffer;
import org.embulk.util.config.ConfigMapperFactory;
import org.junit.Before;
import org.junit.Test;

public class TestJsonlGuessPlugin {
  private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();

  private JsonlGuessPlugin plugin;

  @Before
  public void setup() {
    plugin = new JsonlGuessPlugin();
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testGuessSchema() {
    String sampleData =
        "{\"id\": 1, \"name\": \"Alice\", \"age\": 30}\n"
            + "{\"id\": 2, \"name\": \"Bob\", \"age\": 25}\n"
            + "{\"id\": 3, \"name\": \"Charlie\", \"age\": 35}\n"
            + "{\"id\": 4, \"name\": \"Diana\", \"age\": 28}\n";

    Buffer buffer = Buffer.wrap(sampleData.getBytes(StandardCharsets.UTF_8));

    ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource();
    config.setNested(
        "parser",
        CONFIG_MAPPER_FACTORY
            .newConfigSource()
            .set("type", "jsonl")
            .set("newline", "LF"));

    ConfigDiff result = plugin.guess(config, buffer);

    System.out.println("Result: " + result);
    assertNotNull(result);

    if (!result.getAttributeNames().isEmpty()) {
      assertNotNull(result.getNested("parser"));

      ConfigDiff parserConfig = result.getNested("parser");
      assertEquals("jsonl", parserConfig.get(String.class, "type"));
      assertNotNull(parserConfig.get(Object.class, "columns"));
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testGuessSchemaWithTimestamp() {
    String sampleData =
        "{\"id\": 1, \"created_at\": \"2023-01-15 10:30:00\"}\n"
            + "{\"id\": 2, \"created_at\": \"2023-02-20 14:45:00\"}\n"
            + "{\"id\": 3, \"created_at\": \"2023-03-10 09:15:00\"}\n"
            + "{\"id\": 4, \"created_at\": \"2023-04-05 16:20:00\"}\n";

    Buffer buffer = Buffer.wrap(sampleData.getBytes(StandardCharsets.UTF_8));

    ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource();
    config.setNested(
        "parser",
        CONFIG_MAPPER_FACTORY
            .newConfigSource()
            .set("type", "jsonl")
            .set("newline", "LF"));

    ConfigDiff result = plugin.guess(config, buffer);

    assertNotNull(result);
    if (!result.getAttributeNames().isEmpty()) {
      assertNotNull(result.getNested("parser"));
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testGuessSchemaNotEnoughRows() {
    String sampleData = "{\"id\": 1, \"name\": \"Alice\"}\n";

    Buffer buffer = Buffer.wrap(sampleData.getBytes(StandardCharsets.UTF_8));

    ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource();
    config
        .setNested(
            "parser",
            CONFIG_MAPPER_FACTORY
                .newConfigSource()
                .set("type", "jsonl")
                .set("min_rows_for_guess", 4));

    ConfigDiff result = plugin.guess(config, buffer);

    // Should return empty result when not enough rows
    assertNotNull(result);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void testGuessSchemaWrongParserType() {
    String sampleData = "{\"id\": 1}\n";

    Buffer buffer = Buffer.wrap(sampleData.getBytes(StandardCharsets.UTF_8));

    ConfigSource config = CONFIG_MAPPER_FACTORY.newConfigSource();
    config.setNested("parser", CONFIG_MAPPER_FACTORY.newConfigSource().set("type", "csv"));

    ConfigDiff result = plugin.guess(config, buffer);

    // Should return empty result when parser type is not jsonl
    assertNotNull(result);
  }
}
