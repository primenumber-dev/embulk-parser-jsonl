package org.embulk.parser.jsonl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.spi.Buffer;
import org.embulk.spi.GuessPlugin;
import org.embulk.util.config.ConfigMapperFactory;
import org.embulk.util.guess.SchemaGuess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonlGuessPlugin implements GuessPlugin {
  private static final Logger log = LoggerFactory.getLogger(JsonlGuessPlugin.class);
  private static final ConfigMapperFactory CONFIG_MAPPER_FACTORY =
      ConfigMapperFactory.builder().addDefaultModules().build();

  @Override
  @SuppressWarnings("deprecation")
  public ConfigDiff guess(ConfigSource config, Buffer sample) {
    try {
      // Check if parser type is jsonl
      ConfigSource parserConfig = config.getNested("parser");
      String parserType = parserConfig.get(String.class, "type", null);
      if (!"jsonl".equals(parserType)) {
        return CONFIG_MAPPER_FACTORY.newConfigDiff();
      }

      // Get configuration
      String newlineType = parserConfig.get(String.class, "newline", "CRLF");
      String charset = parserConfig.get(String.class, "charset", "UTF-8");
      int minRowsForGuess = parserConfig.get(Integer.class, "min_rows_for_guess", 4);

      // Convert newline type to character
      String newlineChar = newlineCharacter(newlineType);

      // Parse sample data as texts
      byte[] bytes = sample.array();
      String sampleText = new String(bytes, Charset.forName(charset));

      // Parse JSONL lines
      ObjectMapper objectMapper = new ObjectMapper();
      List<LinkedHashMap<String, Object>> rows = new ArrayList<>();

      for (String line : sampleText.split(newlineChar, -1)) {
        if (line.trim().isEmpty()) {
          continue;
        }

        try {
          JsonNode jsonNode = objectMapper.readTree(line);
          if (!jsonNode.isObject()) {
            log.warn("Skipped non-object JSON value: {}", line);
            continue;
          }

          LinkedHashMap<String, Object> row = new LinkedHashMap<>();
          jsonNode
              .fields()
              .forEachRemaining(
                  entry -> {
                    row.put(entry.getKey(), convertJsonNodeToObject(entry.getValue()));
                  });
          rows.add(row);
        } catch (IOException e) {
          log.warn("Failed to parse line: {}", line, e);
        }
      }

      // Check minimum rows
      if (rows.size() < minRowsForGuess) {
        log.info("Not enough rows for guess: {} < {}", rows.size(), minRowsForGuess);
        return CONFIG_MAPPER_FACTORY.newConfigDiff();
      }

      if (rows.isEmpty()) {
        throw new RuntimeException("SchemaGuess Can't guess schema from no records");
      }

      // Guess schema using SchemaGuess
      SchemaGuess schemaGuess = SchemaGuess.of(CONFIG_MAPPER_FACTORY);
      List<ConfigDiff> guessedColumns = schemaGuess.fromLinkedHashMapRecords(rows);

      // Convert ConfigDiff to Map for easier manipulation
      List<Map<String, Object>> columns = new ArrayList<>();
      for (ConfigDiff columnDiff : guessedColumns) {
        Map<String, Object> columnConfig = new LinkedHashMap<>();
        columnConfig.put("name", columnDiff.get(String.class, "name"));
        columnConfig.put("type", columnDiff.get(String.class, "type"));
        String format = columnDiff.get(String.class, "format", null);
        if (format != null) {
          columnConfig.put("format", format);
        }
        columns.add(columnConfig);
      }

      // Build result
      ConfigDiff result = CONFIG_MAPPER_FACTORY.newConfigDiff();
      result.setNested(
          "parser",
          CONFIG_MAPPER_FACTORY.newConfigDiff().set("type", "jsonl").set("columns", columns));

      return result;
    } catch (Exception e) {
      log.error("Failed to guess schema", e);
      return CONFIG_MAPPER_FACTORY.newConfigDiff();
    }
  }

  private Object convertJsonNodeToObject(JsonNode node) {
    if (node.isNull()) {
      return null;
    } else if (node.isBoolean()) {
      return node.asBoolean();
    } else if (node.isLong()) {
      return node.asLong();
    } else if (node.isDouble() || node.isFloat()) {
      return node.asDouble();
    } else if (node.isTextual()) {
      return node.asText();
    } else if (node.isArray() || node.isObject()) {
      return node.toString();
    } else {
      return node.asText();
    }
  }

  private String newlineCharacter(String newlineType) {
    switch (newlineType) {
      case "CRLF":
        return "\r\n";
      case "LF":
        return "\n";
      case "CR":
        return "\r";
      default:
        return "\r\n";
    }
  }
}
