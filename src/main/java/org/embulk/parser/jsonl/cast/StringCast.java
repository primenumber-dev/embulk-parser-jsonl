package org.embulk.parser.jsonl.cast;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.embulk.spi.DataException;
import org.embulk.util.timestamp.TimestampFormatter;

public class StringCast {
  // copy from csv plugin
  public static final Set<String> TRUE_STRINGS =
      new HashSet<>(
          Arrays.asList(
              "true", "True", "TRUE", "yes", "Yes", "YES", "t", "T", "y", "Y", "on", "On", "ON",
              "1"));

  public static final Set<String> FALSE_STRINGS =
      new HashSet<>(
          Arrays.asList(
              "false", "False", "FALSE", "no", "No", "NO", "f", "F", "n", "N", "off", "Off", "OFF",
              "0"));

  private StringCast() {}

  private static String buildErrorMessage(String as, String value) {
    return String.format("cannot cast String to %s: \"%s\"", as, value);
  }

  public static boolean asBoolean(String value) throws DataException {
    if (TRUE_STRINGS.contains(value)) {
      return true;
    } else if (FALSE_STRINGS.contains(value)) {
      return false;
    } else {
      throw new DataException(buildErrorMessage("boolean", value));
    }
  }

  public static long asLong(String value) throws DataException {
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException ex) {
      throw new DataException(buildErrorMessage("long", value), ex);
    }
  }

  public static double asDouble(String value) throws DataException {
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException ex) {
      throw new DataException(buildErrorMessage("double", value), ex);
    }
  }

  public static String asString(String value) throws DataException {
    return value;
  }

  public static Instant asInstant(String value, TimestampFormatter formatter) throws DataException {
    try {
      return formatter.parse(value);
    } catch (DateTimeParseException ex) {
      throw new DataException(buildErrorMessage("timestamp", value), ex);
    }
  }
}
