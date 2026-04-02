package org.embulk.parser.jsonl;

import java.time.Instant;
import org.embulk.parser.jsonl.cast.BooleanCast;
import org.embulk.parser.jsonl.cast.DoubleCast;
import org.embulk.parser.jsonl.cast.JsonCast;
import org.embulk.parser.jsonl.cast.LongCast;
import org.embulk.parser.jsonl.cast.StringCast;
import org.embulk.spi.DataException;
import org.embulk.util.timestamp.TimestampFormatter;
import org.msgpack.value.Value;

class ColumnCaster {
  ColumnCaster() {}

  public static boolean asBoolean(Value value) throws DataException {
    if (value.isBooleanValue()) {
      return value.asBooleanValue().getBoolean();
    } else if (value.isIntegerValue()) {
      return LongCast.asBoolean(value.asIntegerValue().asLong());
    } else if (value.isFloatValue()) {
      return DoubleCast.asBoolean(value.asFloatValue().toDouble());
    } else if (value.isStringValue()) {
      return StringCast.asBoolean(value.asStringValue().asString());
    } else {
      return JsonCast.asBoolean(value);
    }
  }

  public static long asLong(Value value) throws DataException {
    if (value.isBooleanValue()) {
      return BooleanCast.asLong(value.asBooleanValue().getBoolean());
    } else if (value.isIntegerValue()) {
      return value.asIntegerValue().asLong();
    } else if (value.isFloatValue()) {
      return DoubleCast.asLong(value.asFloatValue().toDouble());
    } else if (value.isStringValue()) {
      return StringCast.asLong(value.asStringValue().asString());
    } else {
      return JsonCast.asLong(value);
    }
  }

  public static double asDouble(Value value) throws DataException {
    if (value.isBooleanValue()) {
      return BooleanCast.asDouble(value.asBooleanValue().getBoolean());
    } else if (value.isIntegerValue()) {
      return LongCast.asDouble(value.asIntegerValue().asLong());
    } else if (value.isFloatValue()) {
      return value.asFloatValue().toDouble();
    } else if (value.isStringValue()) {
      return StringCast.asDouble(value.asStringValue().asString());
    } else {
      return JsonCast.asDouble(value);
    }
  }

  public static String asString(Value value) throws DataException {
    return value.toString();
  }

  public static Instant asInstant(Value value, TimestampFormatter formatter) throws DataException {
    if (value.isBooleanValue()) {
      return BooleanCast.asInstant(value.asBooleanValue().getBoolean());
    } else if (value.isIntegerValue()) {
      return LongCast.asInstant(value.asIntegerValue().asLong());
    } else if (value.isFloatValue()) {
      return DoubleCast.asInstant(value.asFloatValue().toDouble());
    } else if (value.isStringValue()) {
      return StringCast.asInstant(value.asStringValue().asString(), formatter);
    } else {
      return JsonCast.asInstant(value);
    }
  }
}
