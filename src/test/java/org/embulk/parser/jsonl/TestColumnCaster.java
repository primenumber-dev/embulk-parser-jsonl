package org.embulk.parser.jsonl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Instant;
import org.embulk.spi.DataException;
import org.embulk.util.timestamp.TimestampFormatter;
import org.junit.Before;
import org.junit.Test;
import org.msgpack.value.MapValue;
import org.msgpack.value.ValueFactory;

public class TestColumnCaster {
  public MapValue mapValue;
  public DataException thrown;
  public TimestampFormatter formatter;

  @Before
  public void createResource() {
    thrown = new DataException("any");
    org.msgpack.value.Value[] kvs = new org.msgpack.value.Value[2];
    kvs[0] = ValueFactory.newString("k");
    kvs[1] = ValueFactory.newString("v");
    mapValue = ValueFactory.newMap(kvs);
    formatter = TimestampFormatter.builder("%Y-%m-%d %H:%M:%S.%N", true).build();
  }

  @Test
  public void asBooleanFromBoolean() {
    assertEquals(true, ColumnCaster.asBoolean(ValueFactory.newBoolean(true)));
  }

  @Test
  public void asBooleanFromInteger() {
    assertEquals(true, ColumnCaster.asBoolean(ValueFactory.newInteger(1)));
    try {
      ColumnCaster.asBoolean(ValueFactory.newInteger(2));
      fail();
    } catch (Throwable t) {
      assertTrue(t instanceof DataException);
    }
  }

  @Test
  public void asBooleanFromFloat() {
    try {
      ColumnCaster.asBoolean(ValueFactory.newFloat(1.1));
      fail();
    } catch (Throwable t) {
      assertTrue(t instanceof DataException);
    }
  }

  @Test
  public void asBooleanFromString() {
    assertEquals(true, ColumnCaster.asBoolean(ValueFactory.newString("true")));
    try {
      ColumnCaster.asBoolean(ValueFactory.newString("foo"));
      fail();
    } catch (Throwable t) {
      assertTrue(t instanceof DataException);
    }
  }

  @Test
  public void asBooleanFromJson() {
    try {
      ColumnCaster.asBoolean(mapValue);
      fail();
    } catch (Throwable t) {
      assertTrue(t instanceof DataException);
    }
  }

  @Test
  public void asLongFromBoolean() {
    assertEquals(1, ColumnCaster.asLong(ValueFactory.newBoolean(true)));
  }

  @Test
  public void asLongFromInteger() {
    assertEquals(1, ColumnCaster.asLong(ValueFactory.newInteger(1)));
  }

  @Test
  public void asLongFromFloat() {
    assertEquals(1, ColumnCaster.asLong(ValueFactory.newFloat(1.5)));
  }

  @Test
  public void asLongFromString() {
    assertEquals(1, ColumnCaster.asLong(ValueFactory.newString("1")));
    try {
      ColumnCaster.asLong(ValueFactory.newString("foo"));
      fail();
    } catch (Throwable t) {
      assertTrue(t instanceof DataException);
    }
  }

  @Test
  public void asLongFromJson() {
    try {
      ColumnCaster.asLong(mapValue);
      fail();
    } catch (Throwable t) {
      assertTrue(t instanceof DataException);
    }
  }

  @Test
  public void asDoubleFromBoolean() {
    assertEquals(1, ColumnCaster.asLong(ValueFactory.newBoolean(true)));
  }

  @Test
  public void asDoubleFromInteger() {
    assertEquals(1, ColumnCaster.asLong(ValueFactory.newInteger(1)));
  }

  @Test
  public void asDoubleFromFloat() {
    assertEquals(1, ColumnCaster.asLong(ValueFactory.newFloat(1.5)));
  }

  @Test
  public void asDoubleFromString() {
    assertEquals(1, ColumnCaster.asLong(ValueFactory.newString("1")));
    try {
      ColumnCaster.asLong(ValueFactory.newString("foo"));
      fail();
    } catch (Throwable t) {
      assertTrue(t instanceof DataException);
    }
  }

  @Test
  public void asDoubleFromJson() {
    try {
      ColumnCaster.asLong(mapValue);
      fail();
    } catch (Throwable t) {
      assertTrue(t instanceof DataException);
    }
  }

  @Test
  public void asStringFromBoolean() {
    assertEquals("true", ColumnCaster.asString(ValueFactory.newBoolean(true)));
  }

  @Test
  public void asStringFromInteger() {
    assertEquals("1", ColumnCaster.asString(ValueFactory.newInteger(1)));
  }

  @Test
  public void asStringFromFloat() {
    assertEquals("1.5", ColumnCaster.asString(ValueFactory.newFloat(1.5)));
  }

  @Test
  public void asStringFromString() {
    assertEquals("1", ColumnCaster.asString(ValueFactory.newString("1")));
  }

  @Test
  public void asStringFromJson() {
    assertEquals("{\"k\":\"v\"}", ColumnCaster.asString(mapValue));
  }

  @Test
  public void asInstantFromBoolean() {
    try {
      ColumnCaster.asInstant(ValueFactory.newBoolean(true), formatter);
      fail();
    } catch (Throwable t) {
      assertTrue(t instanceof DataException);
    }
  }

  @Test
  public void asInstantFromInteger() {
    assertEquals(1, ColumnCaster.asInstant(ValueFactory.newInteger(1), formatter).getEpochSecond());
  }

  @Test
  public void asInstantFromFloat() {
    Instant expected = Instant.ofEpochSecond(1463084053, 500000000);
    assertEquals(expected, ColumnCaster.asInstant(ValueFactory.newFloat(1463084053.5), formatter));
  }

  @Test
  public void asInstantFromString() {
    Instant expected = Instant.ofEpochSecond(1463084053, 500000000);
    assertEquals(
        expected,
        ColumnCaster.asInstant(ValueFactory.newString("2016-05-12 20:14:13.5"), formatter));
  }

  @Test
  public void asInstantFromJson() {
    try {
      ColumnCaster.asInstant(mapValue, formatter);
      fail();
    } catch (Throwable t) {
      assertTrue(t instanceof DataException);
    }
  }
}
