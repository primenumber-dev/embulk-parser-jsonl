package org.embulk.parser.jsonl.cast;

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import org.junit.Test;

public class TestLongCast {
  @Test
  public void asBoolean() {
    assertEquals(true, LongCast.asBoolean(1));
    assertEquals(false, LongCast.asBoolean(0));
  }

  @Test
  public void asLong() {
    assertEquals(1, LongCast.asLong(1));
  }

  @Test
  public void asDouble() {
    assertEquals(1.0, LongCast.asDouble(1), 0.0);
  }

  @Test
  public void asString() {
    assertEquals("1", LongCast.asString(1));
  }

  @Test
  public void asInstant() {
    Instant expected = Instant.ofEpochSecond(1);
    assertEquals(expected, LongCast.asInstant(1));
  }
}
