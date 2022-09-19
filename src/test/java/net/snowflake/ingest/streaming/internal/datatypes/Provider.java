package net.snowflake.ingest.streaming.internal.datatypes;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Implementations of this class are able to provide parameter of a certain type into JDBC
 * statement. It is used to abstract over JDBC statements working with various types in tests.
 */
interface Provider<T> {
  void provide(PreparedStatement stmt, int parameterIndex, T value) throws SQLException;
}

class LongProvider implements Provider<Long> {
  @Override
  public void provide(PreparedStatement stmt, int parameterIndex, Long value) throws SQLException {
    stmt.setLong(parameterIndex, value);
  }
}

class IntProvider implements Provider<Integer> {
  @Override
  public void provide(PreparedStatement stmt, int parameterIndex, Integer value)
      throws SQLException {
    stmt.setInt(parameterIndex, value);
  }
}

class ByteProvider implements Provider<Byte> {
  @Override
  public void provide(PreparedStatement stmt, int parameterIndex, Byte value) throws SQLException {
    stmt.setByte(parameterIndex, value);
  }
}

class ShortProvider implements Provider<Short> {
  @Override
  public void provide(PreparedStatement stmt, int parameterIndex, Short value) throws SQLException {
    stmt.setShort(parameterIndex, value);
  }
}

class BigDecimalProvider implements Provider<BigDecimal> {
  @Override
  public void provide(PreparedStatement stmt, int parameterIndex, BigDecimal value)
      throws SQLException {
    stmt.setBigDecimal(parameterIndex, value);
  }
}

class StringProvider implements Provider<java.lang.String> {
  @Override
  public void provide(PreparedStatement stmt, int parameterIndex, String value)
      throws SQLException {
    stmt.setString(parameterIndex, value);
  }
}

class FloatProvider implements Provider<Float> {
  @Override
  public void provide(PreparedStatement stmt, int parameterIndex, Float value) throws SQLException {
    stmt.setFloat(parameterIndex, value);
  }
}

class DoubleProvider implements Provider<Double> {
  @Override
  public void provide(PreparedStatement stmt, int parameterIndex, Double value)
      throws SQLException {
    stmt.setDouble(parameterIndex, value);
  }
}

class BooleanProvider implements Provider<Boolean> {
  @Override
  public void provide(PreparedStatement stmt, int parameterIndex, Boolean value)
      throws SQLException {
    stmt.setBoolean(parameterIndex, value);
  }
}

class ByteArrayProvider implements Provider<byte[]> {
  @Override
  public void provide(PreparedStatement stmt, int parameterIndex, byte[] value)
      throws SQLException {
    stmt.setBytes(parameterIndex, value);
  }
}
