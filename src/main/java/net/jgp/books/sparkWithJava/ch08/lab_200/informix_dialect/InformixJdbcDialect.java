/**
 * 
 */
package net.jgp.books.sparkWithJava.ch08.lab_200.informix_dialect;

import java.sql.Connection;

import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;

import scala.Option;
import scala.collection.immutable.Map;

/**
 * @author jgp
 *
 */
public class InformixJdbcDialect extends JdbcDialect {
  private static final long serialVersionUID =
      -6236667577063262901L;

  @Override
  public void beforeFetch(
      Connection connection,
      Map<String, String> properties) {
    super.beforeFetch(connection, properties);
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.spark.sql.jdbc.JdbcDialect#canHandle(java.lang.String)
   */
  @Override
  public boolean canHandle(String url) {
    return url.startsWith("jdbc:informix-sqli");
  }

  /**
   * Processes specific JDBC types for Catalyst.
   */
  @Override
  public Option<DataType> getCatalystType(int sqlType,
      String typeName, int size, MetadataBuilder md) {
    if (typeName.toLowerCase().compareTo("serial") == 0) {
      return Option.apply(DataTypes.IntegerType);
    }
    if (typeName.toLowerCase().compareTo("calendar") == 0) {
      return Option.apply(DataTypes.BinaryType);
    }
    if (typeName.toLowerCase().compareTo(
        "calendarpattern") == 0) {
      return Option.apply(DataTypes.BinaryType);
    }
    if (typeName.toLowerCase().compareTo(
        "se_metadata") == 0) {
      return Option.apply(DataTypes.BinaryType);
    }
    if (typeName.toLowerCase().compareTo(
        "sysbldsqltext") == 0) {
      return Option.apply(DataTypes.BinaryType);
    }
    if (typeName.toLowerCase().startsWith("timeseries")) {
      return Option.apply(DataTypes.BinaryType);
    }
    if (typeName.toLowerCase().compareTo("st_point") == 0) {
      return Option.apply(DataTypes.BinaryType);
    }
    if (typeName.toLowerCase().compareTo(
        "tspartitiondesc_t") == 0) {
      return Option.apply(DataTypes.BinaryType);
    }
    return Option.empty();
  }

  @Override
  public Option<JdbcType> getJDBCType(DataType dt) {
    if (DataTypes.StringType.sameType(dt)) {
      return Option.apply(new JdbcType("calendarpattern",
          java.sql.Types.BLOB));
    }
    if (DataTypes.StringType.sameType(dt)) {
      return Option.apply(new JdbcType("2000",
          java.sql.Types.BLOB));
    }
    return Option.empty();
  }

  @Override
  public String quoteIdentifier(String colName) {
    return super.quoteIdentifier(colName);
  }
}
