package net.jgp.books.spark.ch08.lab200_informix_dialect

import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types.{DataType, DataTypes, MetadataBuilder}

/**
 * An Informix dialect for Apache Spark.
 *
 * @author rambabu.posa
 */
class InformixJdbcDialectScala extends JdbcDialect {
  private val serialVersionUID = -672901

  override def canHandle(url: String): Boolean =
    url.startsWith("jdbc:informix-sqli")

  /**
   * Processes specific JDBC types for Catalyst.
   */
  override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder):Option[DataType] =
    if (typeName.toLowerCase.compareTo("serial") == 0)
      Option.apply(DataTypes.IntegerType)
    else if (typeName.toLowerCase.compareTo("calendar") == 0)
      Option.apply(DataTypes.BinaryType)
    else if (typeName.toLowerCase.compareTo("calendarpattern") == 0)
      Option.apply(DataTypes.BinaryType)
    else if (typeName.toLowerCase.compareTo("se_metadata") == 0)
      Option.apply(DataTypes.BinaryType)
    else if (typeName.toLowerCase.compareTo("sysbldsqltext") == 0)
      Option.apply(DataTypes.BinaryType)
    else if (typeName.toLowerCase.startsWith("timeseries"))
      Option.apply(DataTypes.BinaryType)
    else if (typeName.toLowerCase.compareTo("st_point") == 0)
      Option.apply(DataTypes.BinaryType)
    else if (typeName.toLowerCase.compareTo("tspartitiondesc_t") == 0)
      Option.apply(DataTypes.BinaryType)
    else
      Option.empty // An object from the Scala library

}
