package util

object Util {
  // a row contains the column name and the value
  type Row = Map[String, String]
  // a line is just a row without column names
  type Line = List[String]
}
