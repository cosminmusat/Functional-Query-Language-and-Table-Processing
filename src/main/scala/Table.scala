import util.Util.{Line, Row}

trait FilterCond {
  def &&(other: FilterCond): FilterCond = And(this, other)
  def ||(other: FilterCond): FilterCond = Or(this, other)
  // fails if the column name is not present in the row
  def eval(r: Row): Option[Boolean]
}
case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    r.get(colName) match {
      case None => None
      case Some(x) => Some(predicate(x))
    }
  }
}

case class And(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    (f1.eval(r), f2.eval(r)) match {
      case (None, _) => None
      case (_, None) => None
      case (Some(x), Some(y)) => Some(x && y)
    }
  }
}

case class Or(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    (f1.eval(r), f2.eval(r)) match {
      case (None, _) => None
      case (_, None) => None
      case (Some(x), Some(y)) => Some(x || y)
    }
  }
}

trait Query {
  def eval: Option[Table]
}

/*
  Atom query which evaluates to the input table
  Always succeeds
 */
case class Value(t: Table) extends Query {
  override def eval: Option[Table] = Some(t)
}
/*
  Selects certain columns from the result of a target query
  Fails with None if some rows are not present in the resulting table
 */
case class Select(columns: Line, target: Query) extends Query {
  override def eval: Option[Table] = {
    target.eval match {
      case Some(table) => table.select(columns)
    }
  }
}

/*
  Filters rows from the result of the target query
  Success depends only on the success of the target
 */
case class Filter(condition: FilterCond, target: Query) extends Query {
  override def eval: Option[Table] = {
    target.eval match {
      case Some(table) => table.filter(condition)
    }
  }
}

/*
  Creates a new column with default values
  Success depends only on the success of the target
 */
case class NewCol(name: String, defaultVal: String, target: Query) extends Query {
  override def eval: Option[Table] = {
    target.eval match {
      case Some(table) => Some(table.newCol(name, defaultVal))
    }
  }
}

/*
  Combines two tables based on a common key
  Success depends on whether the key exists in both tables or not AND on the success of the target
 */
case class Merge(key: String, t1: Query, t2: Query) extends Query {
  override def eval: Option[Table] = {
    (t1.eval, t2.eval) match {
      case (Some(s), Some(t)) => s.merge(key, t)
    }
  }
}


class Table (columnNames: Line, tabular: List[List[String]]) {
  def getColumnNames : Line = columnNames
  def getTabular : List[List[String]] = tabular

  // 1.1
  override def toString: String = {
    val csvTabular = tabular.foldRight("")((line, acc) =>
      line.foldRight("")((el, acc2) => el ++ (
        if (acc2 == "") acc2 else "," ++ acc2)) ++ (if (acc == "") acc else "\n" ++ acc))
    val csvColNames = columnNames.foldRight("")((col, acc) => col ++ (if (acc == "") acc else "," ++ acc))
    csvColNames ++ "\n" ++ csvTabular
  }

  // 2.1

  def auxSelect(toSelect: Line, columnNames: Line, tab: List[List[String]]): List[List[String]] = {
    columnNames match {
      case Nil => Nil
      case x :: xs =>
        if (searchKey(x, toSelect)) tab.map(_.head) :: auxSelect(toSelect, xs, tab.map(_.tail))
        else auxSelect(toSelect, xs, tab.map(_.tail))
    }
  }

  def select(columns: Line): Option[Table] = {
    val tabularAsLines = auxSelect(columns, columnNames, tabular)

    if (tabularAsLines.isEmpty) return None
    val selTabular = tabularAsLines.transpose

    Some(new Table(columns, selTabular))
  }

  // 2.2
  def filter(cond: FilterCond): Option[Table] = {
    val newTabular = tabular.foldRight(Nil: List[List[String]])((line, acc) =>
      cond.eval(columnNames.zip(line).foldRight(Map.empty[String, String])((el, acc) =>
        acc + (el._1 -> el._2))) match {
        case None => return None
        case Some(b) =>
          if (b) line :: acc
          else acc
      })
    Some(new Table(columnNames, newTabular))
  }

  // 2.3.
  def newCol(name: String, defaultVal: String): Table = {
    val revColNames = columnNames.foldLeft(Nil: Line)((acc, col) => col :: acc)
    val addedCol = name :: revColNames
    val updatedColNames = addedCol.foldLeft(Nil: Line)((acc, col) => col :: acc)
    val revTab = tabular.map(line => line.foldLeft(Nil: Line)((acc, col) => col :: acc))
    val addedInTab = revTab.map(line => defaultVal :: line)
    val updatedTabular = addedInTab.map(line => line.foldLeft(Nil: Line)((acc, col) => col :: acc))
    new Table(updatedColNames, updatedTabular)
  }

  def searchKey(key: String, colNames: Line): Boolean = {
    colNames match {
      case Nil => false
      case x :: xs =>
        if (x.equals(key)) true
        else searchKey(key, xs)
    }
  }

  def getKeyValues(key: String, colNames: Line, tab: List[List[String]]): Line = {
    colNames match {
      case x :: xs =>
        if (x.equals(key)) tab.map(_.head)
        else getKeyValues(key, xs, tab.map(_.tail))
    }
  }

  def buildMap(keyVals: Line, colNames: Line, tab: List[List[String]], map: Map[(String, String), String]): Map[(String, String), String] = {
    (keyVals, tab) match {
      case (Nil, Nil) => map
      case (x :: xs, y :: ys) =>
        val keys = List.fill(colNames.size)(x).zip(colNames)
        val mappings = keys.zip(y)
        val newMap = mappings.foldRight(map)((mapping, acc) => acc + (mapping._1 -> mapping._2))
        buildMap(xs, colNames, ys, newMap)
    }
  }

  def mergeRow(key: String, colNames: Line, value: String, map1: Map[(String, String), String], map2: Map[(String, String), String]): Line = {
    colNames match {
      case Nil => Nil
      case x :: xs =>
        if (x.equals(key)) value :: mergeRow(key, xs, value, map1, map2)
        else {
          val mapKey = (value, x)
          val valueMap1 = map1 get mapKey
          val valueMap2 = map2 get mapKey
          (valueMap1, valueMap2) match {
            case (None, None) => "" :: mergeRow(key, xs, value, map1, map2)
            case (Some(t), None) => t :: mergeRow(key, xs, value, map1, map2)
            case (None, Some(t)) => t :: mergeRow(key, xs, value, map1, map2)
            case (Some(s), Some(t)) =>
              if (s.equals(t)) s :: mergeRow(key, xs, value, map1, map2)
              else s ++ ";" ++ t :: mergeRow(key, xs, value, map1, map2)
          }
        }
    }
  }

  // 2.4.
  def merge(key: String, other: Table): Option[Table] = {
    if (!searchKey(key, other.getColumnNames)
            || !searchKey(key, columnNames)) return None
    val keyVals1 = getKeyValues(key, columnNames, tabular)
    val keyVals2 = getKeyValues(key, other.getColumnNames, other.getTabular)
    val newColNames = other.getColumnNames.filter(name => !searchKey(name, columnNames))
    val mergedCols = columnNames ++ newColNames
    val newKeyVals = keyVals2.filter(keyVal => !searchKey(keyVal, keyVals1))
    val mergedKeyVals = keyVals1 ++ newKeyVals
    val map1 = buildMap(keyVals1, columnNames, tabular, Map.empty[(String, String), String])
    val map2 = buildMap(keyVals2, other.getColumnNames, other.getTabular, Map.empty[(String, String), String])
    val newTabular = mergedKeyVals.foldRight(Nil: List[List[String]])((mergedKey, acc) => mergeRow(key, mergedCols, mergedKey, map1, map2) :: acc)
    Some(new Table(mergedCols, newTabular))
  }
}

object Table {
  // 1.2
  def apply(s: String): Table = {
    val lines = s.split("\n")
    val columns = lines.head
    val tabular = lines.tail
    val arg1 = columns.split(",").foldRight(Nil: List[String])((el, acc) => el :: acc)
    val auxArg = tabular.foldRight(Nil: List[List[String]])((line, acc) => line.split(",")
      .foldRight(Nil: List[String])((el, acc2) => el :: acc2) :: acc)
    val arg2 = auxArg.map(line => if (line.size < arg1.size) line ++ List.fill(arg1.size - line.size)("") else line)
    new Table(arg1, arg2)
  }
}
