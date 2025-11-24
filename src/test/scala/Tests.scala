class Tests extends munit.FunSuite {
  test("1.1 - 5p") {
    assertEquals(TestTables.table1.toString, TestTables.table1String)
  }

  test("1.2 - 5p") {
    val result = Table(TestTables.table1String)
    assertEquals(result.getColumnNames, TestTables.table1.getColumnNames, "column names don't match")
    assertEquals(result.getTabular, TestTables.table1.getTabular, "table contents don't match")
  }

  test("2.1 single column - 1p") {
    val result = TestTables.table1.select(List("col1"))
    assert(result.nonEmpty, "result is empty")
    assertEquals(result.get.getColumnNames, List("col1"), "column names don't match")
    assertEquals(result.get.getTabular, TestTables.table1.getTabular.map(_.take(1)), "table contents don't match")
  }

  test("2.1 empty result - 1p") {
    val result = TestTables.table1.select(List("non-existent column"))
    assert(result.isEmpty, "result is not empty")
  }

  test("2.1 multiple columns - 3p") {
    val columns = List("Language", "Functional")
    val result = TestTables.table2.select(columns)
    val expectedContent = List(
      List("Haskell", "true"),
      List("Scala", "true"),
      List("COOL", "false"),
      List("Prolog", "false")
    )
    assert(result.nonEmpty, "result is empty")
    assertEquals(result.get.getColumnNames, columns, "column names don't match")
    assertEquals(result.get.getTabular, expectedContent, "table contents don't match")
  }

  test("2.2 Field condition - 4p") {
    val condition = Field("Functional", _ == "true")
    val result = TestTables.table2.filter(condition)

    assert(result.nonEmpty, "result is empty")

    val expectedContent = List(
      List("Haskell", "false", "true"),
      List("Scala", "true", "true"),
    )

    assertEquals(result.get.getColumnNames, TestTables.table2.getColumnNames, "column names don't match")
    assertEquals(result.get.getTabular, expectedContent, "table contents don't match")
  }

  test("2.2 Field non-existent column - 4p") {
    val condition = Field("Logic", _ == "true")
    val result = TestTables.table2.filter(condition)

    assert(result.isEmpty, "result is not empty")
  }

  test("2.2 And - 4p") {
    val condition = And(
      Field("Object-Oriented", _ == "true"),
      Field("Functional", _ == "true")
    )
    val result = TestTables.table2.filter(condition)

    val expectedContent = List(
      List("Scala", "true", "true"),
    )

    assertEquals(result.get.getColumnNames, TestTables.table2.getColumnNames, "column names don't match")
    assertEquals(result.get.getTabular, expectedContent, "table contents don't match")
  }

  test("2.2 Or - 4p") {
    val condition = Or(
      Field("Object-Oriented", _ == "true"),
      Field("Functional", _ == "true")
    )
    val result = TestTables.table2.filter(condition)

    assert(result.nonEmpty, "result is empty")

    val expectedContent = List(
      List("Haskell", "false", "true"),
      List("Scala", "true", "true"),
      List("COOL", "true", "false"),
    )

    assertEquals(result.get.getColumnNames, TestTables.table2.getColumnNames, "column names don't match")
    assertEquals(result.get.getTabular, expectedContent, "table contents don't match")
  }

  test("2.2 Error propagation - 4p") {
    val condition = Or(
      Field("Object-Oriented", _ == "true"),
      Field("Logic", _ == "true")
    )
    val result = TestTables.table2.filter(condition)

    assert(result.isEmpty, "result is not empty")
  }

  test("2.3 - 5p") {
    val result = TestTables.table2.newCol("EsoLang", "false")
    val expectedColumns = List("Language", "Object-Oriented", "Functional", "EsoLang")
    val expectedContent = List(
      List("Haskell", "false", "true", "false"),
      List("Scala", "true", "true", "false"),
      List("COOL", "true", "false", "false"),
      List("Prolog", "false", "false", "false")
    )

    assertEquals(result.getColumnNames, expectedColumns, "column names don't match")
    assertEquals(result.getTabular.sortBy(_.head), expectedContent.sortBy(_.head), "table contents don't match")
  }

  test("2.4 merge languages - 15p") {
    val result = TestTables.table3.merge("Language", TestTables.table4)
    assert(result.nonEmpty, "result is empty")
    println(result.get)
    assertEquals(result.get.getColumnNames, TestTables.table3_4_merged.getColumnNames, "column names don't match")
    assertEquals(result.get.getTabular.sortBy(_.head), TestTables.table3_4_merged.getTabular.sortBy(_.head), "table contents don't match")
  }

  test("2.4 missing key column - 5p") {
    assert(TestTables.table3.merge("Functional", TestTables.table4).isEmpty, "result should be empty, table 4 has no 'Functional' column")
    assert(TestTables.table3.merge("Object-Oriented", TestTables.table4).isEmpty, "result should be empty, table 3 has no 'Object-Oriented' column")
    assert(TestTables.table3.merge("EsoLang", TestTables.table4).isEmpty, "result should be empty, neither table has the 'EsoLang' column")
  }

  test("3 NewCol + Value - 5p") {
    val query = NewCol("New Column", "No", Value(TestTables.tableObjectOriented))
    val result = query.eval
    assertEquals(result.get.getColumnNames, TestTables.test3_newCol_Value.getColumnNames, "column names don't match")
    assertEquals(result.get.getTabular.sortBy(_.head), TestTables.test3_newCol_Value.getTabular.sortBy(_.head), "table contents don't match")
  }

  test("3 Select + Value - 5p") {
    val query = Select(List("Language", "Original purpose"), Value(TestTables.tableFunctional))
    val result = query.eval
    assertEquals(result.get.getColumnNames, TestTables.test3_Select_Value.getColumnNames, "column names don't match")
    assertEquals(result.get.getTabular.sortBy(_.head), TestTables.test3_Select_Value.getTabular.sortBy(_.head), "table contents don't match")
  }

  test("3 Merge + Value - 5p") {
    val query = Merge("Language", Value(TestTables.tableObjectOriented), Value(TestTables.tableFunctional))
    val result = query.eval
    assertEquals(result.get.getColumnNames, TestTables.test3_Merge_Value.getColumnNames, "column names don't match")
    assertEquals(result.get.getTabular.sortBy(_.head), TestTables.test3_Merge_Value.getTabular.sortBy(_.head), "table contents don't match")
  }

  test("3 Filter + Value - 10p") {
    val query = Filter(
      And(Field("Original purpose", x => !x.contains("General")), Field("Language", x => !x.startsWith("C"))),
      Value(TestTables.tableObjectOriented))
    val result = query.eval
    assertEquals(result.get.getColumnNames, TestTables.test3_Filter_Value.getColumnNames, "column names don't match")
    assertEquals(result.get.getTabular.sortBy(_.head), TestTables.test3_Filter_Value.getTabular.sortBy(_.head), "table contents don't match")
  }

  test("3.1 - 5p") {
    val result = TestTables.programmingLanguages1
    assertEquals(result.getColumnNames, TestTables.ref_programmingLanguages1.getColumnNames, "column names don't match")
    assertEquals(result.getTabular.sortBy(_.head), TestTables.ref_programmingLanguages1.getTabular.sortBy(_.head), "table contents don't match")
  }

  test("3.2 - 5p") {
    val result = TestTables.programmingLanguages2
    assertEquals(result.getColumnNames, TestTables.ref_programmingLanguages2.getColumnNames, "column names don't match")
    assertEquals(result.getTabular.sortBy(_.head), TestTables.ref_programmingLanguages2.getTabular.sortBy(_.head), "table contents don't match")
  }

  test("3.3 - 5p") {
    val result = TestTables.programmingLanguages3
    assertEquals(result.getColumnNames, TestTables.ref_programmingLanguages3.getColumnNames, "column names don't match")
    assertEquals(result.getTabular.sortBy(_.head), TestTables.ref_programmingLanguages3.getTabular.sortBy(_.head), "table contents don't match")
  }

}