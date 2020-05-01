/*
Copyright (c) 2007 Health Market Science, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.healthmarketscience.jackcess;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import static com.healthmarketscience.jackcess.Database.*;
import com.healthmarketscience.jackcess.impl.ColumnImpl;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import com.healthmarketscience.jackcess.impl.RowIdImpl;
import com.healthmarketscience.jackcess.impl.RowImpl;
import com.healthmarketscience.jackcess.impl.TableImpl;
import com.healthmarketscience.jackcess.util.LinkResolver;
import com.healthmarketscience.jackcess.util.RowFilterTest;
import static com.healthmarketscience.jackcess.TestUtil.*;


/**
 * @author Tim McCune
 */
@SuppressWarnings("deprecation")
public class DatabaseTest
{
  @Test
  public void testInvalidTableDefs() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      try {
        new TableBuilder("test").toTable(db);
        Assert.fail("created table with no columns?");
      } catch(IllegalArgumentException e) {
        // success
      }

      try {
        new TableBuilder("test")
          .addColumn(new ColumnBuilder("A", DataType.TEXT))
          .addColumn(new ColumnBuilder("a", DataType.MEMO))
          .toTable(db);
        Assert.fail("created table with duplicate column names?");
      } catch(IllegalArgumentException e) {
        // success
      }

      try {
        new TableBuilder("test")
          .addColumn(new ColumnBuilder("A", DataType.TEXT)
                     .setLengthInUnits(352))
          .toTable(db);
        Assert.fail("created table with invalid column length?");
      } catch(IllegalArgumentException e) {
        // success
      }

      try {
        new TableBuilder("test")
          .addColumn(new ColumnBuilder("A_" + createString(70), DataType.TEXT))
          .toTable(db);
        Assert.fail("created table with too long column name?");
      } catch(IllegalArgumentException e) {
        // success
      }

      new TableBuilder("test")
        .addColumn(new ColumnBuilder("A", DataType.TEXT))
        .toTable(db);


      try {
        new TableBuilder("Test")
          .addColumn(new ColumnBuilder("A", DataType.TEXT))
          .toTable(db);
        Assert.fail("create duplicate tables?");
      } catch(IllegalArgumentException e) {
        // success
      }

      db.close();
    }
  }

  @Test
  public void testReadDeletedRows() throws Exception {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.DEL, true)) {
      Table table = open(testDB).getTable("Table");
      int rows = 0;
      while (table.getNextRow() != null) {
        rows++;
      }
      Assert.assertEquals(2, rows);
      table.getDatabase().close();
    }
  }

  @Test
  public void testGetColumns() throws Exception {
    for (final TestDB testDB : SUPPORTED_DBS_TEST_FOR_READ) {

      List<? extends Column> columns = open(testDB).getTable("Table1").getColumns();
      Assert.assertEquals(9, columns.size());
      checkColumn(columns, 0, "A", DataType.TEXT);
      checkColumn(columns, 1, "B", DataType.TEXT);
      checkColumn(columns, 2, "C", DataType.BYTE);
      checkColumn(columns, 3, "D", DataType.INT);
      checkColumn(columns, 4, "E", DataType.LONG);
      checkColumn(columns, 5, "F", DataType.DOUBLE);
      checkColumn(columns, 6, "G", DataType.SHORT_DATE_TIME);
      checkColumn(columns, 7, "H", DataType.MONEY);
      checkColumn(columns, 8, "I", DataType.BOOLEAN);
    }
  }

  private static void checkColumn(
      List<? extends Column> columns, int columnNumber, String name,
      DataType dataType)
    throws Exception
  {
    Column column = columns.get(columnNumber);
    Assert.assertEquals(name, column.getName());
    Assert.assertEquals(dataType, column.getType());
  }

  @Test
  public void testGetNextRow() throws Exception {
    for (final TestDB testDB : SUPPORTED_DBS_TEST_FOR_READ) {
      final Database db = open(testDB);
      Assert.assertEquals(4, db.getTableNames().size());
      final Table table = db.getTable("Table1");

      Row row1 = table.getNextRow();
      Row row2 = table.getNextRow();

      if(!"abcdefg".equals(row1.get("A"))) {
        Row tmpRow = row1;
        row1 = row2;
        row2 = tmpRow;
      }

      checkTestDBTable1RowABCDEFG(testDB, table, row1);
      checkTestDBTable1RowA(testDB, table, row2);

      db.close();
    }
  }

  @Test
  public void testCreate() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);
      Assert.assertEquals(0, db.getTableNames().size());
      db.close();
    }
  }

  @Test
  public void testDeleteCurrentRow() throws Exception {

    // make sure correct row is deleted
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = createMem(fileFormat);
      createTestTable(db);
      Map<String,Object> row1 = createTestRowMap("Tim1");
      Map<String,Object> row2 = createTestRowMap("Tim2");
      Map<String,Object> row3 = createTestRowMap("Tim3");
      Table table = db.getTable("Test");
      @SuppressWarnings("unchecked")
      List<Map<String,Object>> rows = Arrays.asList(row1, row2, row3);
      table.addRowsFromMaps(rows);
      assertRowCount(3, table);

      table.reset();
      table.getNextRow();
      table.getNextRow();
      table.getDefaultCursor().deleteCurrentRow();

      table.reset();

      Map<String, Object> outRow = table.getNextRow();
      Assert.assertEquals("Tim1", outRow.get("A"));
      outRow = table.getNextRow();
      Assert.assertEquals("Tim3", outRow.get("A"));
      assertRowCount(2, table);

      db.close();

      // test multi row delete/add
      db = createMem(fileFormat);
      createTestTable(db);
      Object[] row = createTestRow();
      table = db.getTable("Test");
      for (int i = 0; i < 10; i++) {
        row[3] = i;
        table.addRow(row);
      }
      row[3] = 1974;
      assertRowCount(10, table);
      table.reset();
      table.getNextRow();
      table.getDefaultCursor().deleteCurrentRow();
      assertRowCount(9, table);
      table.reset();
      table.getNextRow();
      table.getDefaultCursor().deleteCurrentRow();
      assertRowCount(8, table);
      table.reset();
      for (int i = 0; i < 8; i++) {
        table.getNextRow();
      }
      table.getDefaultCursor().deleteCurrentRow();
      assertRowCount(7, table);
      table.addRow(row);
      assertRowCount(8, table);
      table.reset();
      for (int i = 0; i < 3; i++) {
        table.getNextRow();
      }
      table.getDefaultCursor().deleteCurrentRow();
      assertRowCount(7, table);
      table.reset();
      Assert.assertEquals(2, table.getNextRow().get("D"));

      db.close();
    }
  }

  @Test
  public void testDeleteRow() throws Exception {

    // make sure correct row is deleted
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = createMem(fileFormat);
      createTestTable(db);
      Table table = db.getTable("Test");
      for(int i = 0; i < 10; ++i) {
        table.addRowFromMap(createTestRowMap("Tim" + i));
      }
      assertRowCount(10, table);

      table.reset();

      List<Row> rows = RowFilterTest.toList(table);

      Row r1 = rows.remove(7);
      Row r2 = rows.remove(3);
      Assert.assertEquals(8, rows.size());

      Assert.assertSame(r2, table.deleteRow(r2));
      Assert.assertSame(r1, table.deleteRow(r1));

      assertTable(rows, table);

      table.deleteRow(r2);
      table.deleteRow(r1);

      assertTable(rows, table);
    }
  }

  @Test
  public void testMissingFile() throws Exception {
    File bogusFile = new File("fooby-dooby.mdb");
    Assert.assertTrue(!bogusFile.exists());
    try {
      new DatabaseBuilder(bogusFile).setReadOnly(true).
        setAutoSync(getTestAutoSync()).open();
      Assert.fail("FileNotFoundException should have been thrown");
    } catch(FileNotFoundException e) {
    }
    Assert.assertTrue(!bogusFile.exists());
  }

  @Test
  public void testReadWithDeletedCols() throws Exception {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.DEL_COL, true)) {
      Table table = open(testDB).getTable("Table1");

      Map<String, Object> expectedRow0 = new LinkedHashMap<String, Object>();
      expectedRow0.put("id", 0);
      expectedRow0.put("id2", 2);
      expectedRow0.put("data", "foo");
      expectedRow0.put("data2", "foo2");

      Map<String, Object> expectedRow1 = new LinkedHashMap<String, Object>();
      expectedRow1.put("id", 3);
      expectedRow1.put("id2", 5);
      expectedRow1.put("data", "bar");
      expectedRow1.put("data2", "bar2");

      int rowNum = 0;
      Map<String, Object> row = null;
      while ((row = table.getNextRow()) != null) {
        if(rowNum == 0) {
          Assert.assertEquals(expectedRow0, row);
        } else if(rowNum == 1) {
          Assert.assertEquals(expectedRow1, row);
        } else if(rowNum >= 2) {
          Assert.fail("should only have 2 rows");
        }
        rowNum++;
      }

      table.getDatabase().close();
    }
  }

  @Test
  public void testCurrency() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      Table table = new TableBuilder("test")
        .addColumn(new ColumnBuilder("A", DataType.MONEY))
        .toTable(db);

      table.addRow(new BigDecimal("-2341234.03450"));
      table.addRow(37L);
      table.addRow("10000.45");

      table.reset();

      List<Object> foundValues = new ArrayList<Object>();
      Map<String, Object> row = null;
      while((row = table.getNextRow()) != null) {
        foundValues.add(row.get("A"));
      }

      Assert.assertEquals(Arrays.asList(
                       new BigDecimal("-2341234.0345"),
                       new BigDecimal("37.0000"),
                       new BigDecimal("10000.4500")),
                   foundValues);

      try {
        table.addRow(new BigDecimal("342523234145343543.3453"));
        Assert.fail("IOException should have been thrown");
      } catch(IOException e) {
        // ignored
      }

      db.close();
    }
  }

  @Test
  public void testGUID() throws Exception
  {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      Table table = new TableBuilder("test")
        .addColumn(new ColumnBuilder("A", DataType.GUID))
        .toTable(db);

      table.addRow("{32A59F01-AA34-3E29-453F-4523453CD2E6}");
      table.addRow("{32a59f01-aa34-3e29-453f-4523453cd2e6}");
      table.addRow("{11111111-1111-1111-1111-111111111111}");
      table.addRow("   {FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF}   ");
      table.addRow(UUID.fromString("32a59f01-1234-3e29-4aaf-4523453cd2e6"));

      table.reset();

      List<Object> foundValues = new ArrayList<Object>();
      Map<String, Object> row = null;
      while((row = table.getNextRow()) != null) {
        foundValues.add(row.get("A"));
      }

      Assert.assertEquals(Arrays.asList(
                       "{32A59F01-AA34-3E29-453F-4523453CD2E6}",
                       "{32A59F01-AA34-3E29-453F-4523453CD2E6}",
                       "{11111111-1111-1111-1111-111111111111}",
                       "{FFFFFFFF-FFFF-FFFF-FFFF-FFFFFFFFFFFF}",
                       "{32A59F01-1234-3E29-4AAF-4523453CD2E6}"),
                   foundValues);

      try {
        table.addRow("3245234");
        Assert.fail("IOException should have been thrown");
      } catch(IOException e) {
        // ignored
      }

      db.close();
    }
  }

  @Test
  public void testNumeric() throws Exception
  {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      ColumnBuilder col = new ColumnBuilder("A", DataType.NUMERIC)
        .setScale(4).setPrecision(8).toColumn();
      Assert.assertTrue(col.getType().isVariableLength());

      Table table = new TableBuilder("test")
        .addColumn(col)
        .addColumn(new ColumnBuilder("B", DataType.NUMERIC)
                   .setScale(8).setPrecision(28))
        .toTable(db);

      table.addRow(new BigDecimal("-1234.03450"),
                   new BigDecimal("23923434453436.36234219"));
      table.addRow(37L, 37L);
      table.addRow("1000.45", "-3452345321000");

      table.reset();

      List<Object> foundSmallValues = new ArrayList<Object>();
      List<Object> foundBigValues = new ArrayList<Object>();
      Map<String, Object> row = null;
      while((row = table.getNextRow()) != null) {
        foundSmallValues.add(row.get("A"));
        foundBigValues.add(row.get("B"));
      }

      Assert.assertEquals(Arrays.asList(
                       new BigDecimal("-1234.0345"),
                       new BigDecimal("37.0000"),
                       new BigDecimal("1000.4500")),
                   foundSmallValues);
      Assert.assertEquals(Arrays.asList(
                       new BigDecimal("23923434453436.36234219"),
                       new BigDecimal("37.00000000"),
                       new BigDecimal("-3452345321000.00000000")),
                   foundBigValues);

      try {
        table.addRow(new BigDecimal("3245234.234"),
                     new BigDecimal("3245234.234"));
        Assert.fail("IOException should have been thrown");
      } catch(IOException e) {
        // ignored
      }

      db.close();
    }
  }

  @Test
  public void testFixedNumeric() throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.FIXED_NUMERIC)) {
      Database db = openCopy(testDB);
      Table t = db.getTable("test");

      boolean first = true;
      for(Column col : t.getColumns()) {
        if(first) {
          Assert.assertTrue(col.isVariableLength());
          Assert.assertEquals(DataType.MEMO, col.getType());
          first = false;
        } else {
          Assert.assertFalse(col.isVariableLength());
          Assert.assertEquals(DataType.NUMERIC, col.getType());
        }
      }

      Map<String, Object> row = t.getNextRow();
      Assert.assertEquals("some data", row.get("col1"));
      Assert.assertEquals(new BigDecimal("1"), row.get("col2"));
      Assert.assertEquals(new BigDecimal("0"), row.get("col3"));
      Assert.assertEquals(new BigDecimal("0"), row.get("col4"));
      Assert.assertEquals(new BigDecimal("4"), row.get("col5"));
      Assert.assertEquals(new BigDecimal("-1"), row.get("col6"));
      Assert.assertEquals(new BigDecimal("1"), row.get("col7"));

      Object[] tmpRow = new Object[]{
        "foo", new BigDecimal("1"), new BigDecimal(3), new BigDecimal("13"),
        new BigDecimal("-17"), new BigDecimal("0"), new BigDecimal("8734")};
      t.addRow(tmpRow);
      t.reset();

      t.getNextRow();
      row = t.getNextRow();
      Assert.assertEquals(tmpRow[0], row.get("col1"));
      Assert.assertEquals(tmpRow[1], row.get("col2"));
      Assert.assertEquals(tmpRow[2], row.get("col3"));
      Assert.assertEquals(tmpRow[3], row.get("col4"));
      Assert.assertEquals(tmpRow[4], row.get("col5"));
      Assert.assertEquals(tmpRow[5], row.get("col6"));
      Assert.assertEquals(tmpRow[6], row.get("col7"));

      db.close();
    }
  }

  @Test
  public void testMultiPageTableDef() throws Exception
  {
    for (final TestDB testDB : SUPPORTED_DBS_TEST_FOR_READ) {
      List<? extends Column> columns = open(testDB).getTable("Table2").getColumns();
      Assert.assertEquals(89, columns.size());
    }
  }

  @Test
  public void testOverflow() throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.OVERFLOW, true)) {
      Database mdb = open(testDB);
      Table table = mdb.getTable("Table1");

      // 7 rows, 3 and 5 are overflow
      table.getNextRow();
      table.getNextRow();

      Map<String, Object> row = table.getNextRow();
      Assert.assertEquals(Arrays.<Object>asList(
                       null, "row3col3", null, null, null, null, null,
                       "row3col9", null),
                   new ArrayList<Object>(row.values()));

      table.getNextRow();

      row = table.getNextRow();
      Assert.assertEquals(Arrays.<Object>asList(
                       null, "row5col2", null, null, null, null, null, null,
                       null),
                   new ArrayList<Object>(row.values()));

      table.reset();
      assertRowCount(7, table);

      mdb.close();
    }
  }

  @Test
  public void testUsageMapPromotion() throws Exception {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.PROMOTION)) {
      Database db = openMem(testDB);
      Table t = db.getTable("jobDB1");

      Assert.assertTrue(((TableImpl)t).getOwnedPagesCursor().getUsageMap().toString()
                 .startsWith("InlineHandler"));

      String lval = createNonAsciiString(255); // "--255 chars long text--";

      ((DatabaseImpl)db).getPageChannel().startWrite();
      try {
        for(int i = 0; i < 1000; ++i) {
          t.addRow(i, 13, 57, lval, lval, lval, lval, lval, lval, 47.0d);
        }
      } finally {
        ((DatabaseImpl)db).getPageChannel().finishWrite();
      }

      Set<Integer> ids = new HashSet<Integer>();
      for(Row row : t) {
        ids.add(row.getInt("ID"));
      }
      Assert.assertEquals(1000, ids.size());

      Assert.assertTrue(((TableImpl)t).getOwnedPagesCursor().getUsageMap().toString()
                 .startsWith("ReferenceHandler"));

      db.close();
    }
  }

  @Test
  public void testLargeTableDef() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      final int numColumns = 90;

      List<ColumnBuilder> columns = new ArrayList<ColumnBuilder>();
      List<String> colNames = new ArrayList<String>();
      for(int i = 0; i < numColumns; ++i) {
        String colName = "MyColumnName" + i;
        colNames.add(colName);
        columns.add(new ColumnBuilder(colName, DataType.TEXT).toColumn());
      }

      Table t = new TableBuilder("test")
        .addColumns(columns)
        .toTable(db);

      List<String> row = new ArrayList<String>();
      Map<String,Object> expectedRowData = new LinkedHashMap<String, Object>();
      for(int i = 0; i < numColumns; ++i) {
        String value = "" + i + " some row data";
        row.add(value);
        expectedRowData.put(colNames.get(i), value);
      }

      t.addRow(row.toArray());

      t.reset();
      Assert.assertEquals(expectedRowData, t.getNextRow());

      db.close();
    }
  }

  @Test
  public void testWriteAndReadDate() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = createMem(fileFormat);

      Table table = new TableBuilder("test")
        .addColumn(new ColumnBuilder("name", DataType.TEXT))
        .addColumn(new ColumnBuilder("date", DataType.SHORT_DATE_TIME))
        .toTable(db);

      // since jackcess does not really store millis, shave them off before
      // storing the current date/time
      long curTimeNoMillis = (System.currentTimeMillis() / 1000L);
      curTimeNoMillis *= 1000L;

      DateFormat df = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
      List<Date> dates =
        new ArrayList<Date>(
            Arrays.asList(
                df.parse("19801231 00:00:00"),
                df.parse("19930513 14:43:27"),
                null,
                df.parse("20210102 02:37:00"),
                new Date(curTimeNoMillis)));

      Calendar c = Calendar.getInstance();
      for(int year = 1801; year < 2050; year +=3) {
        for(int month = 0; month <= 12; ++month) {
          for(int day = 1; day < 29; day += 3) {
            c.clear();
            c.set(Calendar.YEAR, year);
            c.set(Calendar.MONTH, month);
            c.set(Calendar.DAY_OF_MONTH, day);
            dates.add(c.getTime());
          }
        }
      }

      ((DatabaseImpl)db).getPageChannel().startWrite();
      try {
        for(Date d : dates) {
          table.addRow("row " + d, d);
        }
      } finally {
        ((DatabaseImpl)db).getPageChannel().finishWrite();
      }

      List<Date> foundDates = new ArrayList<Date>();
      for(Row row : table) {
        foundDates.add(row.getDate("date"));
      }

      Assert.assertEquals(dates.size(), foundDates.size());
      for(int i = 0; i < dates.size(); ++i) {
        Date expected = dates.get(i);
        Date found = foundDates.get(i);
        assertSameDate(expected, found);
      }

      db.close();
    }
  }

  @Test
  public void testAncientDates() throws Exception
  {
    TimeZone tz = TimeZone.getTimeZone("America/New_York");
    SimpleDateFormat sdf = DatabaseBuilder.createDateFormat("yyyy-MM-dd");
    sdf.getCalendar().setTimeZone(tz);

    List<String> dates = Arrays.asList("1582-10-15", "1582-10-14",
                                       "1492-01-10", "1392-01-10");


    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = createMem(fileFormat);
      db.setTimeZone(tz);

      Table table = new TableBuilder("test")
        .addColumn(new ColumnBuilder("name", DataType.TEXT))
        .addColumn(new ColumnBuilder("date", DataType.SHORT_DATE_TIME))
        .toTable(db);

      for(String dateStr : dates) {
        Date d = sdf.parse(dateStr);
        table.addRow("row " + dateStr, d);
      }

      List<String> foundDates = new ArrayList<String>();
      for(Row row : table) {
        foundDates.add(sdf.format(row.getDate("date")));
      }

      Assert.assertEquals(dates, foundDates);

      db.close();
    }

    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.OLD_DATES)) {
      Database db = openCopy(testDB);

      Table t = db.getTable("Table1");

      List<String> foundDates = new ArrayList<String>();
      for(Row row : t) {
        foundDates.add(sdf.format(row.getDate("DateField")));
      }

      Assert.assertEquals(dates, foundDates);

      db.close();
    }

  }

  @Test
  public void testSystemTable() throws Exception
  {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      Set<String> sysTables = new TreeSet<String>(
          String.CASE_INSENSITIVE_ORDER);
      sysTables.addAll(
          Arrays.asList("MSysObjects", "MSysQueries", "MSysACES",
                        "MSysRelationships"));

      if (fileFormat == FileFormat.GENERIC_JET4) {
        Assert.assertNull("file format: " + fileFormat, db.getSystemTable("MSysAccessObjects"));
      } else if (fileFormat.ordinal() < FileFormat.V2003.ordinal()) {
        Assert.assertNotNull("file format: " + fileFormat, db.getSystemTable("MSysAccessObjects"));
        sysTables.add("MSysAccessObjects");
      } else {
        // v2003+ template files have no "MSysAccessObjects" table
        Assert.assertNull("file format: " + fileFormat, db.getSystemTable("MSysAccessObjects"));
        sysTables.addAll(
            Arrays.asList("MSysNavPaneGroupCategories",
                          "MSysNavPaneGroups", "MSysNavPaneGroupToObjects",
                          "MSysNavPaneObjectIDs", "MSysAccessStorage"));
        if(fileFormat.ordinal() >= FileFormat.V2007.ordinal()) {
          sysTables.addAll(
              Arrays.asList(
                  "MSysComplexColumns", "MSysComplexType_Attachment",
                  "MSysComplexType_Decimal", "MSysComplexType_GUID",
                  "MSysComplexType_IEEEDouble", "MSysComplexType_IEEESingle",
                  "MSysComplexType_Long", "MSysComplexType_Short",
                  "MSysComplexType_Text", "MSysComplexType_UnsignedByte"));
        }
        if(fileFormat.ordinal() >= FileFormat.V2010.ordinal()) {
          sysTables.add("f_12D7448B56564D8AAE333BCC9B3718E5_Data");
          sysTables.add("MSysResources");
        }
      }

      Assert.assertEquals(sysTables, db.getSystemTableNames());

      Assert.assertNotNull(db.getSystemTable("MSysObjects"));
      Assert.assertNotNull(db.getSystemTable("MSysQueries"));
      Assert.assertNotNull(db.getSystemTable("MSysACES"));
      Assert.assertNotNull(db.getSystemTable("MSysRelationships"));

      Assert.assertNull(db.getSystemTable("MSysBogus"));

      TableMetaData tmd = db.getTableMetaData("MSysObjects");
      Assert.assertEquals("MSysObjects", tmd.getName());
      Assert.assertFalse(tmd.isLinked());
      Assert.assertTrue(tmd.isSystem());

      db.close();
    }
  }

  @Test
  public void testFixedText() throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.FIXED_TEXT)) {
      Database db = openCopy(testDB);

      Table t = db.getTable("users");
      Column c = t.getColumn("c_flag_");
      Assert.assertEquals(DataType.TEXT, c.getType());
      Assert.assertEquals(false, c.isVariableLength());
      Assert.assertEquals(2, c.getLength());

      Map<String,Object> row = t.getNextRow();
      Assert.assertEquals("N", row.get("c_flag_"));

      t.addRow(3, "testFixedText", "boo", "foo", "bob", 3, 5, 9, "Y",
               new Date());

      t.getNextRow();
      row = t.getNextRow();
      Assert.assertEquals("testFixedText", row.get("c_user_login"));
      Assert.assertEquals("Y", row.get("c_flag_"));

      db.close();
    }
  }

  @Test
  public void testDbSortOrder() throws Exception {

    for (final TestDB testDB : SUPPORTED_DBS_TEST_FOR_READ) {

      Database db = open(testDB);
      Assert.assertEquals(((DatabaseImpl)db).getFormat().DEFAULT_SORT_ORDER,
                   ((DatabaseImpl)db).getDefaultSortOrder());
      db.close();
    }
  }

  @Test
  public void testUnsupportedColumns() throws Exception {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.UNSUPPORTED)) {

      Database db = open(testDB);
      Table t = db.getTable("Test");
      Column varCol = t.getColumn("UnknownVar");
      Assert.assertEquals(DataType.UNSUPPORTED_VARLEN, varCol.getType());
      Column fixCol = t.getColumn("UnknownFix");
      Assert.assertEquals(DataType.UNSUPPORTED_FIXEDLEN, fixCol.getType());

      List<String> varVals = Arrays.asList(
          "RawData[(10) FF FE 73 6F  6D 65 64 61  74 61]",
          "RawData[(12) FF FE 6F 74  68 65 72 20  64 61 74 61]",
          null);
      List<String> fixVals = Arrays.asList("RawData[(4) 37 00 00 00]",
                                           "RawData[(4) F3 FF FF FF]",
                                           "RawData[(4) 02 00 00 00]");

      int idx = 0;
      for(Map<String,Object> row : t) {
        checkRawValue(varVals.get(idx), varCol.getRowValue(row));
        checkRawValue(fixVals.get(idx), fixCol.getRowValue(row));
        ++idx;
      }
      db.close();
    }
  }

  @Test
  public void testLinkedTables() throws Exception {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.LINKED)) {
      Database db = openCopy(testDB);

      try {
        db.getTable("Table2");
        Assert.fail("FileNotFoundException should have been thrown");
      } catch(FileNotFoundException e) {
        // success
      }

      TableMetaData tmd = db.getTableMetaData("Table2");
      Assert.assertEquals("Table2", tmd.getName());
      Assert.assertTrue(tmd.isLinked());
      Assert.assertFalse(tmd.isSystem());
      Assert.assertEquals("Table1", tmd.getLinkedTableName());
      Assert.assertEquals("Z:\\jackcess_test\\linkeeTest.accdb", tmd.getLinkedDbName());

      tmd = db.getTableMetaData("FooTable");
      Assert.assertNull(tmd);

      Assert.assertTrue(db.getLinkedDatabases().isEmpty());

      final String linkeeDbName = "Z:\\jackcess_test\\linkeeTest.accdb";
      final File linkeeFile = new File("src/test/data/linkeeTest.accdb");
      db.setLinkResolver(new LinkResolver() {
        public Database resolveLinkedDatabase(Database linkerdb, String dbName)
          throws IOException {
          Assert.assertEquals(linkeeDbName, dbName);
          return DatabaseBuilder.open(linkeeFile);
        }
      });

      Table t2 = db.getTable("Table2");

      Assert.assertEquals(1, db.getLinkedDatabases().size());
      Database linkeeDb = db.getLinkedDatabases().get(linkeeDbName);
      Assert.assertNotNull(linkeeDb);
      Assert.assertEquals(linkeeFile, linkeeDb.getFile());
      Assert.assertEquals("linkeeTest.accdb", ((DatabaseImpl)linkeeDb).getName());

      List<? extends Map<String, Object>> expectedRows =
        createExpectedTable(
            createExpectedRow(
                "ID", 1,
                "Field1", "bar"));

      assertTable(expectedRows, t2);

      db.createLinkedTable("FooTable", linkeeDbName, "Table2");

      tmd = db.getTableMetaData("FooTable");
      Assert.assertEquals("FooTable", tmd.getName());
      Assert.assertTrue(tmd.isLinked());
      Assert.assertFalse(tmd.isSystem());
      Assert.assertEquals("Table2", tmd.getLinkedTableName());
      Assert.assertEquals("Z:\\jackcess_test\\linkeeTest.accdb", tmd.getLinkedDbName());

      Table t3 = db.getTable("FooTable");

      Assert.assertEquals(1, db.getLinkedDatabases().size());

      expectedRows =
        createExpectedTable(
            createExpectedRow(
                "ID", 1,
                "Field1", "buzz"));

      assertTable(expectedRows, t3);

      tmd = db.getTableMetaData("Table1");
      Assert.assertEquals("Table1", tmd.getName());
      Assert.assertFalse(tmd.isLinked());
      Assert.assertFalse(tmd.isSystem());
      Assert.assertNull(tmd.getLinkedTableName());
      Assert.assertNull(tmd.getLinkedDbName());

      Table t1 = tmd.open(db);

      Assert.assertFalse(db.isLinkedTable(null));
      Assert.assertTrue(db.isLinkedTable(t2));
      Assert.assertTrue(db.isLinkedTable(t3));
      Assert.assertFalse(db.isLinkedTable(t1));

      List<Table> tables = getTables(db.newIterable());
      Assert.assertEquals(3, tables.size());
      Assert.assertTrue(tables.contains(t1));
      Assert.assertTrue(tables.contains(t2));
      Assert.assertTrue(tables.contains(t3));
      Assert.assertFalse(tables.contains(((DatabaseImpl)db).getSystemCatalog()));

      tables = getTables(db.newIterable().setIncludeNormalTables(false));
      Assert.assertEquals(2, tables.size());
      Assert.assertFalse(tables.contains(t1));
      Assert.assertTrue(tables.contains(t2));
      Assert.assertTrue(tables.contains(t3));
      Assert.assertFalse(tables.contains(((DatabaseImpl)db).getSystemCatalog()));

      tables = getTables(db.newIterable().withLocalUserTablesOnly());
      Assert.assertEquals(1, tables.size());
      Assert.assertTrue(tables.contains(t1));
      Assert.assertFalse(tables.contains(t2));
      Assert.assertFalse(tables.contains(t3));
      Assert.assertFalse(tables.contains(((DatabaseImpl)db).getSystemCatalog()));

      tables = getTables(db.newIterable().withSystemTablesOnly());
      Assert.assertTrue(tables.size() > 5);
      Assert.assertFalse(tables.contains(t1));
      Assert.assertFalse(tables.contains(t2));
      Assert.assertFalse(tables.contains(t3));
      Assert.assertTrue(tables.contains(((DatabaseImpl)db).getSystemCatalog()));

      db.close();
    }
  }

  private static List<Table> getTables(Iterable<Table> tableIter)
  {
    List<Table> tableList = new ArrayList<Table>();
    for(Table t : tableIter) {
      tableList.add(t);
    }
    return tableList;
  }

  @Test
  public void testTimeZone() throws Exception
  {
    TimeZone tz = TimeZone.getTimeZone("America/New_York");
    doTestTimeZone(tz);

    tz = TimeZone.getTimeZone("Australia/Sydney");
    doTestTimeZone(tz);
  }

  private static void doTestTimeZone(final TimeZone tz) throws Exception
  {
    ColumnImpl col = new ColumnImpl(null, null, DataType.SHORT_DATE_TIME, 0, 0, 0) {
      @Override
      public TimeZone getTimeZone() { return tz; }
      @Override
      public ZoneId getZoneId() { return null; }
      @Override
      public ColumnImpl.DateTimeFactory getDateTimeFactory() {
        return getDateTimeFactory(DateTimeType.DATE);
      }
    };

    SimpleDateFormat df = new SimpleDateFormat("yyyy.MM.dd");
    df.setTimeZone(tz);

    long startDate = df.parse("2012.01.01").getTime();
    long endDate = df.parse("2013.01.01").getTime();

    Calendar curCal = Calendar.getInstance(tz);
    curCal.setTimeInMillis(startDate);

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
    sdf.setTimeZone(tz);

    while(curCal.getTimeInMillis() < endDate) {
      Date curDate = curCal.getTime();
      Date newDate = new Date(col.fromDateDouble(col.toDateDouble(curDate)));
      if(curDate.getTime() != newDate.getTime()) {
        Assert.assertEquals(sdf.format(curDate), sdf.format(newDate));
      }
      curCal.add(Calendar.MINUTE, 30);
    }
  }

  @Test
  public void testToString()
  {
    RowImpl row = new RowImpl(new RowIdImpl(1, 1));
    row.put("id", 37);
    row.put("data", null);
    Assert.assertEquals("Row[1:1][{id=37,data=<null>}]", row.toString());
  }

  @Test
  public void testIterateTableNames() throws Exception {
    for (final TestDB testDB : SUPPORTED_DBS_TEST_FOR_READ) {
      final Database db = open(testDB);

      Set<String> names = new HashSet<>();
      int sysCount = 0;
      for(TableMetaData tmd : db.newTableMetaDataIterable()) {
        if(tmd.isSystem()) {
          ++sysCount;
          continue;
        }
        Assert.assertFalse(tmd.isLinked());
        Assert.assertNull(tmd.getLinkedTableName());
        Assert.assertNull(tmd.getLinkedDbName());
        names.add(tmd.getName());
      }

      Assert.assertTrue(sysCount > 4);
      Assert.assertEquals(new HashSet<>(Arrays.asList("Table1", "Table2", "Table3",
                                               "Table4")),
                   names);
    }

    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.LINKED)) {
      final Database db = open(testDB);

      Set<String> names = new HashSet<>();
      for(TableMetaData tmd : db.newTableMetaDataIterable()) {
        if(tmd.isSystem()) {
          continue;
        }
        if("Table1".equals(tmd.getName())) {
          Assert.assertFalse(tmd.isLinked());
          Assert.assertNull(tmd.getLinkedTableName());
          Assert.assertNull(tmd.getLinkedDbName());
        } else {
          Assert.assertTrue(tmd.isLinked());
          Assert.assertEquals("Table1", tmd.getLinkedTableName());
          Assert.assertEquals("Z:\\jackcess_test\\linkeeTest.accdb", tmd.getLinkedDbName());
        }
        names.add(tmd.getName());
      }

      Assert.assertEquals(new HashSet<>(Arrays.asList("Table1", "Table2")),
                   names);
    }
  }

  private static void checkRawValue(String expected, Object val)
  {
    if(expected != null) {
      Assert.assertTrue(ColumnImpl.isRawData(val));
      Assert.assertEquals(expected, val.toString());
    } else {
      Assert.assertNull(val);
    }
  }
}
