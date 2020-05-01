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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.healthmarketscience.jackcess.Database.*;
import com.healthmarketscience.jackcess.impl.ColumnImpl;
import com.healthmarketscience.jackcess.impl.JetFormatTest;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import com.healthmarketscience.jackcess.impl.RowIdImpl;
import com.healthmarketscience.jackcess.impl.TableImpl;
import com.healthmarketscience.jackcess.util.CaseInsensitiveColumnMatcher;
import com.healthmarketscience.jackcess.util.ColumnMatcher;
import com.healthmarketscience.jackcess.util.RowFilterTest;
import com.healthmarketscience.jackcess.util.SimpleColumnMatcher;
import static com.healthmarketscience.jackcess.TestUtil.*;

/**
 * @author James Ahlborn
 */
public class CursorTest {

  static final List<TestDB> INDEX_CURSOR_DBS =
    TestDB.getSupportedForBasename(Basename.INDEX_CURSOR);

  @Before
  public void setUp() {
    TestUtil.setTestAutoSync(false);
  }

  @After
  public void tearDown() {
    TestUtil.clearTestAutoSync();
  }

  private static List<Map<String,Object>> createTestTableData()
    throws Exception
  {
    List<Map<String,Object>> expectedRows =
      new ArrayList<Map<String,Object>>();
    for(int i = 0; i < 10; ++i) {
      expectedRows.add(createExpectedRow("id", i, "value", "data" + i));
    }
    return expectedRows;
  }

  private static List<Map<String,Object>> createTestTableData(
      int startIdx,
      int endIdx)
    throws Exception
  {
    List<Map<String,Object>> expectedRows = createTestTableData();
    expectedRows.subList(endIdx, expectedRows.size()).clear();
    expectedRows.subList(0, startIdx).clear();
    return expectedRows;
  }

  private static Database createTestTable(final FileFormat fileFormat)
    throws Exception
  {
    Database db = createMem(fileFormat);

    Table table = new TableBuilder("test")
      .addColumn(new ColumnBuilder("id", DataType.LONG))
      .addColumn(new ColumnBuilder("value", DataType.TEXT))
      .toTable(db);

    for(Map<String,Object> row : createTestTableData()) {
      table.addRow(row.get("id"), row.get("value"));
    }

    return db;
  }

  private static List<Map<String,Object>> createUnorderedTestTableData()
    throws Exception
  {
    List<Map<String,Object>> expectedRows =
      new ArrayList<Map<String,Object>>();
    int[] ids = new int[]{3, 7, 6, 1, 2, 9, 0, 5, 4, 8};
    for(int i : ids) {
      expectedRows.add(createExpectedRow("id", i, "value", "data" + i));
    }
    return expectedRows;
  }

  static Database createTestIndexTable(final TestDB indexCursorDB)
    throws Exception
  {
    Database db = openMem(indexCursorDB);

    Table table = db.getTable("test");

    for(Map<String,Object> row : createUnorderedTestTableData()) {
      table.addRow(row.get("id"), row.get("value"));
    }

    return db;
  }

  private static List<Map<String,Object>> createDupeTestTableData()
    throws Exception
  {
    List<Map<String,Object>> expectedRows =
      new ArrayList<Map<String,Object>>();
    int[] ids = new int[]{3, 7, 6, 1, 2, 9, 0, 5, 4, 8};
    for(int i : ids) {
      expectedRows.add(createExpectedRow("id", i, "value", "data" + (i % 3)));
    }
    for(int i : ids) {
      expectedRows.add(createExpectedRow("id", i, "value", "data" + (i % 5)));
    }
    return expectedRows;
  }

  private static Database createDupeTestTable(final FileFormat fileFormat)
    throws Exception
  {
    Database db = createMem(fileFormat);

    Table table = new TableBuilder("test")
      .addColumn(new ColumnBuilder("id", DataType.LONG))
      .addColumn(new ColumnBuilder("value", DataType.TEXT))
      .toTable(db);

    for(Map<String,Object> row : createDupeTestTableData()) {
      table.addRow(row.get("id"), row.get("value"));
    }

    return db;
  }

  static Database createDupeTestTable(final TestDB indexCursorDB)
    throws Exception
  {
    Database db = openMem(indexCursorDB);

    Table table = db.getTable("test");

    for(Map<String,Object> row : createDupeTestTableData()) {
      table.addRow(row.get("id"), row.get("value"));
    }

    return db;
  }

  private static Cursor createIndexSubRangeCursor(Table table,
                                                  Index idx,
                                                  int type)
    throws Exception
  {
    return table.newCursor()
      .setIndex(idx)
      .setStartEntry(3 - type)
      .setStartRowInclusive(type == 0)
      .setEndEntry(8 + type)
      .setEndRowInclusive(type == 0)
      .toCursor();
  }

  @Test
  public void testRowId() throws Exception {
    // test special cases
    RowIdImpl rowId1 = new RowIdImpl(1, 2);
    RowIdImpl rowId2 = new RowIdImpl(1, 3);
    RowIdImpl rowId3 = new RowIdImpl(2, 1);

    List<RowIdImpl> sortedRowIds =
      new ArrayList<RowIdImpl>(new TreeSet<RowIdImpl>(
        Arrays.asList(rowId1, rowId2, rowId3, RowIdImpl.FIRST_ROW_ID,
                      RowIdImpl.LAST_ROW_ID)));

    Assert.assertEquals(Arrays.asList(RowIdImpl.FIRST_ROW_ID, rowId1, rowId2, rowId3,
                               RowIdImpl.LAST_ROW_ID),
                 sortedRowIds);
  }

  @Test
  public void testSimple() throws Exception {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createTestTable(fileFormat);

      Table table = db.getTable("test");
      Cursor cursor = CursorBuilder.createCursor(table);
      doTestSimple(cursor, null);
      db.close();
    }
  }

  private static void doTestSimple(Cursor cursor,
                                   List<Map<String, Object>> expectedRows)
    throws Exception
  {
    if(expectedRows == null) {
      expectedRows = createTestTableData();
    }

    List<Map<String, Object>> foundRows =
      new ArrayList<Map<String, Object>>();
    for(Map<String, Object> row : cursor) {
      foundRows.add(row);
    }
    Assert.assertEquals(expectedRows, foundRows);
  }

  @Test
  public void testMove() throws Exception {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createTestTable(fileFormat);

      Table table = db.getTable("test");
      Cursor cursor = CursorBuilder.createCursor(table);
      doTestMove(cursor, null);

      db.close();
    }
  }

  private static void doTestMove(Cursor cursor,
                                 List<Map<String, Object>> expectedRows)
    throws Exception
  {
    if(expectedRows == null) {
      expectedRows = createTestTableData();
    }
    expectedRows.subList(1, 4).clear();

    List<Map<String, Object>> foundRows =
      new ArrayList<Map<String, Object>>();
    Assert.assertTrue(cursor.isBeforeFirst());
    Assert.assertFalse(cursor.isAfterLast());
    foundRows.add(cursor.getNextRow());
    Assert.assertEquals(3, cursor.moveNextRows(3));
    Assert.assertFalse(cursor.isBeforeFirst());
    Assert.assertFalse(cursor.isAfterLast());

    Map<String,Object> expectedRow = cursor.getCurrentRow();
    Cursor.Savepoint savepoint = cursor.getSavepoint();
    Assert.assertEquals(2, cursor.movePreviousRows(2));
    Assert.assertEquals(2, cursor.moveNextRows(2));
    Assert.assertTrue(cursor.moveToNextRow());
    Assert.assertTrue(cursor.moveToPreviousRow());
    Assert.assertEquals(expectedRow, cursor.getCurrentRow());

    while(cursor.moveToNextRow()) {
      foundRows.add(cursor.getCurrentRow());
    }
    Assert.assertEquals(expectedRows, foundRows);
    Assert.assertFalse(cursor.isBeforeFirst());
    Assert.assertTrue(cursor.isAfterLast());

    Assert.assertEquals(0, cursor.moveNextRows(3));

    cursor.beforeFirst();
    Assert.assertTrue(cursor.isBeforeFirst());
    Assert.assertFalse(cursor.isAfterLast());

    cursor.afterLast();
    Assert.assertFalse(cursor.isBeforeFirst());
    Assert.assertTrue(cursor.isAfterLast());

    cursor.restoreSavepoint(savepoint);
    Assert.assertEquals(expectedRow, cursor.getCurrentRow());
  }

  @Test
  public void testMoveNoReset() throws Exception {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createTestTable(fileFormat);

      Table table = db.getTable("test");
      Cursor cursor = CursorBuilder.createCursor(table);
      doTestMoveNoReset(cursor);

      db.close();
    }
  }

  private static void doTestMoveNoReset(Cursor cursor)
    throws Exception
  {
    List<Map<String, Object>> expectedRows = createTestTableData();
    List<Map<String, Object>> foundRows = new ArrayList<Map<String, Object>>();

    Iterator<Row> iter = cursor.newIterable().iterator();

    for(int i = 0; i < 6; ++i) {
      foundRows.add(iter.next());
    }

    iter = cursor.newIterable().reset(false).reverse().iterator();
    iter.next();
    Map<String, Object> row = iter.next();
    Assert.assertEquals(expectedRows.get(4), row);

    iter = cursor.newIterable().reset(false).iterator();
    iter.next();
    row = iter.next();
    Assert.assertEquals(expectedRows.get(5), row);
    iter.next();

    iter = cursor.newIterable().reset(false).iterator();
    for(int i = 6; i < 10; ++i) {
      foundRows.add(iter.next());
    }

    Assert.assertEquals(expectedRows, foundRows);
  }

  @Test
  public void testSearch() throws Exception {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createTestTable(fileFormat);

      Table table = db.getTable("test");
      Cursor cursor = CursorBuilder.createCursor(table);
      doTestSearch(table, cursor, null, 42, -13);

      db.close();
    }
  }

  private static void doTestSearch(Table table, Cursor cursor, Index index,
                                   Integer... outOfRangeValues)
    throws Exception
  {
    Assert.assertTrue(cursor.findFirstRow(table.getColumn("id"), 3));
    Assert.assertEquals(createExpectedRow("id", 3,
                                   "value", "data" + 3),
                 cursor.getCurrentRow());

    Assert.assertTrue(cursor.findFirstRow(createExpectedRow(
                                    "id", 6,
                                    "value", "data" + 6)));
    Assert.assertEquals(createExpectedRow("id", 6,
                                   "value", "data" + 6),
                 cursor.getCurrentRow());

    Assert.assertFalse(cursor.findFirstRow(createExpectedRow(
                                   "id", 8,
                                   "value", "data" + 13)));
    Assert.assertFalse(cursor.findFirstRow(table.getColumn("id"), 13));
    Assert.assertEquals(createExpectedRow("id", 6,
                                   "value", "data" + 6),
                 cursor.getCurrentRow());

    Assert.assertTrue(cursor.findFirstRow(createExpectedRow(
                                    "value", "data" + 7)));
    Assert.assertEquals(createExpectedRow("id", 7,
                                   "value", "data" + 7),
                 cursor.getCurrentRow());

    Assert.assertTrue(cursor.findFirstRow(table.getColumn("value"), "data" + 4));
    Assert.assertEquals(createExpectedRow("id", 4,
                                   "value", "data" + 4),
                 cursor.getCurrentRow());

    for(Integer outOfRangeValue : outOfRangeValues) {
      Assert.assertFalse(cursor.findFirstRow(table.getColumn("id"),
                                 outOfRangeValue));
      Assert.assertFalse(cursor.findFirstRow(table.getColumn("value"),
                                 "data" + outOfRangeValue));
      Assert.assertFalse(cursor.findFirstRow(createExpectedRow(
                                     "id", outOfRangeValue,
                                     "value", "data" + outOfRangeValue)));
    }

    Assert.assertEquals("data" + 5,
                 CursorBuilder.findValue(table,
                                  table.getColumn("value"),
                                  table.getColumn("id"), 5));
    Assert.assertEquals(createExpectedRow("id", 5,
                                   "value", "data" + 5),
                 CursorBuilder.findRow(table,
                                createExpectedRow("id", 5)));
    if(index != null) {
      Assert.assertEquals("data" + 5,
                   CursorBuilder.findValue(index,
                                    table.getColumn("value"),
                                    table.getColumn("id"), 5));
      Assert.assertEquals(createExpectedRow("id", 5,
                                     "value", "data" + 5),
                   CursorBuilder.findRow(index,
                                  createExpectedRow("id", 5)));

      Assert.assertNull(CursorBuilder.findValue(index,
                                  table.getColumn("value"),
                                  table.getColumn("id"),
                                  -17));
      Assert.assertNull(CursorBuilder.findRow(index,
                                createExpectedRow("id", 13)));
    }
  }

  @Test
  public void testReverse() throws Exception {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createTestTable(fileFormat);

      Table table = db.getTable("test");
      Cursor cursor = CursorBuilder.createCursor(table);
      doTestReverse(cursor, null);

      db.close();
    }
  }

  private static void doTestReverse(Cursor cursor,
                                    List<Map<String, Object>> expectedRows)
    throws Exception
  {
    if(expectedRows == null) {
      expectedRows = createTestTableData();
    }
    Collections.reverse(expectedRows);

    List<Map<String, Object>> foundRows =
      new ArrayList<Map<String, Object>>();
    for(Map<String, Object> row : cursor.newIterable().reverse()) {
      foundRows.add(row);
    }
    Assert.assertEquals(expectedRows, foundRows);
  }

  @Test
  public void testLiveAddition() throws Exception {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createTestTable(fileFormat);

      Table table = db.getTable("test");

      Cursor cursor1 = CursorBuilder.createCursor(table);
      Cursor cursor2 = CursorBuilder.createCursor(table);
      doTestLiveAddition(table, cursor1, cursor2, 11);

      db.close();
    }
  }

  private static void doTestLiveAddition(Table table,
                                         Cursor cursor1,
                                         Cursor cursor2,
                                         Integer newRowNum) throws Exception
  {
    cursor1.moveNextRows(11);
    cursor2.moveNextRows(11);

    Assert.assertTrue(cursor1.isAfterLast());
    Assert.assertTrue(cursor2.isAfterLast());

    table.addRow(newRowNum, "data" + newRowNum);
    Map<String,Object> expectedRow =
      createExpectedRow("id", newRowNum, "value", "data" + newRowNum);

    Assert.assertFalse(cursor1.isAfterLast());
    Assert.assertFalse(cursor2.isAfterLast());

    Assert.assertEquals(expectedRow, cursor1.getCurrentRow());
    Assert.assertEquals(expectedRow, cursor2.getCurrentRow());
    Assert.assertFalse(cursor1.moveToNextRow());
    Assert.assertFalse(cursor2.moveToNextRow());
    Assert.assertTrue(cursor1.isAfterLast());
    Assert.assertTrue(cursor2.isAfterLast());
  }


  @Test
  public void testLiveDeletion() throws Exception {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createTestTable(fileFormat);

      Table table = db.getTable("test");

      Cursor cursor1 = CursorBuilder.createCursor(table);
      Cursor cursor2 = CursorBuilder.createCursor(table);
      Cursor cursor3 = CursorBuilder.createCursor(table);
      Cursor cursor4 = CursorBuilder.createCursor(table);
      doTestLiveDeletion(cursor1, cursor2, cursor3, cursor4, 1);

      db.close();
    }
  }

  private static void doTestLiveDeletion(
          Cursor cursor1,
          Cursor cursor2,
          Cursor cursor3,
          Cursor cursor4,
          int firstValue) throws Exception
  {
    Assert.assertEquals(2, cursor1.moveNextRows(2));
    Assert.assertEquals(3, cursor2.moveNextRows(3));
    Assert.assertEquals(3, cursor3.moveNextRows(3));
    Assert.assertEquals(4, cursor4.moveNextRows(4));

    Map<String,Object> expectedPrevRow =
      createExpectedRow("id", firstValue, "value", "data" + firstValue);
    ++firstValue;
    Map<String,Object> expectedDeletedRow =
      createExpectedRow("id", firstValue, "value", "data" + firstValue);
    ++firstValue;
    Map<String,Object> expectedNextRow =
      createExpectedRow("id", firstValue, "value", "data" + firstValue);

    Assert.assertEquals(expectedDeletedRow, cursor2.getCurrentRow());
    Assert.assertEquals(expectedDeletedRow, cursor3.getCurrentRow());

    Assert.assertFalse(cursor2.isCurrentRowDeleted());
    Assert.assertFalse(cursor3.isCurrentRowDeleted());

    cursor2.deleteCurrentRow();

    Assert.assertTrue(cursor2.isCurrentRowDeleted());
    Assert.assertTrue(cursor3.isCurrentRowDeleted());

    Assert.assertEquals(expectedNextRow, cursor1.getNextRow());
    Assert.assertEquals(expectedNextRow, cursor2.getNextRow());
    Assert.assertEquals(expectedNextRow, cursor3.getNextRow());

    Assert.assertEquals(expectedPrevRow, cursor3.getPreviousRow());

    Assert.assertTrue(cursor3.moveToNextRow());
    cursor3.deleteCurrentRow();
    Assert.assertTrue(cursor3.isCurrentRowDeleted());

    firstValue += 2;
    expectedNextRow =
      createExpectedRow("id", firstValue, "value", "data" + firstValue);
    Assert.assertTrue(cursor3.moveToNextRow());
    Assert.assertEquals(expectedNextRow, cursor3.getNextRow());

    cursor1.beforeFirst();
    Assert.assertTrue(cursor1.moveToNextRow());
    cursor1.deleteCurrentRow();
    Assert.assertFalse(cursor1.isBeforeFirst());
    Assert.assertFalse(cursor1.isAfterLast());
    Assert.assertFalse(cursor1.moveToPreviousRow());
    Assert.assertTrue(cursor1.isBeforeFirst());
    Assert.assertFalse(cursor1.isAfterLast());

    cursor1.afterLast();
    Assert.assertTrue(cursor1.moveToPreviousRow());
    cursor1.deleteCurrentRow();
    Assert.assertFalse(cursor1.isBeforeFirst());
    Assert.assertFalse(cursor1.isAfterLast());
    Assert.assertFalse(cursor1.moveToNextRow());
    Assert.assertFalse(cursor1.isBeforeFirst());
    Assert.assertTrue(cursor1.isAfterLast());

    cursor1.beforeFirst();
    while(cursor1.moveToNextRow()) {
      cursor1.deleteCurrentRow();
    }

    Assert.assertTrue(cursor1.isAfterLast());
    Assert.assertTrue(cursor2.isCurrentRowDeleted());
    Assert.assertTrue(cursor3.isCurrentRowDeleted());
    Assert.assertTrue(cursor4.isCurrentRowDeleted());
  }

  @Test
  public void testSimpleIndex() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);

      assertTable(createUnorderedTestTableData(), table);

      Cursor cursor = CursorBuilder.createCursor(idx);
      doTestSimple(cursor, null);

      db.close();
    }
  }

  @Test
  public void testMoveIndex() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);
      Cursor cursor = CursorBuilder.createCursor(idx);
      doTestMove(cursor, null);

      db.close();
    }
  }

  @Test
  public void testReverseIndex() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);
      Cursor cursor = CursorBuilder.createCursor(idx);
      doTestReverse(cursor, null);

      db.close();
    }
  }

  @Test
  public void testSearchIndex() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);
      Cursor cursor = CursorBuilder.createCursor(idx);
      doTestSearch(table, cursor, idx, 42, -13);

      db.close();
    }
  }

  @Test
  public void testLiveAdditionIndex() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);

      Cursor cursor1 = CursorBuilder.createCursor(idx);
      Cursor cursor2 = CursorBuilder.createCursor(idx);
      doTestLiveAddition(table, cursor1, cursor2, 11);

      db.close();
    }
  }

  @Test
  public void testLiveDeletionIndex() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);

      Cursor cursor1 = CursorBuilder.createCursor(idx);
      Cursor cursor2 = CursorBuilder.createCursor(idx);
      Cursor cursor3 = CursorBuilder.createCursor(idx);
      Cursor cursor4 = CursorBuilder.createCursor(idx);
      doTestLiveDeletion(cursor1, cursor2, cursor3, cursor4, 1);

      db.close();
    }
  }

  @Test
  public void testSimpleIndexSubRange() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      for(int i = 0; i < 2; ++i) {
        Database db = createTestIndexTable(indexCursorDB);

        Table table = db.getTable("test");
        Index idx = table.getIndexes().get(0);

        Cursor cursor = createIndexSubRangeCursor(table, idx, i);

        List<Map<String,Object>> expectedRows =
          createTestTableData(3, 9);

        doTestSimple(cursor, expectedRows);

        db.close();
      }
    }
  }

  @Test
  public void testMoveIndexSubRange() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      for(int i = 0; i < 2; ++i) {
        Database db = createTestIndexTable(indexCursorDB);

        Table table = db.getTable("test");
        Index idx = table.getIndexes().get(0);

        Cursor cursor = createIndexSubRangeCursor(table, idx, i);

        List<Map<String,Object>> expectedRows =
          createTestTableData(3, 9);

        doTestMove(cursor, expectedRows);

        db.close();
      }
    }
  }

  @Test
  public void testSearchIndexSubRange() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      for(int i = 0; i < 2; ++i) {
        Database db = createTestIndexTable(indexCursorDB);

        Table table = db.getTable("test");
        Index idx = table.getIndexes().get(0);

        Cursor cursor = createIndexSubRangeCursor(table, idx, i);

        doTestSearch(table, cursor, idx, 2, 9);

        db.close();
      }
    }
  }

  @Test
  public void testReverseIndexSubRange() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      for(int i = 0; i < 2; ++i) {
        Database db = createTestIndexTable(indexCursorDB);

        Table table = db.getTable("test");
        Index idx = table.getIndexes().get(0);

        Cursor cursor = createIndexSubRangeCursor(table, idx, i);

        List<Map<String,Object>> expectedRows =
          createTestTableData(3, 9);

        doTestReverse(cursor, expectedRows);

        db.close();
      }
    }
  }

  @Test
  public void testLiveAdditionIndexSubRange() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      for(int i = 0; i < 2; ++i) {
        Database db = createTestIndexTable(indexCursorDB);

        Table table = db.getTable("test");
        Index idx = table.getIndexes().get(0);

        Cursor cursor1 = createIndexSubRangeCursor(table, idx, i);
        Cursor cursor2 = createIndexSubRangeCursor(table, idx, i);

        doTestLiveAddition(table, cursor1, cursor2, 8);

        db.close();
      }
    }
  }

  @Test
  public void testLiveDeletionIndexSubRange() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      for(int i = 0; i < 2; ++i) {
        Database db = createTestIndexTable(indexCursorDB);

        Table table = db.getTable("test");
        Index idx = table.getIndexes().get(0);

        Cursor cursor1 = createIndexSubRangeCursor(table, idx, i);
        Cursor cursor2 = createIndexSubRangeCursor(table, idx, i);
        Cursor cursor3 = createIndexSubRangeCursor(table, idx, i);
        Cursor cursor4 = createIndexSubRangeCursor(table, idx, i);

        doTestLiveDeletion(cursor1, cursor2, cursor3, cursor4, 4);

        db.close();
      }
    }
  }

  @Test
  public void testFindAllIndex() throws Exception {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createDupeTestTable(fileFormat);

      Table table = db.getTable("test");
      Cursor cursor = CursorBuilder.createCursor(table);

      doTestFindAll(table, cursor, null);

      db.close();
    }
  }

  @Test
  public void testFindAll() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createDupeTestTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);
      Cursor cursor = CursorBuilder.createCursor(idx);

      doTestFindAll(table, cursor, idx);

      db.close();
    }
  }

  private static void doTestFindAll(Table table, Cursor cursor, Index index)
    throws Exception
  {
    List<? extends Map<String,Object>> rows = RowFilterTest.toList(
        cursor.newIterable().setMatchPattern("value", "data2"));

    List<? extends Map<String, Object>> expectedRows = null;

    if(index == null) {
      expectedRows =
        createExpectedTable(
            createExpectedRow(
                "id", 2, "value", "data2"),
            createExpectedRow(
                "id", 5, "value", "data2"),
            createExpectedRow(
                "id", 8, "value", "data2"),
            createExpectedRow(
                "id", 7, "value", "data2"),
            createExpectedRow(
                "id", 2, "value", "data2"));
    } else {
      expectedRows =
        createExpectedTable(
            createExpectedRow(
                "id", 2, "value", "data2"),
            createExpectedRow(
                "id", 2, "value", "data2"),
            createExpectedRow(
                "id", 5, "value", "data2"),
            createExpectedRow(
                "id", 7, "value", "data2"),
            createExpectedRow(
                "id", 8, "value", "data2"));
    }
    Assert.assertEquals(expectedRows, rows);

    Column valCol = table.getColumn("value");
    rows = RowFilterTest.toList(
        cursor.newIterable().setMatchPattern(valCol, "data4"));

    if(index == null) {
      expectedRows =
        createExpectedTable(
            createExpectedRow(
                "id", 9, "value", "data4"),
            createExpectedRow(
                "id", 4, "value", "data4"));
    } else {
      expectedRows =
        createExpectedTable(
            createExpectedRow(
                "id", 4, "value", "data4"),
            createExpectedRow(
                "id", 9, "value", "data4"));
    }
    Assert.assertEquals(expectedRows, rows);

    rows = RowFilterTest.toList(
        cursor.newIterable().setMatchPattern(valCol, "data9"));

    Assert.assertTrue(rows.isEmpty());

    rows = RowFilterTest.toList(
        cursor.newIterable().setMatchPattern(
            Collections.singletonMap("id", 8)));

    expectedRows =
      createExpectedTable(
          createExpectedRow(
              "id", 8, "value", "data2"),
          createExpectedRow(
              "id", 8, "value", "data3"));
    Assert.assertEquals(expectedRows, rows);

    for(Map<String,Object> row : table) {

      List<Map<String,Object>> tmpRows = new ArrayList<Map<String,Object>>();
      for(Map<String,Object> tmpRow : cursor) {
        if(row.equals(tmpRow)) {
          tmpRows.add(tmpRow);
        }
      }
      expectedRows = tmpRows;
      Assert.assertFalse(expectedRows.isEmpty());

      rows = RowFilterTest.toList(cursor.newIterable().setMatchPattern(row));

      Assert.assertEquals(expectedRows, rows);
    }

    rows = RowFilterTest.toList(
        cursor.newIterable().addMatchPattern("id", 8)
        .addMatchPattern("value", "data13"));
    Assert.assertTrue(rows.isEmpty());
  }

  @Test
  public void testId() throws Exception
  {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);

      Cursor tCursor = CursorBuilder.createCursor(table);
      Cursor iCursor = CursorBuilder.createCursor(idx);

      Cursor.Savepoint tSave = tCursor.getSavepoint();
      Cursor.Savepoint iSave = iCursor.getSavepoint();

      tCursor.restoreSavepoint(tSave);
      iCursor.restoreSavepoint(iSave);

      try {
        tCursor.restoreSavepoint(iSave);
        Assert.fail("IllegalArgumentException should have been thrown");
      } catch(IllegalArgumentException e) {
        // success
      }

      try {
        iCursor.restoreSavepoint(tSave);
        Assert.fail("IllegalArgumentException should have been thrown");
      } catch(IllegalArgumentException e) {
        // success
      }

      Cursor tCursor2 = CursorBuilder.createCursor(table);
      Cursor iCursor2 = CursorBuilder.createCursor(idx);

      tCursor2.restoreSavepoint(tSave);
      iCursor2.restoreSavepoint(iSave);

      db.close();
    }
  }

  @Test
  public void testColumnMatcher() throws Exception {


    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createTestTable(fileFormat);

      Table table = db.getTable("test");

      doTestMatchers(table, SimpleColumnMatcher.INSTANCE, false);
      doTestMatchers(table, CaseInsensitiveColumnMatcher.INSTANCE, true);

      Cursor cursor = CursorBuilder.createCursor(table);
      doTestMatcher(table, cursor, SimpleColumnMatcher.INSTANCE, false);
      doTestMatcher(table, cursor, CaseInsensitiveColumnMatcher.INSTANCE,
                    true);
      db.close();
    }
  }

  private static void doTestMatchers(Table table, ColumnMatcher columnMatcher,
                                     boolean caseInsensitive)
    throws Exception
  {
      Assert.assertTrue(columnMatcher.matches(table, "value", null, null));
      Assert.assertFalse(columnMatcher.matches(table, "value", "foo", null));
      Assert.assertFalse(columnMatcher.matches(table, "value", null, "foo"));
      Assert.assertTrue(columnMatcher.matches(table, "value", "foo", "foo"));
      Assert.assertTrue(columnMatcher.matches(table, "value", "foo", "Foo")
                 == caseInsensitive);

      Assert.assertFalse(columnMatcher.matches(table, "value", 13, null));
      Assert.assertFalse(columnMatcher.matches(table, "value", null, 13));
      Assert.assertTrue(columnMatcher.matches(table, "value", 13, 13));
  }

  private static void doTestMatcher(Table table, Cursor cursor,
                                    ColumnMatcher columnMatcher,
                                    boolean caseInsensitive)
    throws Exception
  {
    cursor.setColumnMatcher(columnMatcher);

    Assert.assertTrue(cursor.findFirstRow(table.getColumn("id"), 3));
    Assert.assertEquals(createExpectedRow("id", 3,
                                   "value", "data" + 3),
                 cursor.getCurrentRow());

    Assert.assertTrue(cursor.findFirstRow(createExpectedRow(
                                    "id", 6,
                                    "value", "data" + 6)));
    Assert.assertEquals(createExpectedRow("id", 6,
                                   "value", "data" + 6),
                 cursor.getCurrentRow());

    Assert.assertTrue(cursor.findFirstRow(createExpectedRow(
                                    "id", 6,
                                    "value", "Data" + 6)) == caseInsensitive);
    if(caseInsensitive) {
      Assert.assertEquals(createExpectedRow("id", 6,
                                     "value", "data" + 6),
                   cursor.getCurrentRow());
    }

    Assert.assertFalse(cursor.findFirstRow(createExpectedRow(
                                   "id", 8,
                                   "value", "data" + 13)));
    Assert.assertFalse(cursor.findFirstRow(table.getColumn("id"), 13));
    Assert.assertEquals(createExpectedRow("id", 6,
                                   "value", "data" + 6),
                 cursor.getCurrentRow());

    Assert.assertTrue(cursor.findFirstRow(createExpectedRow(
                                    "value", "data" + 7)));
    Assert.assertEquals(createExpectedRow("id", 7,
                                   "value", "data" + 7),
                 cursor.getCurrentRow());

    Assert.assertTrue(cursor.findFirstRow(createExpectedRow(
                                    "value", "Data" + 7)) == caseInsensitive);
    if(caseInsensitive) {
      Assert.assertEquals(createExpectedRow("id", 7,
                                     "value", "data" + 7),
                   cursor.getCurrentRow());
    }

    Assert.assertTrue(cursor.findFirstRow(table.getColumn("value"), "data" + 4));
    Assert.assertEquals(createExpectedRow("id", 4,
                                   "value", "data" + 4),
                 cursor.getCurrentRow());

    Assert.assertTrue(cursor.findFirstRow(table.getColumn("value"), "Data" + 4)
               == caseInsensitive);
    if(caseInsensitive) {
      Assert.assertEquals(createExpectedRow("id", 4,
                                     "value", "data" + 4),
                   cursor.getCurrentRow());
    }

    Assert.assertEquals(Arrays.asList(createExpectedRow("id", 4,
                                                 "value", "data" + 4)),
                 RowFilterTest.toList(
                     cursor.newIterable()
                     .setMatchPattern("value", "data4")
                     .setColumnMatcher(SimpleColumnMatcher.INSTANCE)));

    Assert.assertEquals(Arrays.asList(createExpectedRow("id", 3,
                                                 "value", "data" + 3)),
                 RowFilterTest.toList(
                     cursor.newIterable()
                     .setMatchPattern("value", "DaTa3")
                     .setColumnMatcher(CaseInsensitiveColumnMatcher.INSTANCE)));

    Assert.assertEquals(Arrays.asList(createExpectedRow("id", 2,
                                                 "value", "data" + 2)),
                 RowFilterTest.toList(
                     cursor.newIterable()
                     .addMatchPattern("value", "DaTa2")
                     .addMatchPattern("id", 2)
                     .setColumnMatcher(CaseInsensitiveColumnMatcher.INSTANCE)));
  }

  @Test
  public void testIndexCursor() throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX, true)) {

      Database db = openMem(testDB);
      Table t1 = db.getTable("Table1");
      Index idx = t1.getIndex(IndexBuilder.PRIMARY_KEY_NAME);
      IndexCursor cursor = CursorBuilder.createCursor(idx);

      Assert.assertFalse(cursor.findFirstRowByEntry(-1));
      cursor.findClosestRowByEntry(-1);
      Assert.assertEquals(0, cursor.getCurrentRow().get("id"));

      Assert.assertTrue(cursor.findFirstRowByEntry(1));
      Assert.assertEquals(1, cursor.getCurrentRow().get("id"));

      cursor.findClosestRowByEntry(2);
      Assert.assertEquals(2, cursor.getCurrentRow().get("id"));

      Assert.assertFalse(cursor.findFirstRowByEntry(4));
      cursor.findClosestRowByEntry(4);
      Assert.assertTrue(cursor.isAfterLast());

      db.close();
    }
  }

  @Test
  public void testIndexCursorDelete() throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX)) {

      Database db = openMem(testDB);
      Table t1 = db.getTable("Table1");
      Index idx = t1.getIndex("Table2Table1");
      IndexCursor cursor = CursorBuilder.createCursor(idx);

      List<String> expectedData = new ArrayList<String>();
      for(Row row : cursor.newEntryIterable(1)
            .addColumnNames("data")) {
        expectedData.add(row.getString("data"));
      }

      Assert.assertEquals(Arrays.asList("baz11", "baz11-2"), expectedData);

      expectedData = new ArrayList<String>();
      for(Iterator<? extends Row> iter =
            cursor.newEntryIterable(1).iterator();
          iter.hasNext(); ) {
        expectedData.add(iter.next().getString("data"));
        iter.remove();
        try {
          iter.remove();
          Assert.fail("IllegalArgumentException should have been thrown");
        } catch(IllegalStateException e) {
          // success
        }

        if(!iter.hasNext()) {
          try {
            iter.next();
            Assert.fail("NoSuchElementException should have been thrown");
          } catch(NoSuchElementException e) {
            // success
          }
        }
      }

      Assert.assertEquals(Arrays.asList("baz11", "baz11-2"), expectedData);

      expectedData = new ArrayList<String>();
      for(Row row : cursor.newEntryIterable(1)
            .addColumnNames("data")) {
        expectedData.add(row.getString("data"));
      }

      Assert.assertTrue(expectedData.isEmpty());

      db.close();
    }
  }

  @Test
  public void testCursorDelete() throws Exception
  {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX)) {

      Database db = openMem(testDB);
      Table t1 = db.getTable("Table1");
      Cursor cursor = CursorBuilder.createCursor(t1);

      List<String> expectedData = new ArrayList<String>();
      for(Row row : cursor.newIterable().setColumnNames(
              Arrays.asList("otherfk1", "data"))) {
        if(row.get("otherfk1").equals(1)) {
          expectedData.add(row.getString("data"));
        }
      }

      Assert.assertEquals(Arrays.asList("baz11", "baz11-2"), expectedData);

      expectedData = new ArrayList<String>();
      for(Iterator<? extends Row> iter = cursor.iterator();
          iter.hasNext(); ) {
        Row row = iter.next();
        if(row.get("otherfk1").equals(1)) {
          expectedData.add(row.getString("data"));
          iter.remove();
          try {
            iter.remove();
            Assert.fail("IllegalArgumentException should have been thrown");
          } catch(IllegalStateException e) {
            // success
          }
        }

        if(!iter.hasNext()) {
          try {
            iter.next();
            Assert.fail("NoSuchElementException should have been thrown");
          } catch(NoSuchElementException e) {
            // success
          }
        }
      }

      Assert.assertEquals(Arrays.asList("baz11", "baz11-2"), expectedData);

      expectedData = new ArrayList<String>();
      for(Row row : cursor.newIterable().setColumnNames(
              Arrays.asList("otherfk1", "data"))) {
        if(row.get("otherfk1").equals(1)) {
          expectedData.add(row.getString("data"));
        }
      }

      Assert.assertTrue(expectedData.isEmpty());

      db.close();
    }
  }

  @Test
  public void testFindByRowId() throws Exception {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {
      Database db = createTestTable(fileFormat);

      Table table = db.getTable("test");
      Cursor cursor = CursorBuilder.createCursor(table);
      doTestFindByRowId(cursor);
      db.close();
    }
  }

  @Test
  public void testFindByRowIdIndex() throws Exception {
    for (final TestDB indexCursorDB : INDEX_CURSOR_DBS) {
      Database db = createTestIndexTable(indexCursorDB);

      Table table = db.getTable("test");
      Index idx = table.getIndexes().get(0);

      assertTable(createUnorderedTestTableData(), table);

      Cursor cursor = CursorBuilder.createCursor(idx);
      doTestFindByRowId(cursor);

      db.close();
    }
  }

  private static void doTestFindByRowId(Cursor cursor)
    throws Exception
  {
    for(int i = 0; i < 3; ++i) {
      cursor.moveToNextRow();
    }

    Row r1 = cursor.getCurrentRow();

    for(int i = 0; i < 3; ++i) {
      cursor.moveToNextRow();
    }

    Row r2 = cursor.getCurrentRow();

    doTestFindByRowId(cursor, r1, 2);

    doTestFindByRowId(cursor, r2, 5);
  }

  private static void doTestFindByRowId(Cursor cursor, Row row, int id)
    throws Exception
  {
    cursor.reset();
    Assert.assertTrue(cursor.findRow(row.getId()));
    Row rFound = cursor.getCurrentRow();
    Assert.assertEquals(id, rFound.get("id"));
    Assert.assertEquals(row, rFound);
    Cursor.Savepoint save = cursor.getSavepoint();

    Assert.assertTrue(cursor.moveToNextRow());
    Assert.assertEquals(id + 1, cursor.getCurrentRow().get("id"));

    cursor.restoreSavepoint(save);

    Assert.assertTrue(cursor.moveToPreviousRow());
    Assert.assertEquals(id - 1, cursor.getCurrentRow().get("id"));

    Assert.assertFalse(cursor.findRow(RowIdImpl.FIRST_ROW_ID));

    Assert.assertEquals(id - 1, cursor.getCurrentRow().get("id"));
  }

  @Test
  public void testIterationEarlyExit() throws Exception {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {

      Database db = createMem(fileFormat);

      Table table = new TableBuilder("test")
        .addColumn(new ColumnBuilder("id", DataType.LONG))
        .addColumn(new ColumnBuilder("value", DataType.TEXT))
        .addColumn(new ColumnBuilder("memo", DataType.MEMO))
        .addIndex(new IndexBuilder("value_idx")
                  .addColumns("value"))
        .toTable(db);

      for(int i = 0; i < 20; ++i) {
        Object memo = "memo-" + i;
        table.addRow(i, "val-" + (i/2), memo);
      }

      // generate an "invalid" memo
      byte[] b = new byte[12];
      b[3] = (byte)0xC0;
      table.addRow(20, "val-9", ColumnImpl.rawDataWrapper(b));

      IndexCursor cursor = CursorBuilder.createCursor(
          table.getIndex("value_idx"));

      try {
        cursor.newIterable()
          .addMatchPattern("value", "val-9")
          .addMatchPattern("memo", "anything")
          .iterator().hasNext();
        Assert.fail("RuntimeIOException should have been thrown");
      } catch(RuntimeIOException ignored) {
        // success
      }

      List<Row> rows = new ArrayList<Row>();
      for (Row row : cursor.newIterable()
             .addMatchPattern("value", "val-5")
             .addMatchPattern("memo", "memo-11")) {
        rows.add(row);
      }

      Assert.assertEquals(rows, createExpectedTable(
                       createExpectedRow("id", 11,
                                         "value", "val-5",
                                         "memo", "memo-11")));

      Assert.assertFalse(cursor.newIterable()
                  .addMatchPattern("value", "val-31")
                  .addMatchPattern("memo", "anything")
                  .iterator().hasNext());

      db.close();
    }
  }

  @Test
  public void testPartialIndexFind() throws Exception
  {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {

      Database db = createMem(fileFormat);

      TableImpl t = (TableImpl)new TableBuilder("Test")
        .addColumn(new ColumnBuilder("id", DataType.LONG))
        .addColumn(new ColumnBuilder("data1", DataType.TEXT))
        .addColumn(new ColumnBuilder("num2", DataType.LONG))
        .addColumn(new ColumnBuilder("key3", DataType.TEXT))
        .addColumn(new ColumnBuilder("value", DataType.TEXT))
        .addIndex(new IndexBuilder("idx3").addColumns("data1", "num2", "key3"))
        .toTable(db);

      Index idx = t.findIndexForColumns(Arrays.asList("data1"),
                                        TableImpl.IndexFeature.ANY_MATCH);
      Assert.assertEquals("idx3", idx.getName());

      idx = t.findIndexForColumns(Arrays.asList("data1", "num2"),
                                  TableImpl.IndexFeature.ANY_MATCH);
      Assert.assertEquals("idx3", idx.getName());

      idx = t.findIndexForColumns(Arrays.asList("data1", "num2", "key3"),
                                  TableImpl.IndexFeature.ANY_MATCH);
      Assert.assertEquals("idx3", idx.getName());

      Assert.assertNull(t.findIndexForColumns(Arrays.asList("num2"),
                                       TableImpl.IndexFeature.ANY_MATCH));
      Assert.assertNull(t.findIndexForColumns(Arrays.asList("data1", "key3"),
                                       TableImpl.IndexFeature.ANY_MATCH));
      Assert.assertNull(t.findIndexForColumns(Arrays.asList("data1"),
                                       TableImpl.IndexFeature.EXACT_MATCH));


      new IndexBuilder("idx2")
        .addColumns("data1", "num2")
        .addToTable(t);

      idx = t.findIndexForColumns(Arrays.asList("data1"),
                                  TableImpl.IndexFeature.ANY_MATCH);
      Assert.assertEquals("idx2", idx.getName());

      idx = t.findIndexForColumns(Arrays.asList("data1", "num2"),
                                  TableImpl.IndexFeature.ANY_MATCH);
      Assert.assertEquals("idx2", idx.getName());

      idx = t.findIndexForColumns(Arrays.asList("data1", "num2", "key3"),
                                  TableImpl.IndexFeature.ANY_MATCH);
      Assert.assertEquals("idx3", idx.getName());

      Assert.assertNull(t.findIndexForColumns(Arrays.asList("num2"),
                                       TableImpl.IndexFeature.ANY_MATCH));
      Assert.assertNull(t.findIndexForColumns(Arrays.asList("data1", "key3"),
                                       TableImpl.IndexFeature.ANY_MATCH));
      Assert.assertNull(t.findIndexForColumns(Arrays.asList("data1"),
                                       TableImpl.IndexFeature.EXACT_MATCH));


      new IndexBuilder("idx1")
        .addColumns("data1")
        .addToTable(t);

      idx = t.findIndexForColumns(Arrays.asList("data1"),
                                  TableImpl.IndexFeature.ANY_MATCH);
      Assert.assertEquals("idx1", idx.getName());

      idx = t.findIndexForColumns(Arrays.asList("data1", "num2"),
                                  TableImpl.IndexFeature.ANY_MATCH);
      Assert.assertEquals("idx2", idx.getName());

      idx = t.findIndexForColumns(Arrays.asList("data1", "num2", "key3"),
                                  TableImpl.IndexFeature.ANY_MATCH);
      Assert.assertEquals("idx3", idx.getName());

      Assert.assertNull(t.findIndexForColumns(Arrays.asList("num2"),
                                       TableImpl.IndexFeature.ANY_MATCH));
      Assert.assertNull(t.findIndexForColumns(Arrays.asList("data1", "key3"),
                                       TableImpl.IndexFeature.ANY_MATCH));

      db.close();
    }
  }

  @Test
  public void testPartialIndexLookup() throws Exception
  {
    for (final FileFormat fileFormat : JetFormatTest.SUPPORTED_FILEFORMATS) {

      Database db = createMem(fileFormat);

      TableImpl t = (TableImpl)new TableBuilder("Test")
        .addColumn(new ColumnBuilder("id", DataType.LONG))
        .addColumn(new ColumnBuilder("data1", DataType.TEXT))
        .addColumn(new ColumnBuilder("num2", DataType.LONG))
        .addColumn(new ColumnBuilder("key3", DataType.TEXT))
        .addColumn(new ColumnBuilder("value", DataType.TEXT))
        .addIndex(new IndexBuilder("idx3")
                  .addColumns(true, "data1")
                  .addColumns(false, "num2")
                  .addColumns(true, "key3")
                  )
        .toTable(db);

      int id = 1;
      for(String str : Arrays.asList("A", "B", "C", "D")) {
        for(int i = 4; i >= 0; --i) {
        // for(int i = 0; i < 5; ++i) {
          for(int j = 1; j < 3; ++j) {
            t.addRow(id, str, i, "K" + j, "value" + id);
            ++id;
          }
        }
      }

      Index idx = t.getIndex("idx3");
      doPartialIndexLookup(idx);

      idx = new IndexBuilder("idx2")
                  .addColumns(true, "data1")
                  .addColumns(false, "num2")
        .addToTable(t);
      doPartialIndexLookup(idx);

      idx = new IndexBuilder("idx1")
                  .addColumns(true, "data1")
        .addToTable(t);
      doPartialIndexLookup(idx);

      db.close();
    }
  }

  private static void doPartialIndexLookup(Index idx) throws Exception
  {
    int colCount = idx.getColumnCount();
    IndexCursor c = new CursorBuilder(idx.getTable()).setIndex(idx).toIndexCursor();

    doFindFirstByEntry(c, 21, "C");
    doFindFirstByEntry(c, null, "Z");

    if(colCount > 1) {
      doFindFirstByEntry(c, 23, "C", 3);
      doFindFirstByEntry(c, null, "C", 20);
    }

    if(colCount > 2) {
      doFindFirstByEntry(c, 27, "C", 1, "K1");
      doFindFirstByEntry(c, null, "C", 4, "K3");
    }

    try {
      if(colCount > 2) {
        c.findFirstRowByEntry("C", 4, "K1", 14);
      } else if(colCount > 1) {
        c.findFirstRowByEntry("C", 4, "K1");
      } else {
        c.findFirstRowByEntry("C", 4);
      }
      Assert.fail("IllegalArgumentException should have been thrown");
    } catch(IllegalArgumentException expected) {
      // scucess
    }

    doFindByEntryRange(c, 11, 20, "B");
    doFindByEntry(c, new int[]{}, "Z");

    if(colCount > 1) {
      doFindByEntryRange(c, 13, 14, "B", 3);
      doFindByEntry(c, new int[]{}, "B", 20);
    }

    if(colCount > 2) {
      doFindByEntryRange(c, 14, 14, "B", 3, "K2");
      doFindByEntry(c, new int[]{}, "B", 3, "K3");
    }

    doFindByRow(idx, 13,
                "data1", "B", "value", "value13");
    doFindByRow(idx, 13,
                "data1", "B", "key3", "K1", "value", "value13");
    doFindByRow(idx, 13,
        "data1", "B", "num2", 3, "key3", "K1", "value", "value13");
    doFindByRow(idx, 13,
        "num2", 3, "value", "value13");
    doFindByRow(idx, 13,
        "value", "value13");
    doFindByRow(idx, null,
        "data1", "B", "num2", 5, "key3", "K1", "value", "value13");
    doFindByRow(idx, null,
        "data1", "B", "value", "value4");

    Column col = idx.getTable().getColumn("data1");
    doFindValue(idx, 21, col, "C");
    doFindValue(idx, null, col, "Z");
    col = idx.getTable().getColumn("value");
    doFindValue(idx, 21, col, "value21");
    doFindValue(idx, null, col, "valueZ");
  }

  private static void doFindFirstByEntry(IndexCursor c, Integer expectedId,
                                         Object... entry)
    throws Exception
  {
    if(expectedId != null) {
      Assert.assertTrue(c.findFirstRowByEntry(entry));
      Assert.assertEquals(expectedId, c.getCurrentRow().get("id"));
    } else {
      Assert.assertFalse(c.findFirstRowByEntry(entry));
    }
  }

  private static void doFindByEntryRange(IndexCursor c, int start, int end,
                                         Object... entry)
  {
    List<Integer> expectedIds = new ArrayList<Integer>();
    for(int i = start; i <= end; ++i) {
      expectedIds.add(i);
    }
    doFindByEntry(c, expectedIds, entry);
  }

  private static void doFindByEntry(IndexCursor c, int[] ids,
                                    Object... entry)
  {
    List<Integer> expectedIds = new ArrayList<Integer>();
    for(int id : ids) {
      expectedIds.add(id);
    }
    doFindByEntry(c, expectedIds, entry);
  }

  private static void doFindByEntry(IndexCursor c, List<Integer> expectedIds,
                                    Object... entry)
  {
    List<Integer> foundIds = new ArrayList<Integer>();
    for(Row row : c.newEntryIterable(entry)) {
      foundIds.add((Integer)row.get("id"));
    }
    Assert.assertEquals(expectedIds, foundIds);
  }

  private static void doFindByRow(Index idx, Integer id, Object... rowPairs)
    throws Exception
  {
    Map<String,Object> map = createExpectedRow(
        rowPairs);
    Row r = CursorBuilder.findRow(idx, map);
    if(id != null) {
      Assert.assertEquals(id, r.get("id"));
    } else {
      Assert.assertNull(r);
    }
  }

  private static void doFindValue(Index idx, Integer id,
                                  Column columnPattern, Object valuePattern)
    throws Exception
  {
    Object value = CursorBuilder.findValue(
        idx, idx.getTable().getColumn("id"), columnPattern, valuePattern);
    if(id != null) {
      Assert.assertEquals(id, value);
    } else {
      Assert.assertNull(value);
    }
  }
}
