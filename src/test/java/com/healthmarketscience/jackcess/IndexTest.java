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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.healthmarketscience.jackcess.Database.*;
import com.healthmarketscience.jackcess.impl.ByteUtil;
import com.healthmarketscience.jackcess.impl.IndexCodesTest;
import com.healthmarketscience.jackcess.impl.IndexData;
import com.healthmarketscience.jackcess.impl.IndexImpl;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import com.healthmarketscience.jackcess.impl.RowIdImpl;
import com.healthmarketscience.jackcess.impl.TableImpl;
import static com.healthmarketscience.jackcess.TestUtil.*;

/**
 * @author James Ahlborn
 */
public class IndexTest {

  @Before
  public void setUp() {
    TestUtil.setTestAutoSync(false);
  }

  @After
  public void tearDown() {
    TestUtil.clearTestAutoSync();
  }

  @Test
  public void testByteOrder() throws Exception {
    final byte b1 = (byte) 0x00;
    final byte b2 = (byte) 0x01;
    final byte b3 = (byte) 0x7F;
    final byte b4 = (byte) 0x80;
    final byte b5 = (byte) 0xFF;

    Assert.assertTrue(ByteUtil.asUnsignedByte(b1) < ByteUtil.asUnsignedByte(b2));
    Assert.assertTrue(ByteUtil.asUnsignedByte(b2) < ByteUtil.asUnsignedByte(b3));
    Assert.assertTrue(ByteUtil.asUnsignedByte(b3) < ByteUtil.asUnsignedByte(b4));
    Assert.assertTrue(ByteUtil.asUnsignedByte(b4) < ByteUtil.asUnsignedByte(b5));
  }

  @Test
  public void testByteCodeComparator() {
    final byte[] b0 = null;
    final byte[] b1 = new byte[] { (byte) 0x00 };
    final byte[] b2 = new byte[] { (byte) 0x00, (byte) 0x00 };
    final byte[] b3 = new byte[] { (byte) 0x00, (byte) 0x01 };
    final byte[] b4 = new byte[] { (byte) 0x01 };
    final byte[] b5 = new byte[] { (byte) 0x80 };
    final byte[] b6 = new byte[] { (byte) 0xFF };
    final byte[] b7 = new byte[] { (byte) 0xFF, (byte) 0x00 };
    final byte[] b8 = new byte[] { (byte) 0xFF, (byte) 0x01 };

    final List<byte[]> expectedList = Arrays.<byte[]>asList(b0, b1, b2, b3, b4, b5, b6, b7, b8);
    final SortedSet<byte[]> sortedSet = new TreeSet<byte[]>(IndexData.BYTE_CODE_COMPARATOR);
    sortedSet.addAll(expectedList);
    Assert.assertEquals(expectedList, new ArrayList<byte[]>(sortedSet));

  }

  @Test
  public void testPrimaryKey() throws Exception {
    for (final TestDB testDB : SUPPORTED_DBS_TEST_FOR_READ) {
      final Table table = open(testDB).getTable("Table1");
      final Map<String, Boolean> foundPKs = new HashMap<String, Boolean>();
      Index pkIndex = null;
      for (final Index index : table.getIndexes()) {
        foundPKs.put(index.getColumns().iterator().next().getName(), index.isPrimaryKey());
        if (index.isPrimaryKey()) {
          pkIndex = index;

        }
      }
      final Map<String, Boolean> expectedPKs = new HashMap<String, Boolean>();
      expectedPKs.put("A", Boolean.TRUE);
      expectedPKs.put("B", Boolean.FALSE);
      Assert.assertEquals(expectedPKs, foundPKs);
      Assert.assertSame(pkIndex, table.getPrimaryKeyIndex());
    }
  }

  @Test
  public void testLogicalIndexes() throws Exception {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX, true)) {
      final Database mdb = open(testDB);

      TableImpl table = (TableImpl) mdb.getTable("Table1");
      for (final IndexImpl idx : table.getIndexes()) {
        idx.initialize();
      }
      Assert.assertEquals(4, table.getIndexes().size());
      Assert.assertEquals(4, table.getLogicalIndexCount());
      checkIndexColumns(table, "id", "id", "PrimaryKey", "id", "Table2Table1", "otherfk1", "Table3Table1", "otherfk2");

      table = (TableImpl) mdb.getTable("Table2");
      for (final IndexImpl idx : table.getIndexes()) {
        idx.initialize();
      }
      Assert.assertEquals(3, table.getIndexes().size());
      Assert.assertEquals(2, table.getIndexDatas().size());
      Assert.assertEquals(3, table.getLogicalIndexCount());
      checkIndexColumns(table, "id", "id", "PrimaryKey", "id", ".rC", "id");

      IndexImpl pkIdx = table.getIndex("PrimaryKey");
      IndexImpl fkIdx = table.getIndex(".rC");
      Assert.assertNotSame(pkIdx, fkIdx);
      Assert.assertTrue(fkIdx.isForeignKey());
      Assert.assertSame(pkIdx.getIndexData(), fkIdx.getIndexData());
      IndexData indexData = pkIdx.getIndexData();
      Assert.assertEquals(Arrays.asList(pkIdx, fkIdx), indexData.getIndexes());
      Assert.assertSame(pkIdx, indexData.getPrimaryIndex());

      table = (TableImpl) mdb.getTable("Table3");
      for (final IndexImpl idx : table.getIndexes()) {
        idx.initialize();
      }
      Assert.assertEquals(3, table.getIndexes().size());
      Assert.assertEquals(2, table.getIndexDatas().size());
      Assert.assertEquals(3, table.getLogicalIndexCount());
      checkIndexColumns(table, "id", "id", "PrimaryKey", "id", ".rC", "id");

      pkIdx = table.getIndex("PrimaryKey");
      fkIdx = table.getIndex(".rC");
      Assert.assertNotSame(pkIdx, fkIdx);
      Assert.assertTrue(fkIdx.isForeignKey());
      Assert.assertSame(pkIdx.getIndexData(), fkIdx.getIndexData());
      indexData = pkIdx.getIndexData();
      Assert.assertEquals(Arrays.asList(pkIdx, fkIdx), indexData.getIndexes());
      Assert.assertSame(pkIdx, indexData.getPrimaryIndex());
    }
  }

  @Test
  public void testComplexIndex() throws Exception {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.COMP_INDEX)) {
      // this file has an index with "compressed" entries and node pages
      Database db = open(testDB);
      TableImpl t = (TableImpl) db.getTable("Table1");
      IndexImpl index = t.getIndexes().get(0);
      Assert.assertFalse(index.isInitialized());
      Assert.assertEquals(512, countRows(t));
      Assert.assertEquals(512, index.getIndexData().getEntryCount());
      db.close();

      // copy to temp file and attempt to edit
      db = openCopy(testDB);
      t = (TableImpl) db.getTable("Table1");
      index = t.getIndexes().get(0);

      t.addRow(99, "abc", "def");
    }
  }

  @Test
  public void testEntryDeletion() throws Exception {
    for (final TestDB testDB : SUPPORTED_DBS_TEST) {
      final Table table = openCopy(testDB).getTable("Table1");

      for (int i = 0; i < 10; ++i) {
        table.addRow("foo" + i, "bar" + i, (byte) 42 + i, (short) 53 + i, 13 * i, (6.7d / i), null, null, true);
      }
      table.reset();
      assertRowCount(12, table);

      for (final Index index : table.getIndexes()) {
        Assert.assertEquals(12, ((IndexImpl) index).getIndexData().getEntryCount());
      }

      table.reset();
      table.getNextRow();
      table.getNextRow();
      table.getDefaultCursor().deleteCurrentRow();
      table.getNextRow();
      table.getDefaultCursor().deleteCurrentRow();
      table.getNextRow();
      table.getNextRow();
      table.getDefaultCursor().deleteCurrentRow();
      table.getNextRow();
      table.getNextRow();
      table.getNextRow();
      table.getDefaultCursor().deleteCurrentRow();

      table.reset();
      assertRowCount(8, table);

      for (final Index index : table.getIndexes()) {
        Assert.assertEquals(8, ((IndexImpl) index).getIndexData().getEntryCount());
      }
    }
  }

  @Test
  public void testIgnoreNulls() throws Exception {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX_PROPERTIES)) {
      final Database db = openCopy(testDB);

      doTestIgnoreNulls(db, "TableIgnoreNulls1");
      doTestIgnoreNulls(db, "TableIgnoreNulls2");

      db.close();
    }
  }

  private void doTestIgnoreNulls(final Database db, final String tableName) throws Exception {
    final Table orig = db.getTable(tableName);
    final IndexImpl origI = (IndexImpl) orig.getIndex("DataIndex");
    final Table temp = db.getTable(tableName + "_temp");
    final IndexImpl tempI = (IndexImpl) temp.getIndex("DataIndex");

    // copy from orig table to temp table
    for (final Map<String, Object> row : orig) {
      temp.addRow(orig.asRow(row));
    }

    Assert.assertEquals(origI.getIndexData().getEntryCount(), tempI.getIndexData().getEntryCount());

    final Cursor origC = origI.newCursor().toCursor();
    final Cursor tempC = tempI.newCursor().toCursor();

    while (true) {
      final boolean origHasNext = origC.moveToNextRow();
      final boolean tempHasNext = tempC.moveToNextRow();
      Assert.assertTrue(origHasNext == tempHasNext);
      if (!origHasNext) {
        break;
      }

      final Map<String, Object> origRow = origC.getCurrentRow();
      final Cursor.Position origCurPos = origC.getSavepoint().getCurrentPosition();
      final Map<String, Object> tempRow = tempC.getCurrentRow();
      final Cursor.Position tempCurPos = tempC.getSavepoint().getCurrentPosition();

      Assert.assertEquals(origRow, tempRow);
      Assert.assertEquals(IndexCodesTest.entryToString(origCurPos), IndexCodesTest.entryToString(tempCurPos));
    }
  }

  @Test
  public void testUnique() throws Exception {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX_PROPERTIES)) {
      final Database db = openCopy(testDB);

      Table t = db.getTable("TableUnique1_temp");
      Index index = t.getIndex("DataIndex");

      doTestUnique(index, 1, null, true, "unique data", true, null, true, "more", false, "stuff", false, "unique data",
          false);

      t = db.getTable("TableUnique2_temp");
      index = t.getIndex("DataIndex");

      doTestUnique(index, 2, null, null, true, "unique data", 42, true, "unique data", null, true, null, null, true,
          "some", 42, true, "more unique data", 13, true, null, -4242, true, "another row", -3462, false, null, 49,
          false, "more", null, false, "unique data", 42, false, "unique data", null, false, null, -4242, false);

      db.close();
    }
  }

  private void doTestUnique(final Index index, final int numValues, final Object... testData) throws Exception {
    for (int i = 0; i < testData.length; i += (numValues + 1)) {
      final Object[] row = new Object[numValues + 1];
      row[0] = "testRow" + i;
      for (int j = 1; j < (numValues + 1); ++j) {
        row[j] = testData[i + j - 1];
      }
      final boolean expectedSuccess = (Boolean) testData[i + numValues];

      IOException failure = null;
      try {
        ((IndexImpl) index).getIndexData().prepareAddRow(row, new RowIdImpl(400 + i, 0), null).commit();
      } catch (final IOException e) {
        failure = e;
      }
      if (expectedSuccess) {
        Assert.assertNull(failure);
      } else {
        Assert.assertTrue(failure != null);
        Assert.assertTrue(failure.getMessage().contains("uniqueness"));
      }
    }
  }

  @Test
  public void testUniqueEntryCount() throws Exception {
    for (final TestDB testDB : SUPPORTED_DBS_TEST) {
      final Database db = openCopy(testDB);
      Table table = db.getTable("Table1");
      IndexImpl indA = (IndexImpl) table.getIndex("PrimaryKey");
      IndexImpl indB = (IndexImpl) table.getIndex("B");

      Assert.assertEquals(2, indA.getUniqueEntryCount());
      Assert.assertEquals(2, indB.getUniqueEntryCount());

      final List<String> bElems = Arrays.asList("bar", null, "baz", "argle", null, "bazzle", "37", "bar", "bar", "BAZ");

      for (int i = 0; i < 10; ++i) {
        table.addRow("foo" + i, bElems.get(i), (byte) 42 + i, (short) 53 + i, 13 * i, (6.7d / i), null, null, true);
      }

      Assert.assertEquals(12, indA.getIndexData().getEntryCount());
      Assert.assertEquals(12, indB.getIndexData().getEntryCount());

      Assert.assertEquals(12, indA.getUniqueEntryCount());
      Assert.assertEquals(8, indB.getUniqueEntryCount());

      table = null;
      indA = null;
      indB = null;

      table = db.getTable("Table1");
      indA = (IndexImpl) table.getIndex("PrimaryKey");
      indB = (IndexImpl) table.getIndex("B");

      Assert.assertEquals(12, indA.getIndexData().getEntryCount());
      Assert.assertEquals(12, indB.getIndexData().getEntryCount());

      Assert.assertEquals(12, indA.getUniqueEntryCount());
      Assert.assertEquals(8, indB.getUniqueEntryCount());

      final Cursor c = CursorBuilder.createCursor(table);
      Assert.assertTrue(c.moveToNextRow());

      final Row row = c.getCurrentRow();
      // Row order is arbitrary, so v2007 row order difference is valid
      if (testDB.getExpectedFileFormat().ordinal() >= Database.FileFormat.V2007.ordinal()) {
        TestUtil.checkTestDBTable1RowA(testDB, table, row);
      } else {
        TestUtil.checkTestDBTable1RowABCDEFG(testDB, table, row);
      }
      c.deleteCurrentRow();

      Assert.assertEquals(11, indA.getIndexData().getEntryCount());
      Assert.assertEquals(11, indB.getIndexData().getEntryCount());

      Assert.assertEquals(12, indA.getUniqueEntryCount());
      Assert.assertEquals(8, indB.getUniqueEntryCount());

      db.close();
    }
  }

  @Test
  public void testReplId() throws Exception {
    for (final TestDB testDB : SUPPORTED_DBS_TEST) {
      final Database db = openCopy(testDB);
      final Table table = db.getTable("Table4");

      for (int i = 0; i < 20; ++i) {
        table.addRow("row" + i, Column.AUTO_NUMBER);
      }

      Assert.assertEquals(20, table.getRowCount());

      db.close();
    }
  }

  @Test
  public void testIndexCreation() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      final Database db = create(fileFormat);

      final Table t = new TableBuilder("TestTable").addColumn(new ColumnBuilder("id", DataType.LONG))
          .addColumn(new ColumnBuilder("data", DataType.TEXT))
          .addIndex(new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME).addColumns("id").setPrimaryKey()).toTable(db);

      Assert.assertEquals(1, t.getIndexes().size());
      final IndexImpl idx = (IndexImpl) t.getIndexes().get(0);

      Assert.assertEquals(IndexBuilder.PRIMARY_KEY_NAME, idx.getName());
      Assert.assertEquals(1, idx.getColumns().size());
      Assert.assertEquals("id", idx.getColumns().get(0).getName());
      Assert.assertTrue(idx.getColumns().get(0).isAscending());
      Assert.assertTrue(idx.isPrimaryKey());
      Assert.assertTrue(idx.isUnique());
      Assert.assertFalse(idx.shouldIgnoreNulls());
      Assert.assertNull(idx.getReference());

      t.addRow(2, "row2");
      t.addRow(1, "row1");
      t.addRow(3, "row3");

      final Cursor c = t.newCursor().setIndexByName(IndexBuilder.PRIMARY_KEY_NAME).toCursor();

      for (int i = 1; i <= 3; ++i) {
        final Map<String, Object> row = c.getNextRow();
        Assert.assertEquals(i, row.get("id"));
        Assert.assertEquals("row" + i, row.get("data"));
      }
      Assert.assertFalse(c.moveToNextRow());
    }
  }

  @Test
  public void testIndexCreationSharedData() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      final Database db = create(fileFormat);

      final Table t = new TableBuilder("TestTable").addColumn(new ColumnBuilder("id", DataType.LONG))
          .addColumn(new ColumnBuilder("data", DataType.TEXT))
          .addIndex(new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME).addColumns("id").setPrimaryKey())
          .addIndex(new IndexBuilder("Index1").addColumns("id")).addIndex(new IndexBuilder("Index2").addColumns("id"))
          .addIndex(new IndexBuilder("Index3").addColumns(false, "id")).toTable(db);

      Assert.assertEquals(4, t.getIndexes().size());
      final IndexImpl idx = (IndexImpl) t.getIndexes().get(0);

      Assert.assertEquals(IndexBuilder.PRIMARY_KEY_NAME, idx.getName());
      Assert.assertEquals(1, idx.getColumns().size());
      Assert.assertEquals("id", idx.getColumns().get(0).getName());
      Assert.assertTrue(idx.getColumns().get(0).isAscending());
      Assert.assertTrue(idx.isPrimaryKey());
      Assert.assertTrue(idx.isUnique());
      Assert.assertFalse(idx.shouldIgnoreNulls());
      Assert.assertNull(idx.getReference());

      final IndexImpl idx1 = (IndexImpl) t.getIndexes().get(1);
      final IndexImpl idx2 = (IndexImpl) t.getIndexes().get(2);
      final IndexImpl idx3 = (IndexImpl) t.getIndexes().get(3);

      Assert.assertNotSame(idx.getIndexData(), idx1.getIndexData());
      Assert.assertSame(idx1.getIndexData(), idx2.getIndexData());
      Assert.assertNotSame(idx2.getIndexData(), idx3.getIndexData());

      t.addRow(2, "row2");
      t.addRow(1, "row1");
      t.addRow(3, "row3");

      final Cursor c = t.newCursor().setIndexByName(IndexBuilder.PRIMARY_KEY_NAME).toCursor();

      for (int i = 1; i <= 3; ++i) {
        final Map<String, Object> row = c.getNextRow();
        Assert.assertEquals(i, row.get("id"));
        Assert.assertEquals("row" + i, row.get("data"));
      }
      Assert.assertFalse(c.moveToNextRow());
    }
  }

  @Test
  public void testGetForeignKeyIndex() throws Exception {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX, true)) {
      final Database db = open(testDB);
      final Table t1 = db.getTable("Table1");
      final Table t2 = db.getTable("Table2");
      final Table t3 = db.getTable("Table3");

      final IndexImpl t2t1 = (IndexImpl) t1.getIndex("Table2Table1");
      final IndexImpl t3t1 = (IndexImpl) t1.getIndex("Table3Table1");

      Assert.assertTrue(t2t1.isForeignKey());
      Assert.assertNotNull(t2t1.getReference());
      Assert.assertFalse(t2t1.getReference().isPrimaryTable());
      Assert.assertFalse(t2t1.getReference().isCascadeUpdates());
      Assert.assertTrue(t2t1.getReference().isCascadeDeletes());
      doCheckForeignKeyIndex(t1, t2t1, t2);

      Assert.assertTrue(t3t1.isForeignKey());
      Assert.assertNotNull(t3t1.getReference());
      Assert.assertFalse(t3t1.getReference().isPrimaryTable());
      Assert.assertTrue(t3t1.getReference().isCascadeUpdates());
      Assert.assertFalse(t3t1.getReference().isCascadeDeletes());
      doCheckForeignKeyIndex(t1, t3t1, t3);

      final Index t1pk = t1.getIndex(IndexBuilder.PRIMARY_KEY_NAME);
      Assert.assertNotNull(t1pk);
      Assert.assertNull(((IndexImpl) t1pk).getReference());
      Assert.assertNull(t1pk.getReferencedIndex());
    }
  }

  @Test
  public void testConstraintViolation() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      final Database db = create(fileFormat);

      final Table t = new TableBuilder("TestTable").addColumn(new ColumnBuilder("id", DataType.LONG))
          .addColumn(new ColumnBuilder("data", DataType.TEXT))
          .addIndex(new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME).addColumns("id").setPrimaryKey())
          .addIndex(new IndexBuilder("data_ind").addColumns("data").setUnique()).toTable(db);

      for (int i = 0; i < 5; ++i) {
        t.addRow(i, "row" + i);
      }

      try {
        t.addRow(3, "badrow");
        Assert.fail("ConstraintViolationException should have been thrown");
      } catch (final ConstraintViolationException ce) {
        // success
      }

      Assert.assertEquals(5, t.getRowCount());

      List<Row> expectedRows = createExpectedTable(createExpectedRow("id", 0, "data", "row0"),
          createExpectedRow("id", 1, "data", "row1"), createExpectedRow("id", 2, "data", "row2"),
          createExpectedRow("id", 3, "data", "row3"), createExpectedRow("id", 4, "data", "row4"));

      assertTable(expectedRows, t);

      final IndexCursor pkCursor = CursorBuilder.createPrimaryKeyCursor(t);
      assertCursor(expectedRows, pkCursor);

      assertCursor(expectedRows, CursorBuilder.createCursor(t.getIndex("data_ind")));

      final List<Object[]> batch = new ArrayList<Object[]>();
      batch.add(new Object[] { 5, "row5" });
      batch.add(new Object[] { 6, "row6" });
      batch.add(new Object[] { 7, "row2" });
      batch.add(new Object[] { 8, "row8" });

      try {
        t.addRows(batch);
        Assert.fail("BatchUpdateException should have been thrown");
      } catch (final BatchUpdateException be) {
        // success
        Assert.assertTrue(be.getCause() instanceof ConstraintViolationException);
        Assert.assertEquals(2, be.getUpdateCount());
      }

      expectedRows = new ArrayList<Row>(expectedRows);
      expectedRows.add(createExpectedRow("id", 5, "data", "row5"));
      expectedRows.add(createExpectedRow("id", 6, "data", "row6"));

      assertTable(expectedRows, t);

      assertCursor(expectedRows, pkCursor);

      assertCursor(expectedRows, CursorBuilder.createCursor(t.getIndex("data_ind")));

      pkCursor.findFirstRowByEntry(4);
      final Row row4 = pkCursor.getCurrentRow();

      row4.put("id", 3);

      try {
        t.updateRow(row4);
        Assert.fail("ConstraintViolationException should have been thrown");
      } catch (final ConstraintViolationException ce) {
        // success
      }

      assertTable(expectedRows, t);

      assertCursor(expectedRows, pkCursor);

      assertCursor(expectedRows, CursorBuilder.createCursor(t.getIndex("data_ind")));

      db.close();
    }
  }

  @Test
  public void testAutoNumberRecover() throws Exception {
    for (final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      final Database db = create(fileFormat);

      final Table t = new TableBuilder("TestTable")
          .addColumn(new ColumnBuilder("id", DataType.LONG).setAutoNumber(true))
          .addColumn(new ColumnBuilder("data", DataType.TEXT))
          .addIndex(new IndexBuilder(IndexBuilder.PRIMARY_KEY_NAME).addColumns("id").setPrimaryKey())
          .addIndex(new IndexBuilder("data_ind").addColumns("data").setUnique()).toTable(db);

      for (int i = 1; i < 3; ++i) {
        t.addRow(null, "row" + i);
      }

      try {
        t.addRow(null, "row1");
        Assert.fail("ConstraintViolationException should have been thrown");
      } catch (final ConstraintViolationException ce) {
        // success
      }

      t.addRow(null, "row3");

      Assert.assertEquals(3, t.getRowCount());

      List<Row> expectedRows = createExpectedTable(createExpectedRow("id", 1, "data", "row1"),
          createExpectedRow("id", 2, "data", "row2"), createExpectedRow("id", 3, "data", "row3"));

      assertTable(expectedRows, t);

      final IndexCursor pkCursor = CursorBuilder.createPrimaryKeyCursor(t);
      assertCursor(expectedRows, pkCursor);

      assertCursor(expectedRows, CursorBuilder.createCursor(t.getIndex("data_ind")));

      final List<Object[]> batch = new ArrayList<Object[]>();
      batch.add(new Object[] { null, "row4" });
      batch.add(new Object[] { null, "row5" });
      batch.add(new Object[] { null, "row3" });

      try {
        t.addRows(batch);
        Assert.fail("BatchUpdateException should have been thrown");
      } catch (final BatchUpdateException be) {
        // success
        Assert.assertTrue(be.getCause() instanceof ConstraintViolationException);
        Assert.assertEquals(2, be.getUpdateCount());
      }

      expectedRows = new ArrayList<Row>(expectedRows);
      expectedRows.add(createExpectedRow("id", 4, "data", "row4"));
      expectedRows.add(createExpectedRow("id", 5, "data", "row5"));

      assertTable(expectedRows, t);

      assertCursor(expectedRows, pkCursor);

      assertCursor(expectedRows, CursorBuilder.createCursor(t.getIndex("data_ind")));

      db.close();
    }
  }

  @Test
  public void testBinaryIndex() throws Exception {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.BINARY_INDEX)) {
      final Database db = open(testDB);

      final Table table = db.getTable("Test");

      Index idx = table.getIndex("BinAscIdx");
      doTestBinaryIndex(idx, "BinAsc", false);

      idx = table.getIndex("BinDscIdx");
      doTestBinaryIndex(idx, "BinDsc", true);

      db.close();
    }
  }

  private static void doTestBinaryIndex(final Index idx, final String colName, final boolean forward) throws Exception {
    final IndexCursor ic = CursorBuilder.createCursor(idx);

    for (final Row row : idx.getTable().getDefaultCursor().newIterable().setForward(forward)) {
      final int id = row.getInt("ID");
      final byte[] data = row.getBytes(colName);

      boolean found = false;
      for (final Row idxRow : ic.newEntryIterable(data)) {

        Assert.assertTrue(Arrays.equals(data, idxRow.getBytes(colName)));
        if (id == idxRow.getInt("ID")) {
          found = true;
        }
      }

      Assert.assertTrue(found);
    }
  }

  private void doCheckForeignKeyIndex(final Table ta, final Index ia, final Table tb) throws Exception {
    final IndexImpl ib = (IndexImpl) ia.getReferencedIndex();
    Assert.assertNotNull(ib);
    Assert.assertSame(tb, ib.getTable());

    Assert.assertNotNull(ib.getReference());
    Assert.assertSame(ia, ib.getReferencedIndex());
    Assert.assertTrue(ib.getReference().isPrimaryTable());
  }

  private void checkIndexColumns(final Table table, final String... idxInfo) throws Exception {
    final Map<String, String> expectedIndexes = new HashMap<String, String>();
    for (int i = 0; i < idxInfo.length; i += 2) {
      expectedIndexes.put(idxInfo[i], idxInfo[i + 1]);
    }

    for (final Index idx : table.getIndexes()) {
      final String colName = expectedIndexes.get(idx.getName());
      Assert.assertEquals(1, idx.getColumns().size());
      Assert.assertEquals(colName, idx.getColumns().get(0).getName());
      if("PrimaryKey".equals(idx.getName())) {
        Assert.assertTrue(idx.isPrimaryKey());
      } else {
        Assert.assertFalse(idx.isPrimaryKey());
      }
    }
  }

}
