/*
Copyright (c) 2011 James Ahlborn

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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import static com.healthmarketscience.jackcess.Database.*;
import com.healthmarketscience.jackcess.impl.DatabaseImpl;
import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import com.healthmarketscience.jackcess.impl.PropertyMapImpl;
import com.healthmarketscience.jackcess.impl.PropertyMaps;
import com.healthmarketscience.jackcess.impl.TableImpl;
import static com.healthmarketscience.jackcess.TestUtil.*;

/**
 * @author James Ahlborn
 */
public class PropertiesTest
{
  @Test
  public void testPropertyMaps() throws Exception
  {
    PropertyMaps maps = new PropertyMaps(10, null, null, null);
    Assert.assertTrue(maps.isEmpty());
    Assert.assertEquals(0, maps.getSize());
    Assert.assertFalse(maps.iterator().hasNext());
    Assert.assertEquals(10, maps.getObjectId());

    PropertyMapImpl defMap = maps.getDefault();
    Assert.assertTrue(defMap.isEmpty());
    Assert.assertEquals(0, defMap.getSize());
    Assert.assertFalse(defMap.iterator().hasNext());

    PropertyMapImpl colMap = maps.get("testcol");
    Assert.assertTrue(colMap.isEmpty());
    Assert.assertEquals(0, colMap.getSize());
    Assert.assertFalse(colMap.iterator().hasNext());

    Assert.assertFalse(maps.isEmpty());
    Assert.assertEquals(2, maps.getSize());

    Assert.assertSame(defMap, maps.get(PropertyMaps.DEFAULT_NAME));
    Assert.assertEquals(PropertyMaps.DEFAULT_NAME, defMap.getName());
    Assert.assertSame(colMap, maps.get("TESTCOL"));
    Assert.assertEquals("testcol", colMap.getName());

    defMap.put("foo", DataType.TEXT, "bar", false);
    defMap.put("baz", DataType.LONG, 13, true);

    Assert.assertFalse(defMap.isEmpty());
    Assert.assertEquals(2, defMap.getSize());
    Assert.assertFalse(defMap.get("foo").isDdl());
    Assert.assertTrue(defMap.get("baz").isDdl());

    colMap.put("buzz", DataType.BOOLEAN, Boolean.TRUE, true);

    Assert.assertFalse(colMap.isEmpty());
    Assert.assertEquals(1, colMap.getSize());

    Assert.assertEquals("bar", defMap.getValue("foo"));
    Assert.assertEquals("bar", defMap.getValue("FOO"));
    Assert.assertNull(colMap.getValue("foo"));
    Assert.assertEquals(13, defMap.get("baz").getValue());
    Assert.assertEquals(Boolean.TRUE, colMap.getValue("Buzz"));

    Assert.assertEquals("bar", defMap.getValue("foo", "blah"));
    Assert.assertEquals("blah", defMap.getValue("bogus", "blah"));

    List<PropertyMap.Property> props = new ArrayList<PropertyMap.Property>();
    for(PropertyMap map : maps) {
      for(PropertyMap.Property prop : map) {
        props.add(prop);
      }
    }

    Assert.assertEquals(Arrays.asList(defMap.get("foo"), defMap.get("baz"),
                               colMap.get("buzz")), props);
  }

  @Test
  public void testInferTypes() throws Exception
  {
    PropertyMaps maps = new PropertyMaps(10, null, null, null);
    PropertyMap defMap = maps.getDefault();

    Assert.assertEquals(DataType.TEXT,
                 defMap.put(PropertyMap.FORMAT_PROP, null).getType());
    Assert.assertEquals(DataType.BOOLEAN,
                 defMap.put(PropertyMap.REQUIRED_PROP, null).getType());

    Assert.assertEquals(DataType.TEXT,
                 defMap.put("strprop", "this is a string").getType());
    Assert.assertEquals(DataType.BOOLEAN,
                 defMap.put("boolprop", true).getType());
    Assert.assertEquals(DataType.LONG,
                 defMap.put("intprop", 37).getType());
  }

  @Test
  public void testReadProperties() throws Exception
  {
    byte[] nameMapBytes = null;

    for(TestDB testDb : SUPPORTED_DBS_TEST_FOR_READ) {
      Database db = open(testDb);

      TableImpl t = (TableImpl)db.getTable("Table1");
      Assert.assertEquals(t.getTableDefPageNumber(),
                   t.getPropertyMaps().getObjectId());
      PropertyMap tProps = t.getProperties();
      Assert.assertEquals(PropertyMaps.DEFAULT_NAME, tProps.getName());
      int expectedNumProps = 3;
      if(db.getFileFormat() != Database.FileFormat.V1997) {
        Assert.assertEquals("{5A29A676-1145-4D1A-AE47-9F5415CDF2F1}",
                     tProps.getValue(PropertyMap.GUID_PROP));
        if(nameMapBytes == null) {
          nameMapBytes = (byte[])tProps.getValue("NameMap");
        } else {
          Assert.assertTrue(Arrays.equals(nameMapBytes,
                                   (byte[])tProps.getValue("NameMap")));
        }
        expectedNumProps += 2;
      }
      Assert.assertEquals(expectedNumProps, tProps.getSize());
      Assert.assertEquals((byte)0, tProps.getValue("Orientation"));
      Assert.assertEquals(Boolean.FALSE, tProps.getValue("OrderByOn"));
      Assert.assertEquals((byte)2, tProps.getValue("DefaultView"));

      PropertyMap colProps = t.getColumn("A").getProperties();
      Assert.assertEquals("A", colProps.getName());
      expectedNumProps = 9;
      if(db.getFileFormat() != Database.FileFormat.V1997) {
        Assert.assertEquals("{E9EDD90C-CE55-4151-ABE1-A1ACE1007515}",
                     colProps.getValue(PropertyMap.GUID_PROP));
        ++expectedNumProps;
      }
      Assert.assertEquals(expectedNumProps, colProps.getSize());
      Assert.assertEquals((short)-1, colProps.getValue("ColumnWidth"));
      Assert.assertEquals((short)0, colProps.getValue("ColumnOrder"));
      Assert.assertEquals(Boolean.FALSE, colProps.getValue("ColumnHidden"));
      Assert.assertEquals(Boolean.FALSE,
                   colProps.getValue(PropertyMap.REQUIRED_PROP));
      Assert.assertEquals(Boolean.FALSE,
                   colProps.getValue(PropertyMap.ALLOW_ZERO_LEN_PROP));
      Assert.assertEquals((short)109, colProps.getValue("DisplayControl"));
      Assert.assertEquals(Boolean.TRUE, colProps.getValue("UnicodeCompression"));
      Assert.assertEquals((byte)0, colProps.getValue("IMEMode"));
      Assert.assertEquals((byte)3, colProps.getValue("IMESentenceMode"));

      PropertyMap dbProps = db.getDatabaseProperties();
      Assert.assertTrue(((String)dbProps.getValue(PropertyMap.ACCESS_VERSION_PROP))
                 .matches("[0-9]{2}[.][0-9]{2}"));

      PropertyMap sumProps = db.getSummaryProperties();
      Assert.assertEquals(3, sumProps.getSize());
      Assert.assertEquals("test", sumProps.getValue(PropertyMap.TITLE_PROP));
      Assert.assertEquals("tmccune", sumProps.getValue(PropertyMap.AUTHOR_PROP));
      Assert.assertEquals("Health Market Science", sumProps.getValue(PropertyMap.COMPANY_PROP));

      PropertyMap userProps = db.getUserDefinedProperties();
      Assert.assertEquals(1, userProps.getSize());
      Assert.assertEquals(Boolean.TRUE, userProps.getValue("ReplicateProject"));

      db.close();
    }
  }

  @Test
  public void testParseProperties() throws Exception
  {
    for(FileFormat ff : SUPPORTED_FILEFORMATS_FOR_READ) {
      File[] dbFiles = new File(DIR_TEST_DATA, ff.name()).listFiles();
      if(dbFiles == null) {
        continue;
      }
      for(File f : dbFiles) {

        if(!f.isFile()) {
          continue;
        }

        Database db = open(ff, f);

        PropertyMap dbProps = db.getDatabaseProperties();
        Assert.assertFalse(dbProps.isEmpty());
        Assert.assertTrue(((String)dbProps.getValue(PropertyMap.ACCESS_VERSION_PROP))
                   .matches("[0-9]{2}[.][0-9]{2}"));

        for(Row row : ((DatabaseImpl)db).getSystemCatalog()) {
          int id = row.getInt("Id");
          byte[] propBytes = row.getBytes("LvProp");
          PropertyMaps propMaps = ((DatabaseImpl)db).getPropertiesForObject(
              id, null);
          int byteLen = ((propBytes != null) ? propBytes.length : 0);
          if(byteLen == 0) {
            Assert.assertTrue(propMaps.isEmpty());
          } else if(propMaps.isEmpty()) {
            Assert.assertTrue(byteLen < 80);
          } else {
            Assert.assertTrue(byteLen > 0);
          }
        }

        db.close();
      }
    }
  }

  @Test
  public void testWriteProperties() throws Exception
  {
    for(TestDB testDb : SUPPORTED_DBS_TEST) {
      Database db = open(testDb);

      TableImpl t = (TableImpl)db.getTable("Table1");

      PropertyMap tProps = t.getProperties();

      PropertyMaps maps = ((PropertyMapImpl)tProps).getOwner();

      byte[] mapsBytes = maps.write();

      PropertyMaps maps2 = ((DatabaseImpl)db).readProperties(
          mapsBytes, maps.getObjectId(), null);

      Iterator<PropertyMapImpl> iter = maps.iterator();
      Iterator<PropertyMapImpl> iter2 = maps2.iterator();

      while(iter.hasNext() && iter2.hasNext()) {
        PropertyMapImpl propMap = iter.next();
        PropertyMapImpl propMap2 = iter2.next();

        checkProperties(propMap, propMap2);
      }

      Assert.assertFalse(iter.hasNext());
      Assert.assertFalse(iter2.hasNext());

      db.close();
    }
  }

  @Test
  public void testModifyProperties() throws Exception
  {
    for(TestDB testDb : SUPPORTED_DBS_TEST) {
      Database db = openCopy(testDb);
      File dbFile = db.getFile();

      Table t = db.getTable("Table1");

      // grab originals
      PropertyMap origCProps = t.getColumn("C").getProperties();
      PropertyMap origFProps = t.getColumn("F").getProperties();
      PropertyMap origDProps = t.getColumn("D").getProperties();

      db.close();


      // modify but do not save
      db = new DatabaseBuilder(dbFile).open();

      t = db.getTable("Table1");

      PropertyMap cProps = t.getColumn("C").getProperties();
      PropertyMap fProps = t.getColumn("F").getProperties();
      PropertyMap dProps = t.getColumn("D").getProperties();

      Assert.assertFalse((Boolean)cProps.getValue(PropertyMap.REQUIRED_PROP));
      Assert.assertEquals("0", fProps.getValue(PropertyMap.DEFAULT_VALUE_PROP));
      Assert.assertEquals((short)109, dProps.getValue("DisplayControl"));

      cProps.put(PropertyMap.REQUIRED_PROP, DataType.BOOLEAN, true);
      fProps.get(PropertyMap.DEFAULT_VALUE_PROP).setValue("42");
      dProps.remove("DisplayControl");

      db.close();


      // modify and save
      db = new DatabaseBuilder(dbFile).open();

      t = db.getTable("Table1");

      cProps = t.getColumn("C").getProperties();
      fProps = t.getColumn("F").getProperties();
      dProps = t.getColumn("D").getProperties();

      Assert.assertFalse((Boolean)cProps.getValue(PropertyMap.REQUIRED_PROP));
      Assert.assertEquals("0", fProps.getValue(PropertyMap.DEFAULT_VALUE_PROP));
      Assert.assertEquals((short)109, dProps.getValue("DisplayControl"));

      checkProperties(origCProps, cProps);
      checkProperties(origFProps, fProps);
      checkProperties(origDProps, dProps);

      cProps.put(PropertyMap.REQUIRED_PROP, DataType.BOOLEAN, true);
      cProps.save();
      fProps.get(PropertyMap.DEFAULT_VALUE_PROP).setValue("42");
      fProps.save();
      dProps.remove("DisplayControl");
      dProps.save();

      db.close();


      // reload saved props
      db = new DatabaseBuilder(dbFile).open();

      t = db.getTable("Table1");

      cProps = t.getColumn("C").getProperties();
      fProps = t.getColumn("F").getProperties();
      dProps = t.getColumn("D").getProperties();

      Assert.assertTrue((Boolean)cProps.getValue(PropertyMap.REQUIRED_PROP));
      Assert.assertEquals("42", fProps.getValue(PropertyMap.DEFAULT_VALUE_PROP));
      Assert.assertNull(dProps.getValue("DisplayControl"));

      cProps.put(PropertyMap.REQUIRED_PROP, DataType.BOOLEAN, false);
      fProps.get(PropertyMap.DEFAULT_VALUE_PROP).setValue("0");
      dProps.put("DisplayControl", DataType.INT, (short)109);

      checkProperties(origCProps, cProps);
      checkProperties(origFProps, fProps);
      checkProperties(origDProps, dProps);

      db.close();
    }
  }

  @Test
  public void testCreateDbProperties() throws Exception
  {
    for(FileFormat ff : SUPPORTED_FILEFORMATS) {

      if(ff == FileFormat.GENERIC_JET4) {
        // weirdo format, no properties
        continue;
      }

      File file = TestUtil.createTempFile(false);
      Database db = new DatabaseBuilder(file)
        .setFileFormat(ff)
        .putUserDefinedProperty("testing", "123")
        .create();

      UUID u1 = UUID.randomUUID();
      UUID u2 = UUID.randomUUID();
      Table t = new TableBuilder("Test")
        .putProperty("awesome_table", true)
        .addColumn(new ColumnBuilder("id", DataType.LONG)
                   .setAutoNumber(true)
                   .putProperty(PropertyMap.REQUIRED_PROP, true)
                   .putProperty(PropertyMap.GUID_PROP, u1))
        .addColumn(new ColumnBuilder("data", DataType.TEXT)
                   .putProperty(PropertyMap.ALLOW_ZERO_LEN_PROP, false)
                   .putProperty(PropertyMap.GUID_PROP, u2))
        .toTable(db);

      t.addRow(Column.AUTO_NUMBER, "value");

      db.close();

      db = new DatabaseBuilder(file).open();

      Assert.assertEquals("123", db.getUserDefinedProperties().getValue("testing"));

      t = db.getTable("Test");

      Assert.assertEquals(Boolean.TRUE,
                   t.getProperties().getValue("awesome_table"));

      Column c = t.getColumn("id");
      Assert.assertEquals(Boolean.TRUE,
                   c.getProperties().getValue(PropertyMap.REQUIRED_PROP));
      Assert.assertEquals("{" + u1.toString().toUpperCase() + "}",
                   c.getProperties().getValue(PropertyMap.GUID_PROP));

      c = t.getColumn("data");
      Assert.assertEquals(Boolean.FALSE,
                   c.getProperties().getValue(PropertyMap.ALLOW_ZERO_LEN_PROP));
      Assert.assertEquals("{" + u2.toString().toUpperCase() + "}",
                   c.getProperties().getValue(PropertyMap.GUID_PROP));

    }
  }

  @Test
  public void testEnforceProperties() throws Exception
  {
    for(final FileFormat fileFormat : SUPPORTED_FILEFORMATS) {
      Database db = create(fileFormat);

      Table t = new TableBuilder("testReq")
        .addColumn(new ColumnBuilder("id", DataType.LONG)
                   .setAutoNumber(true)
                   .putProperty(PropertyMap.REQUIRED_PROP, true))
        .addColumn(new ColumnBuilder("value", DataType.TEXT)
                   .putProperty(PropertyMap.REQUIRED_PROP, true))
        .toTable(db);

      t.addRow(Column.AUTO_NUMBER, "v1");

      try {
        t.addRow(Column.AUTO_NUMBER, null);
        Assert.fail("InvalidValueException should have been thrown");
      } catch(InvalidValueException expected) {
        // success
      }

      t.addRow(Column.AUTO_NUMBER, "");

      List<? extends Map<String, Object>> expectedRows =
        createExpectedTable(
            createExpectedRow(
                "id", 1,
                "value", "v1"),
            createExpectedRow(
                "id", 2,
                "value", ""));
      assertTable(expectedRows, t);


      t = new TableBuilder("testNz")
        .addColumn(new ColumnBuilder("id", DataType.LONG)
                   .setAutoNumber(true)
                   .putProperty(PropertyMap.REQUIRED_PROP, true))
        .addColumn(new ColumnBuilder("value", DataType.TEXT)
                   .putProperty(PropertyMap.ALLOW_ZERO_LEN_PROP, false))
        .toTable(db);

      t.addRow(Column.AUTO_NUMBER, "v1");

      try {
        t.addRow(Column.AUTO_NUMBER, "");
        Assert.fail("InvalidValueException should have been thrown");
      } catch(InvalidValueException expected) {
        // success
      }

      t.addRow(Column.AUTO_NUMBER, null);

      expectedRows =
        createExpectedTable(
            createExpectedRow(
                "id", 1,
                "value", "v1"),
            createExpectedRow(
                "id", 2,
                "value", null));
      assertTable(expectedRows, t);


      t = new TableBuilder("testReqNz")
        .addColumn(new ColumnBuilder("id", DataType.LONG)
                   .setAutoNumber(true)
                   .putProperty(PropertyMap.REQUIRED_PROP, true))
        .addColumn(new ColumnBuilder("value", DataType.TEXT))
        .toTable(db);

      Column col = t.getColumn("value");
      PropertyMap props = col.getProperties();
      props.put(PropertyMap.REQUIRED_PROP, true);
      props.put(PropertyMap.ALLOW_ZERO_LEN_PROP, false);
      props.save();

      t.addRow(Column.AUTO_NUMBER, "v1");

      try {
        t.addRow(Column.AUTO_NUMBER, "");
        Assert.fail("InvalidValueException should have been thrown");
      } catch(InvalidValueException expected) {
        // success
      }

      try {
        t.addRow(Column.AUTO_NUMBER, null);
        Assert.fail("InvalidValueException should have been thrown");
      } catch(InvalidValueException expected) {
        // success
      }

      t.addRow(Column.AUTO_NUMBER, "v2");

      expectedRows =
        createExpectedTable(
            createExpectedRow(
                "id", 1,
                "value", "v1"),
            createExpectedRow(
                "id", 2,
                "value", "v2"));
      assertTable(expectedRows, t);

      db.close();
    }
  }

  @Test
  public void testEnumValues() throws Exception
  {
    PropertyMaps maps = new PropertyMaps(10, null, null, null);

    PropertyMapImpl colMap = maps.get("testcol");

    colMap.put(PropertyMap.DISPLAY_CONTROL_PROP,
               PropertyMap.DisplayControl.TEXT_BOX);

    Assert.assertEquals(PropertyMap.DisplayControl.TEXT_BOX.getValue(),
                 colMap.getValue(PropertyMap.DISPLAY_CONTROL_PROP));
  }

  private static void checkProperties(PropertyMap propMap1,
                                      PropertyMap propMap2)
  {
    Assert.assertEquals(propMap1.getSize(), propMap2.getSize());
    for(PropertyMap.Property prop : propMap1) {
      PropertyMap.Property prop2 = propMap2.get(prop.getName());

      Assert.assertEquals(prop.getName(), prop2.getName());
      Assert.assertEquals(prop.getType(), prop2.getType());

      Object v1 = prop.getValue();
      Object v2 = prop2.getValue();

      if(v1 instanceof byte[]) {
        Assert.assertTrue(Arrays.equals((byte[])v1, (byte[])v2));
      } else {
        Assert.assertEquals(v1, v2);
      }
    }
  }

}
