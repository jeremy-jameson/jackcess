/*
Copyright (c) 2008 Health Market Science, Inc.

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
import java.util.Comparator;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import static com.healthmarketscience.jackcess.impl.JetFormatTest.*;
import com.healthmarketscience.jackcess.impl.RelationshipImpl;
import static com.healthmarketscience.jackcess.TestUtil.*;

/**
 * @author James Ahlborn
 */
public class RelationshipTest {

  private static final Comparator<Relationship> REL_COMP = new Comparator<Relationship>() {
    public int compare(Relationship r1, Relationship r2) {
      return String.CASE_INSENSITIVE_ORDER.compare(r1.getName(), r2.getName());
    }
  };

  @Test
  public void testTwoTables() throws Exception {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX, true)) {
      Database db = open(testDB);
      Table t1 = db.getTable("Table1");
      Table t2 = db.getTable("Table2");
      Table t3 = db.getTable("Table3");

      List<Relationship> rels = db.getRelationships(t1, t2);
      Assert.assertEquals(1, rels.size());
      Relationship rel = rels.get(0);
      Assert.assertEquals("Table2Table1", rel.getName());
      Assert.assertEquals(t2, rel.getFromTable());
      Assert.assertEquals(Arrays.asList(t2.getColumn("id")),
                   rel.getFromColumns());
      Assert.assertEquals(t1, rel.getToTable());
      Assert.assertEquals(Arrays.asList(t1.getColumn("otherfk1")),
                   rel.getToColumns());
      Assert.assertTrue(rel.hasReferentialIntegrity());
      Assert.assertEquals(4096, ((RelationshipImpl)rel).getFlags());
      Assert.assertTrue(rel.cascadeDeletes());
      assertSameRelationships(rels, db.getRelationships(t2, t1), true);

      rels = db.getRelationships(t2, t3);
      Assert.assertTrue(db.getRelationships(t2, t3).isEmpty());
      assertSameRelationships(rels, db.getRelationships(t3, t2), true);

      rels = db.getRelationships(t1, t3);
      Assert.assertEquals(1, rels.size());
      rel = rels.get(0);
      Assert.assertEquals("Table3Table1", rel.getName());
      Assert.assertEquals(t3, rel.getFromTable());
      Assert.assertEquals(Arrays.asList(t3.getColumn("id")),
                   rel.getFromColumns());
      Assert.assertEquals(t1, rel.getToTable());
      Assert.assertEquals(Arrays.asList(t1.getColumn("otherfk2")),
                   rel.getToColumns());
      Assert.assertTrue(rel.hasReferentialIntegrity());
      Assert.assertEquals(256, ((RelationshipImpl)rel).getFlags());
      Assert.assertTrue(rel.cascadeUpdates());
      assertSameRelationships(rels, db.getRelationships(t3, t1), true);

      try {
        db.getRelationships(t1, t1);
        Assert.fail("IllegalArgumentException should have been thrown");
      } catch(IllegalArgumentException ignored) {
        // success
      }
    }
  }

  @Test
  public void testOneTable() throws Exception {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX, true)) {
      Database db = open(testDB);
      Table t1 = db.getTable("Table1");
      Table t2 = db.getTable("Table2");
      Table t3 = db.getTable("Table3");

      List<Relationship> expected = new ArrayList<Relationship>();
      expected.addAll(db.getRelationships(t1, t2));
      expected.addAll(db.getRelationships(t2, t3));

      assertSameRelationships(expected, db.getRelationships(t2), false);
      
    }
  }

  @Test
  public void testNoTables() throws Exception {
    for (final TestDB testDB : TestDB.getSupportedForBasename(Basename.INDEX, true)) {
      Database db = open(testDB);
      Table t1 = db.getTable("Table1");
      Table t2 = db.getTable("Table2");
      Table t3 = db.getTable("Table3");

      List<Relationship> expected = new ArrayList<Relationship>();
      expected.addAll(db.getRelationships(t1, t2));
      expected.addAll(db.getRelationships(t2, t3));
      expected.addAll(db.getRelationships(t1, t3));

      assertSameRelationships(expected, db.getRelationships(), false);
    }
  }

  private static void assertSameRelationships(
      List<Relationship> expected, List<Relationship> found, boolean ordered)
  {
    Assert.assertEquals(expected.size(), found.size());
    if(!ordered) {
      Collections.sort(expected, REL_COMP);
      Collections.sort(found, REL_COMP);
    }
    for(int i = 0; i < expected.size(); ++i) {
      Relationship eRel = expected.get(i);
      Relationship fRel = found.get(i);
      Assert.assertEquals(eRel.getName(), fRel.getName());
    }
  }
  
}
