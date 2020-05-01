/*
Copyright (c) 2016 James Ahlborn

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

package com.healthmarketscience.jackcess.impl.expr;

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import javax.script.Bindings;
import javax.script.SimpleBindings;

import org.junit.Assert;
import org.junit.Test;

import com.healthmarketscience.jackcess.DataType;
import com.healthmarketscience.jackcess.TestUtil;
import com.healthmarketscience.jackcess.expr.EvalContext;
import com.healthmarketscience.jackcess.expr.Expression;
import com.healthmarketscience.jackcess.expr.FunctionLookup;
import com.healthmarketscience.jackcess.expr.Identifier;
import com.healthmarketscience.jackcess.expr.NumericConfig;
import com.healthmarketscience.jackcess.expr.ParseException;
import com.healthmarketscience.jackcess.expr.TemporalConfig;
import com.healthmarketscience.jackcess.expr.Value;
import com.healthmarketscience.jackcess.impl.BaseEvalContext;

/**
 *
 * @author James Ahlborn
 */
public class ExpressionatorTest
{
  private static final double[] DBLS = {
    -10.3d,-9.0d,-8.234d,-7.11111d,-6.99999d,-5.5d,-4.0d,-3.4159265d,-2.84d,
    -1.0000002d,-1.0d,-0.0002013d,0.0d, 0.9234d,1.0d,1.954d,2.200032d,3.001d,
    4.9321d,5.0d,6.66666d,7.396d,8.1d,9.20456200d,10.325d};

  private static final int TRUE_NUM = -1;
  private static final int FALSE_NUM = 0;

  @Test
  public void testParseSimpleExprs() throws Exception
  {
    validateExpr("\"A\"", "<ELiteralValue>{\"A\"}");

    validateExpr("13", "<ELiteralValue>{13}");

    validateExpr("-42", "<EUnaryOp>{- <ELiteralValue>{42}}");

    validateExpr("(+37)", "<EParen>{(<EUnaryOp>{+ <ELiteralValue>{37}})}");

    doTestSimpleBinOp("EBinaryOp", "+", "-", "*", "/", "\\", "^", "&", "Mod");
    doTestSimpleBinOp("ECompOp", "<", "<=", ">", ">=", "=", "<>");
    doTestSimpleBinOp("ELogicalOp", "And", "Or", "Eqv", "Xor", "Imp");

    for(String constStr : new String[]{"True", "False", "Null"}) {
      validateExpr(constStr, "<EConstValue>{" + constStr + "}");
    }

    validateExpr("[Field1]", "<EObjValue>{[Field1]}");

    validateExpr("[Table2].[Field3]", "<EObjValue>{[Table2].[Field3]}");

    validateExpr("Not \"A\"", "<EUnaryOp>{Not <ELiteralValue>{\"A\"}}");

    validateExpr("-[Field1]", "<EUnaryOp>{- <EObjValue>{[Field1]}}");

    validateExpr("\"A\" Is Null", "<ENullOp>{<ELiteralValue>{\"A\"} Is Null}");

    validateExpr("\"A\" In (1,2,3)", "<EInOp>{<ELiteralValue>{\"A\"} In (<ELiteralValue>{1},<ELiteralValue>{2},<ELiteralValue>{3})}");

    validateExpr("\"A\" Not Between 3 And 7", "<EBetweenOp>{<ELiteralValue>{\"A\"} Not Between <ELiteralValue>{3} And <ELiteralValue>{7}}");

    validateExpr("(\"A\" Or \"B\")", "<EParen>{(<ELogicalOp>{<ELiteralValue>{\"A\"} Or <ELiteralValue>{\"B\"}})}");

    validateExpr("IIf(\"A\",42,False)", "<EFunc>{IIf(<ELiteralValue>{\"A\"},<ELiteralValue>{42},<EConstValue>{False})}");

    validateExpr("\"A\" Like \"a*b\"", "<ELikeOp>{<ELiteralValue>{\"A\"} Like \"a*b\"(a.*b)}");

    validateExpr("' \"A\" '", "<ELiteralValue>{\" \"\"A\"\" \"}",
                 "\" \"\"A\"\" \"");

    validateExpr("<=1 And >=0", "<ELogicalOp>{<ECompOp>{<EThisValue>{<THIS_COL>} <= <ELiteralValue>{1}} And <ECompOp>{<EThisValue>{<THIS_COL>} >= <ELiteralValue>{0}}}", "<= 1 And >= 0");
  }

  private static void doTestSimpleBinOp(String opName, String... ops) throws Exception
  {
    for(String op : ops) {
      validateExpr("\"A\" " + op + " \"B\"",
                   "<" + opName + ">{<ELiteralValue>{\"A\"} " + op +
                   " <ELiteralValue>{\"B\"}}");
    }
  }

  @Test
  public void testOrderOfOperations() throws Exception
  {
    validateExpr("\"A\" Eqv \"B\"",
                 "<ELogicalOp>{<ELiteralValue>{\"A\"} Eqv <ELiteralValue>{\"B\"}}");

    validateExpr("\"A\" Eqv \"B\" Xor \"C\"",
                 "<ELogicalOp>{<ELiteralValue>{\"A\"} Eqv <ELogicalOp>{<ELiteralValue>{\"B\"} Xor <ELiteralValue>{\"C\"}}}");

    validateExpr("\"A\" Eqv \"B\" Xor \"C\" Or \"D\"",
                 "<ELogicalOp>{<ELiteralValue>{\"A\"} Eqv <ELogicalOp>{<ELiteralValue>{\"B\"} Xor <ELogicalOp>{<ELiteralValue>{\"C\"} Or <ELiteralValue>{\"D\"}}}}");

    validateExpr("\"A\" Eqv \"B\" Xor \"C\" Or \"D\" And \"E\"",
                 "<ELogicalOp>{<ELiteralValue>{\"A\"} Eqv <ELogicalOp>{<ELiteralValue>{\"B\"} Xor <ELogicalOp>{<ELiteralValue>{\"C\"} Or <ELogicalOp>{<ELiteralValue>{\"D\"} And <ELiteralValue>{\"E\"}}}}}");

    validateExpr("\"A\" Or \"B\" Or \"C\"",
                 "<ELogicalOp>{<ELogicalOp>{<ELiteralValue>{\"A\"} Or <ELiteralValue>{\"B\"}} Or <ELiteralValue>{\"C\"}}");

    validateExpr("\"A\" & \"B\" Is Null",
                 "<ENullOp>{<EBinaryOp>{<ELiteralValue>{\"A\"} & <ELiteralValue>{\"B\"}} Is Null}");

    validateExpr("\"A\" Or \"B\" Is Null",
                 "<ELogicalOp>{<ELiteralValue>{\"A\"} Or <ENullOp>{<ELiteralValue>{\"B\"} Is Null}}");

    validateExpr("Not \"A\" & \"B\"",
                 "<EUnaryOp>{Not <EBinaryOp>{<ELiteralValue>{\"A\"} & <ELiteralValue>{\"B\"}}}");

    validateExpr("Not \"A\" Or \"B\"",
                 "<ELogicalOp>{<EUnaryOp>{Not <ELiteralValue>{\"A\"}} Or <ELiteralValue>{\"B\"}}");

    validateExpr("\"A\" + \"B\" Not Between 37 - 15 And 52 / 4",
                 "<EBetweenOp>{<EBinaryOp>{<ELiteralValue>{\"A\"} + <ELiteralValue>{\"B\"}} Not Between <EBinaryOp>{<ELiteralValue>{37} - <ELiteralValue>{15}} And <EBinaryOp>{<ELiteralValue>{52} / <ELiteralValue>{4}}}");

    validateExpr("\"A\" + (\"B\" Not Between 37 - 15 And 52) / 4",
                 "<EBinaryOp>{<ELiteralValue>{\"A\"} + <EBinaryOp>{<EParen>{(<EBetweenOp>{<ELiteralValue>{\"B\"} Not Between <EBinaryOp>{<ELiteralValue>{37} - <ELiteralValue>{15}} And <ELiteralValue>{52}})} / <ELiteralValue>{4}}}");


  }

  @Test
  public void testSimpleMathExpressions() throws Exception
  {
    for(int i = -10; i <= 10; ++i) {
      Assert.assertEquals(-i, eval("=-(" + i + ")"));
    }

    for(int i = -10; i <= 10; ++i) {
      Assert.assertEquals(i, eval("=+(" + i + ")"));
    }

    for(double i : DBLS) {
      Assert.assertEquals(toBD(-i), eval("=-(" + i + ")"));
    }

    for(double i : DBLS) {
      Assert.assertEquals(toBD(i), eval("=+(" + i + ")"));
    }

    for(int i = -10; i <= 10; ++i) {
      for(int j = -10; j <= 10; ++j) {
        Assert.assertEquals((i + j), eval("=" + i + " + " + j));
      }
    }

    for(double i : DBLS) {
      for(double j : DBLS) {
        Assert.assertEquals(toBD(toBD(i).add(toBD(j))), eval("=" + i + " + " + j));
      }
    }

    for(int i = -10; i <= 10; ++i) {
      for(int j = -10; j <= 10; ++j) {
        Assert.assertEquals((i - j), eval("=" + i + " - " + j));
      }
    }

    for(double i : DBLS) {
      for(double j : DBLS) {
        Assert.assertEquals(toBD(toBD(i).subtract(toBD(j))), eval("=" + i + " - " + j));
      }
    }

    for(int i = -10; i <= 10; ++i) {
      for(int j = -10; j <= 10; ++j) {
        Assert.assertEquals((i * j), eval("=" + i + " * " + j));
      }
    }

    for(double i : DBLS) {
      for(double j : DBLS) {
        Assert.assertEquals(toBD(toBD(i).multiply(toBD(j))), eval("=" + i + " * " + j));
      }
    }

    for(int i = -10; i <= 10; ++i) {
      for(int j = -10; j <= 10; ++j) {
        if(j == 0L) {
          evalFail("=" + i + " \\ " + j, ArithmeticException.class);
        } else {
          Assert.assertEquals((i / j), eval("=" + i + " \\ " + j));
        }
      }
    }

    for(double i : DBLS) {
      for(double j : DBLS) {
        if(roundToLongInt(j) == 0) {
          evalFail("=" + i + " \\ " + j, ArithmeticException.class);
        } else {
          Assert.assertEquals((roundToLongInt(i) / roundToLongInt(j)),
                       eval("=" + i + " \\ " + j));
        }
      }
    }

    for(int i = -10; i <= 10; ++i) {
      for(int j = -10; j <= 10; ++j) {
        if(j == 0) {
          evalFail("=" + i + " Mod " + j, ArithmeticException.class);
        } else {
          Assert.assertEquals((i % j), eval("=" + i + " Mod " + j));
        }
      }
    }

    for(double i : DBLS) {
      for(double j : DBLS) {
        if(roundToLongInt(j) == 0) {
          evalFail("=" + i + " Mod " + j, ArithmeticException.class);
        } else {
          Assert.assertEquals((roundToLongInt(i) % roundToLongInt(j)),
                       eval("=" + i + " Mod " + j));
        }
      }
    }

    for(int i = -10; i <= 10; ++i) {
      for(int j = -10; j <= 10; ++j) {
        if(j == 0) {
          evalFail("=" + i + " / " + j, ArithmeticException.class);
        } else {
          double result = (double)i / (double)j;
          if((int)result == result) {
            Assert.assertEquals((int)result, eval("=" + i + " / " + j));
          } else {
            Assert.assertEquals(result, eval("=" + i + " / " + j));
          }
        }
      }
    }

    for(double i : DBLS) {
      for(double j : DBLS) {
        if(j == 0.0d) {
          evalFail("=" + i + " / " + j, ArithmeticException.class);
        } else {
          Assert.assertEquals(toBD(BuiltinOperators.divide(toBD(i), toBD(j))),
                       eval("=" + i + " / " + j));
        }
      }
    }

    for(int i = -10; i <= 10; ++i) {
      for(int j = -10; j <= 10; ++j) {
        double result = Math.pow(i, j);
        if((int)result == result) {
          Assert.assertEquals((int)result, eval("=" + i + " ^ " + j));
        } else {
          Assert.assertEquals(result, eval("=" + i + " ^ " + j));
        }
      }
    }
  }

  @Test
  public void testComparison() throws Exception
  {
    Assert.assertEquals(TRUE_NUM, eval("='blah'<'fuzz'"));
    Assert.assertEquals(FALSE_NUM, eval("=23>56"));
    Assert.assertEquals(FALSE_NUM, eval("=23>=56"));
    Assert.assertEquals(TRUE_NUM, eval("=13.2<=45.8"));
    Assert.assertEquals(FALSE_NUM, eval("='blah'='fuzz'"));
    Assert.assertEquals(TRUE_NUM, eval("='blah'<>'fuzz'"));
    Assert.assertEquals(TRUE_NUM, eval("=CDbl(13.2)<=CDbl(45.8)"));

    Assert.assertEquals(FALSE_NUM, eval("='blah' Is Null"));
    Assert.assertEquals(TRUE_NUM, eval("='blah' Is Not Null"));

    Assert.assertEquals(TRUE_NUM, eval("=Null Is Null"));
    Assert.assertEquals(FALSE_NUM, eval("=Null Is Not Null"));

    Assert.assertEquals(TRUE_NUM, eval("='blah' Between 'a' And 'z'"));
    Assert.assertEquals(TRUE_NUM, eval("='blah' Between 'z' And 'a'"));
    Assert.assertEquals(FALSE_NUM, eval("='blah' Not Between 'a' And 'z'"));

    Assert.assertEquals(TRUE_NUM, eval("='blah' In ('foo','bar','blah')"));
    Assert.assertEquals(FALSE_NUM, eval("='blah' Not In ('foo','bar','blah')"));

    Assert.assertEquals(TRUE_NUM, eval("=True Xor False"));
    Assert.assertEquals(TRUE_NUM, eval("=True Or False"));
    Assert.assertEquals(TRUE_NUM, eval("=False Or True"));
    Assert.assertEquals(FALSE_NUM, eval("=True Imp False"));
    Assert.assertEquals(FALSE_NUM, eval("=True Eqv False"));
    Assert.assertEquals(TRUE_NUM, eval("=Not(True Eqv False)"));
  }

  @Test
  public void testDateArith() throws Exception
  {
    Assert.assertEquals(LocalDateTime.of(2003,1,2,7,0), eval("=#01/02/2003# + #7:00:00 AM#"));
    Assert.assertEquals(LocalDateTime.of(2003,1,1,17,0), eval("=#01/02/2003# - #7:00:00 AM#"));
    Assert.assertEquals(LocalDateTime.of(2003,2,8,0,0), eval("=#01/02/2003# + '37'"));
    Assert.assertEquals(LocalDateTime.of(2003,2,8,0,0), eval("='37' + #01/02/2003#"));
    Assert.assertEquals(LocalDateTime.of(2003,1,2,7,0), eval("=#01/02/2003 7:00:00 AM#"));

    Assert.assertEquals("2/8/2003", eval("=CStr(#01/02/2003# + '37')"));
    Assert.assertEquals("9:24:00 AM", eval("=CStr(#7:00:00 AM# + 0.1)"));
    Assert.assertEquals("1/2/2003 1:10:00 PM", eval("=CStr(#01/02/2003# + #13:10:00#)"));
  }

  @Test
  public void testNull() throws Exception
  {
    Assert.assertNull(eval("=37 + Null"));
    Assert.assertNull(eval("=37 - Null"));
    Assert.assertNull(eval("=37 / Null"));
    Assert.assertNull(eval("=37 * Null"));
    Assert.assertNull(eval("=37 ^ Null"));
    Assert.assertEquals("37", eval("=37 & Null"));
    Assert.assertEquals("37", eval("=Null & 37"));
    Assert.assertNull(eval("=37 Mod Null"));
    Assert.assertNull(eval("=37 \\ Null"));
    Assert.assertNull(eval("=-(Null)"));
    Assert.assertNull(eval("=+(Null)"));
    Assert.assertNull(eval("=Not Null"));
    Assert.assertEquals(TRUE_NUM, eval("=37 Or Null"));
    Assert.assertNull(eval("=Null Or 37"));
    Assert.assertNull(eval("=37 And Null"));
    Assert.assertNull(eval("=Null And 37"));
    Assert.assertNull(eval("=37 Xor Null"));
    Assert.assertNull(eval("=37 Imp Null"));
    Assert.assertNull(eval("=Null Imp Null"));
    Assert.assertEquals(TRUE_NUM, eval("=Null Imp 37"));
    Assert.assertNull(eval("=37 Eqv Null"));
    Assert.assertNull(eval("=37 < Null"));
    Assert.assertNull(eval("=37 > Null"));
    Assert.assertNull(eval("=37 = Null"));
    Assert.assertNull(eval("=37 <> Null"));
    Assert.assertNull(eval("=37 <= Null"));
    Assert.assertNull(eval("=37 >= Null"));

    Assert.assertNull(eval("=37 Between Null And 54"));
    Assert.assertEquals(FALSE_NUM, eval("=37 In (23, Null, 45)"));
    Assert.assertNull(eval("=Null In (23, Null, 45)"));
  }

  @Test
  public void testTrickyMathExpressions() throws Exception
  {
    Assert.assertEquals(37, eval("=30+7"));
    Assert.assertEquals(23, eval("=30+-7"));
    Assert.assertEquals(23, eval("=30-+7"));
    Assert.assertEquals(37, eval("=30--7"));
    Assert.assertEquals(23, eval("=30-7"));

    Assert.assertEquals(100, eval("=-10^2"));
    Assert.assertEquals(-100, eval("=-(10)^2"));
    Assert.assertEquals(-100d, eval("=-\"10\"^2"));
    Assert.assertEquals(toBD(-98.9d), eval("=1.1+(-\"10\"^2)"));

    Assert.assertEquals(toBD(99d), eval("=-10E-1+10e+1"));
    Assert.assertEquals(toBD(-101d), eval("=-10E-1-10e+1"));
  }

  @Test
  public void testTypeCoercion() throws Exception
  {
    Assert.assertEquals("foobar", eval("=\"foo\" + \"bar\""));

    Assert.assertEquals("12foo", eval("=12 + \"foo\""));
    Assert.assertEquals("foo12", eval("=\"foo\" + 12"));

    Assert.assertEquals(37d, eval("=\"25\" + 12"));
    Assert.assertEquals(37d, eval("=12 + \"25\""));
    Assert.assertEquals(37d, eval("=\" 25 \" + 12"));
    Assert.assertEquals(37d, eval("=\" &h1A \" + 11"));
    Assert.assertEquals(37d, eval("=\" &h1a \" + 11"));
    Assert.assertEquals(37d, eval("=\" &O32 \" + 11"));
    Assert.assertEquals(1037d, eval("=\"1,025\" + 12"));

    evalFail(("=12 - \"foo\""), RuntimeException.class);
    evalFail(("=\"foo\" - 12"), RuntimeException.class);

    Assert.assertEquals("foo1225", eval("=\"foo\" + 12 + 25"));
    Assert.assertEquals("37foo", eval("=12 + 25 + \"foo\""));
    Assert.assertEquals("foo37", eval("=\"foo\" + (12 + 25)"));
    Assert.assertEquals("25foo12", eval("=\"25foo\" + 12"));

    Assert.assertEquals(LocalDateTime.of(2017,1,28,0,0), eval("=#1/1/2017# + 27"));
    Assert.assertEquals(128208, eval("=#1/1/2017# * 3"));
  }

  @Test
  public void testLikeExpression() throws Exception
  {
    validateExpr("Like \"[abc]*\"", "<ELikeOp>{<EThisValue>{<THIS_COL>} Like \"[abc]*\"([abc].*)}",
                 "Like \"[abc]*\"");
    Assert.assertTrue(evalCondition("Like \"[abc]*\"", "afcd"));
    Assert.assertFalse(evalCondition("Like \"[abc]*\"", "fcd"));

    validateExpr("Like  \"[abc*\"", "<ELikeOp>{<EThisValue>{<THIS_COL>} Like \"[abc*\"((?!))}",
                 "Like \"[abc*\"");
    Assert.assertFalse(evalCondition("Like \"[abc*\"", "afcd"));
    Assert.assertFalse(evalCondition("Like \"[abc*\"", "fcd"));
    Assert.assertTrue(evalCondition("Not Like \"[abc*\"", "fcd"));
    Assert.assertFalse(evalCondition("Like \"[abc*\"", ""));
  }

  @Test
  public void testLiteralDefaultValue() throws Exception
  {
    Assert.assertEquals("-28 blah ", eval("=CDbl(9)-37 & \" blah \"",
                                     Value.Type.STRING));
    Assert.assertEquals("CDbl(9)-37 & \" blah \"",
                 eval("CDbl(9)-37 & \" blah \"", Value.Type.STRING));

    Assert.assertEquals(-28d, eval("=CDbl(9)-37", Value.Type.DOUBLE));
    Assert.assertEquals(-28d, eval("CDbl(9)-37", Value.Type.DOUBLE));
  }

  @Test
  public void testParseSomeExprs() throws Exception
  {
    BufferedReader br = new BufferedReader(new FileReader("src/test/resources/test_exprs.txt"));

    TestContext tc = new TestContext() {
      @Override
      public Value getThisColumnValue() {
        return ValueSupport.toValue(23.0);
      }

      @Override
      public Value getIdentifierValue(Identifier identifier) {
        return ValueSupport.toValue(23.0);
      }
    };

    String line = null;
    while((line = br.readLine()) != null) {
      line = line.trim();
      if(line.isEmpty()) {
        continue;
      }

      String[] parts = line.split(";", 3);
      Expressionator.Type type = Expressionator.Type.valueOf(parts[0]);
      DataType dType =
        (("null".equals(parts[1])) ? null : DataType.valueOf(parts[1]));
      String exprStr = parts[2];

      Value.Type resultType = ((dType != null) ?
                               BaseEvalContext.toValueType(dType) : null);

      Expression expr = Expressionator.parse(
          type, exprStr, resultType, tc);

      expr.eval(tc);
    }

    br.close();
  }

  @Test
  public void testInvalidExpressions() throws Exception
  {
    doTestEvalFail("", "empty");
    doTestEvalFail("=", "found?");
    doTestEvalFail("=(34 + 5", "closing");
    doTestEvalFail("=(34 + )", "found?");
    doTestEvalFail("=(34 + [A].[B].[C].[D])", "object reference");
    doTestEvalFail("=34 + 5,", "delimiter");
    doTestEvalFail("=Foo()", "find function");
    doTestEvalFail("=(/37)", "left expression");
    doTestEvalFail("=(>37)", "left expression");
    doTestEvalFail("=(And 37)", "left expression");
    doTestEvalFail("=37 In 42", "'In' expression");
    doTestEvalFail("=37 Between 42", "'Between' expression");
    doTestEvalFail("=(3 + 5) Rnd()", "multiple expressions");
  }

  private static void doTestEvalFail(String exprStr, String msgStr) {
    try {
      eval(exprStr);
      Assert.fail("ParseException should have been thrown");
    } catch(ParseException pe) {
      // success
      Assert.assertTrue(pe.getMessage().contains(msgStr));
    }
  }

  private static void validateExpr(String exprStr, String debugStr) {
    validateExpr(exprStr, debugStr, exprStr);
  }

  private static void validateExpr(String exprStr, String debugStr,
                                   String cleanStr) {
    TestContext ctx = new TestContext();
    Expression expr = Expressionator.parse(
        Expressionator.Type.FIELD_VALIDATOR, exprStr, null, ctx);
    String foundDebugStr = expr.toDebugString(ctx);
    if(foundDebugStr.startsWith("<EImplicitCompOp>")) {
      Assert.assertEquals("<EImplicitCompOp>{<EThisValue>{<THIS_COL>} = " +
                   debugStr + "}", foundDebugStr);
    } else {
      Assert.assertEquals(debugStr, foundDebugStr);
    }
    Assert.assertEquals(cleanStr, expr.toCleanString(ctx));
    Assert.assertEquals(exprStr, expr.toRawString());
  }

  static Object eval(String exprStr) {
    return eval(exprStr, null);
  }

  static Object eval(String exprStr, Value.Type resultType) {
    TestContext tc = new TestContext();
    Expression expr = Expressionator.parse(
        Expressionator.Type.DEFAULT_VALUE, exprStr, resultType, tc);
    return expr.eval(tc);
  }

  private static void evalFail(
      String exprStr, Class<? extends Exception> failure)
  {
    TestContext tc = new TestContext();
    Expression expr = Expressionator.parse(
        Expressionator.Type.DEFAULT_VALUE, exprStr, null, tc);
    try {
      expr.eval(tc);
      Assert.fail(failure + " should have been thrown");
    } catch(Exception e) {
      Assert.assertTrue(failure.isInstance(e));
    }
  }

  private static Boolean evalCondition(String exprStr, String thisVal) {
    TestContext tc = new TestContext(ValueSupport.toValue(thisVal));
    Expression expr = Expressionator.parse(
        Expressionator.Type.FIELD_VALIDATOR, exprStr, null, tc);
    return (Boolean)expr.eval(tc);
  }

  static int roundToLongInt(double d) {
    return new BigDecimal(d).setScale(0, NumberFormatter.ROUND_MODE)
      .intValueExact();
  }

  static BigDecimal toBD(double d) {
    return toBD(BigDecimal.valueOf(d));
  }

  static BigDecimal toBD(BigDecimal bd) {
    return ValueSupport.normalize(bd);
  }

  private static class TestContext
    implements Expressionator.ParseContext, EvalContext
  {
    private final Value _thisVal;
    private final RandomContext _rndCtx = new RandomContext();
    private final Bindings _bindings = new SimpleBindings();

    private TestContext() {
      this(null);
    }

    private TestContext(Value thisVal) {
      _thisVal = thisVal;
    }

    public Value.Type getResultType() {
      return null;
    }

    public TemporalConfig getTemporalConfig() {
      return TemporalConfig.US_TEMPORAL_CONFIG;
    }

    public DateTimeFormatter createDateFormatter(String formatStr) {
      DateTimeFormatter dtf = DateTimeFormatter.ofPattern(
          formatStr, TemporalConfig.US_TEMPORAL_CONFIG.getLocale());
      return dtf;
    }

    public ZoneId getZoneId() {
      return TestUtil.TEST_TZ.toZoneId();
    }

    public NumericConfig getNumericConfig() {
      return NumericConfig.US_NUMERIC_CONFIG;
    }

    public DecimalFormat createDecimalFormat(String formatStr) {
      return new DecimalFormat(
          formatStr, NumericConfig.US_NUMERIC_CONFIG.getDecimalFormatSymbols());
    }

    public FunctionLookup getFunctionLookup() {
      return DefaultFunctions.LOOKUP;
    }

    public Value getThisColumnValue() {
      if(_thisVal == null) {
        throw new UnsupportedOperationException();
      }
      return _thisVal;
    }

    public Value getIdentifierValue(Identifier identifier) {
      throw new UnsupportedOperationException();
    }

    public float getRandom(Integer seed) {
      return _rndCtx.getRandom(seed);
    }

    public Bindings getBindings() {
      return _bindings;
    }

    public Object get(String key) {
      return _bindings.get(key);
    }

    public void put(String key, Object value) {
      _bindings.put(key, value);
    }
  }
}
