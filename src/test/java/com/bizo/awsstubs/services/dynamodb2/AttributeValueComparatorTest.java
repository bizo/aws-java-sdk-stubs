package com.bizo.awsstubs.services.dynamodb2;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public class AttributeValueComparatorTest {

  private static final String STRING_VALUE_1 = "Alpha";
  private static final String STRING_VALUE_2 = "Beta";
  private static final int INT_VALUE_1 = 1;
  private static final int INT_VALUE_2 = 2;

  @Before
  public void setUp() {
  }

  @Test
  public void test_equal_Strings() throws Exception {
    AttributeValue attribute1 = new AttributeValue().withS(STRING_VALUE_1);
    AttributeValue attribute2 = new AttributeValue().withS(STRING_VALUE_2);

    boolean result = AttributeValueComparator.equal(attribute1, attribute2);

    assertFalse(result);
  }

  @Test
  public void test_equal_Numbers() throws Exception {
    AttributeValue attribute1 = new AttributeValue().withN(String.valueOf(INT_VALUE_1));
    AttributeValue attribute2 = new AttributeValue().withN(String.valueOf(INT_VALUE_2));

    boolean result = AttributeValueComparator.equal(attribute1, attribute2);

    assertFalse(result);
  }

  @Test
  public void test_lessThan_Strings() throws Exception {
    AttributeValue attribute1 = new AttributeValue().withS(STRING_VALUE_1);
    AttributeValue attribute2 = new AttributeValue().withS(STRING_VALUE_2);

    boolean result = AttributeValueComparator.lessThan(attribute1, attribute2);

    assertTrue(result);
  }

  @Test
  public void test_lessThan_Strings2() throws Exception {
    AttributeValue attribute1 = new AttributeValue().withS(STRING_VALUE_2);
    AttributeValue attribute2 = new AttributeValue().withS(STRING_VALUE_1);

    boolean result = AttributeValueComparator.lessThan(attribute1, attribute2);

    assertFalse(result);
  }

  @Test
  public void test_lessThan_Numbers() throws Exception {
    AttributeValue attribute1 = new AttributeValue().withN(String.valueOf(INT_VALUE_1));
    AttributeValue attribute2 = new AttributeValue().withN(String.valueOf(INT_VALUE_2));

    boolean result = AttributeValueComparator.lessThan(attribute1, attribute2);

    assertTrue(result);
  }

  @Test
  public void test_greaterThan_Strings() throws Exception {
    AttributeValue attribute1 = new AttributeValue().withS(STRING_VALUE_1);
    AttributeValue attribute2 = new AttributeValue().withS(STRING_VALUE_2);

    boolean result = AttributeValueComparator.greaterThan(attribute1, attribute2);

    assertFalse(result);
  }

  @Test
  public void test_greaterThan_Strings2() throws Exception {
    AttributeValue attribute1 = new AttributeValue().withS(STRING_VALUE_2);
    AttributeValue attribute2 = new AttributeValue().withS(STRING_VALUE_1);

    boolean result = AttributeValueComparator.greaterThan(attribute1, attribute2);

    assertTrue(result);
  }

  @Test
  public void test_greaterThan_Numbers() throws Exception {
    AttributeValue attribute1 = new AttributeValue().withN(String.valueOf(INT_VALUE_1));
    AttributeValue attribute2 = new AttributeValue().withN(String.valueOf(INT_VALUE_2));

    boolean result = AttributeValueComparator.greaterThan(attribute1, attribute2);

    assertFalse(result);
  }

  @Test
  public void test_lessThanEqual_Strings() throws Exception {
    AttributeValue attribute1 = new AttributeValue().withS(STRING_VALUE_1);
    AttributeValue attribute2 = new AttributeValue().withS(STRING_VALUE_2);

    boolean result = AttributeValueComparator.lessThanEqual(attribute1, attribute2);

    assertTrue(result);
  }

  @Test
  public void test_lessThanEqual_Numbers() throws Exception {
    AttributeValue attribute1 = new AttributeValue().withN(String.valueOf(INT_VALUE_1));
    AttributeValue attribute2 = new AttributeValue().withN(String.valueOf(INT_VALUE_2));

    boolean result = AttributeValueComparator.lessThanEqual(attribute1, attribute2);

    assertTrue(result);
  }

  @Test
  public void test_greaterThanEqual_Strings() throws Exception {
    AttributeValue attribute1 = new AttributeValue().withS(STRING_VALUE_1);
    AttributeValue attribute2 = new AttributeValue().withS(STRING_VALUE_2);

    boolean result = AttributeValueComparator.greaterThanEqual(attribute1, attribute2);

    assertFalse(result);
  }

  @Test
  public void test_greaterThanEqual_Numbers() throws Exception {
    AttributeValue attribute1 = new AttributeValue().withN(String.valueOf(INT_VALUE_1));
    AttributeValue attribute2 = new AttributeValue().withN(String.valueOf(INT_VALUE_2));

    boolean result = AttributeValueComparator.greaterThanEqual(attribute1, attribute2);

    assertFalse(result);
  }
}
