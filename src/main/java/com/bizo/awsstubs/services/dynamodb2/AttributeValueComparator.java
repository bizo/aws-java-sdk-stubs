package com.bizo.awsstubs.services.dynamodb2;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public class AttributeValueComparator {

  private AttributeValueComparator() {
  }

  // value == comparisonValue
  //  true: 0
  public static boolean equal(AttributeValue value, AttributeValue comparisonValue) {
    if (value == comparisonValue)
      return true;
    if ((value == null) || (comparisonValue == null))
        return false;

    if (value.getS() == null ^ comparisonValue.getS() == null)
        return false;
    if (value.getS() != null && value.getS().equals(comparisonValue.getS()) == false)
        return false;

    if (value.getN() == null ^ value.getN() == null)
        return false;
    if (value.getN() != null && value.getN().equals(comparisonValue.getN()) == false)
        return false;

    if (value.getB() == null ^ value.getB() == null)
        return false;
    if (value.getB() != null && value.getB().equals(comparisonValue.getB()) == false)
        return false;

    if (value.getSS() == null ^ value.getSS() == null)
        return false;
    if (value.getSS() != null && value.getSS().equals(comparisonValue.getSS()) == false)
        return false;

    if (value.getNS() == null ^ value.getNS() == null)
        return false;
    if (value.getNS() != null && value.getNS().equals(comparisonValue.getNS()) == false)
        return false;

    if (value.getBS() == null ^ value.getBS() == null)
        return false;
    if (value.getBS() != null && value.getBS().equals(comparisonValue.getBS()) == false)
        return false;

    if (value.getM() == null ^ value.getM() == null)
        return false;
    if (value.getM() != null && value.getM().equals(comparisonValue.getM()) == false)
        return false;

    if (value.getL() == null ^ value.getL() == null)
        return false;
    if (value.getL() != null && value.getL().equals(comparisonValue.getL()) == false)
        return false;

    if (value.getNULL() == null ^ value.getNULL() == null)
        return false;
    if (value.getNULL() != null && value.getNULL().equals(comparisonValue.getNULL()) == false)
        return false;

    if (value.getBOOL() == null ^ value.getBOOL() == null)
        return false;
    if (value.getBOOL() != null && value.getBOOL().equals(comparisonValue.getBOOL()) == false)
        return false;

    return true;
  }

  // value < comparisonValue
  //  true: -1
  public static boolean lessThan(AttributeValue value, AttributeValue comparisonValue) {
    if ((value == null) || (comparisonValue == null))
      throw new NullAttributeValueException("Both AttributeValue instances must be non-null.");
    if (value == comparisonValue)
      return false;

    if (value.getS() == null ^ value.getS() == null)
        return false;
    if (value.getS() != null && value.getS().compareTo(comparisonValue.getS()) < 0)
        return true;

    if (value.getN() == null ^ value.getN() == null)
        return false;
    if (value.getN() != null && value.getN().compareTo(comparisonValue.getN()) < 0)
        return true;

    if (value.getB() == null ^ value.getB() == null)
        return false;
    if (value.getB() != null && value.getB().compareTo(comparisonValue.getB()) < 0)
        return true;

//    if (value.getSS() == null ^ comparisonValue.getSS() == null)
//        return false;
//    if (value.getSS() != null && value.getSS().compareTo(comparisonValue.getSS()) < 0)
//        return true;

//    if (value.getNS() == null ^ comparisonValue.getNS() == null)
//        return false;
//    if (value.getNS() != null && value.getNS().compareTo(comparisonValue.getNS()) < 0)
//        return true;

//    if (value.getBS() == null ^ comparisonValue.getBS() == null)
//        return false;
//    if (value.getBS() != null && value.getBS().compareTo(comparisonValue.getBS()) < 0)
//        return true;

//    if (value.getM() == null ^ comparisonValue.getM() == null)
//        return false;
//    if (value.getM() != null && value.getM().compareTo(comparisonValue.getM()) < 0)
//        return true;

//    if (value.getL() == null ^ comparisonValue.getL() == null)
//        return false;
//    if (value.getL() != null && value.getL().compareTo(comparisonValue.getL()) < 0)
//        return true;

    if (value.getNULL() == null ^ value.getNULL() == null)
        return false;
    if (value.getNULL() != null && value.getNULL().compareTo(comparisonValue.getNULL()) < 0)
        return true;

    if (value.getBOOL() == null ^ value.getBOOL() == null)
        return false;
    if (value.getBOOL() != null && value.getBOOL().compareTo(comparisonValue.getBOOL()) < 0)
        return true;

    return false;
  }

  // value > comparisonValue
  //  true: + 1
  public static boolean greaterThan(AttributeValue value, AttributeValue comparisonValue) {
    if ((value == null) || (comparisonValue == null))
      throw new NullAttributeValueException("Both AttributeValue instances must be non-null.");
    if (value == comparisonValue)
      return false;

    if (value.getS() == null ^ value.getS() == null)
        return false;
    if (value.getS() != null && value.getS().compareTo(comparisonValue.getS()) > 0)
        return true;

    if (value.getN() == null ^ value.getN() == null)
        return false;
    if (value.getN() != null && value.getN().compareTo(comparisonValue.getN()) > 0)
        return true;

    if (value.getB() == null ^ value.getB() == null)
        return false;
    if (value.getB() != null && value.getB().compareTo(comparisonValue.getB()) > 0)
        return true;

//    if (value.getSS() == null ^ comparisonValue.getSS() == null)
//        return false;
//    if (value.getSS() != null && value.getSS().compareTo(comparisonValue.getSS()) > 0)
//        return true;

//    if (value.getNS() == null ^ comparisonValue.getNS() == null)
//        return false;
//    if (value.getNS() != null && value.getNS().compareTo(comparisonValue.getNS()) > 0)
//        return true;

//    if (value.getBS() == null ^ comparisonValue.getBS() == null)
//        return false;
//    if (value.getBS() != null && value.getBS().compareTo(comparisonValue.getBS()) > 0)
//        return true;

//    if (value.getM() == null ^ comparisonValue.getM() == null)
//        return false;
//    if (value.getM() != null && value.getM().compareTo(comparisonValue.getM()) > 0)
//        return true;

//    if (value.getL() == null ^ comparisonValue.getL() == null)
//        return false;
//    if (value.getL() != null && value.getL().compareTo(comparisonValue.getL()) > 0)
//        return true;

    if (value.getNULL() == null ^ value.getNULL() == null)
        return false;
    if (value.getNULL() != null && value.getNULL().compareTo(comparisonValue.getNULL()) > 0)
        return true;

    if (value.getBOOL() == null ^ value.getBOOL() == null)
        return false;
    if (value.getBOOL() != null && value.getBOOL().compareTo(comparisonValue.getBOOL()) > 0)
        return true;

    return false;
  }

  // value <= comparisonValue
  //  true: -1, 0
  public static boolean lessThanEqual(AttributeValue value, AttributeValue comparisonValue) {
    if (value == comparisonValue)
      return true;
    if ((value == null) || (comparisonValue == null))
      throw new NullAttributeValueException("Both AttributeValue instances must be non-null.");

    if (value.getS() == null ^ value.getS() == null)
        return false;
    if (value.getS() != null && value.getS().compareTo(comparisonValue.getS()) <= 0)
        return true;

    if (value.getN() == null ^ value.getN() == null)
        return false;
    if (value.getN() != null && value.getN().compareTo(comparisonValue.getN()) <= 0)
        return true;

    if (value.getB() == null ^ value.getB() == null)
        return false;
    if (value.getB() != null && value.getB().compareTo(comparisonValue.getB()) <= 0)
        return true;

//    if (value.getSS() == null ^ comparisonValue.getSS() == null)
//        return false;
//    if (value.getSS() != null && value.getSS().compareTo(comparisonValue.getSS()) <= 0)
//        return true;

//    if (value.getNS() == null ^ comparisonValue.getNS() == null)
//        return false;
//    if (value.getNS() != null && value.getNS().compareTo(comparisonValue.getNS()) <= 0)
//        return true;

//    if (value.getBS() == null ^ comparisonValue.getBS() == null)
//        return false;
//    if (value.getBS() != null && value.getBS().compareTo(comparisonValue.getBS()) <= 0)
//        return true;

//    if (value.getM() == null ^ comparisonValue.getM() == null)
//        return false;
//    if (value.getM() != null && value.getM().compareTo(comparisonValue.getM()) <= 0)
//        return true;

//    if (value.getL() == null ^ comparisonValue.getL() == null)
//        return false;
//    if (value.getL() != null && value.getL().compareTo(comparisonValue.getL()) <= 0)
//        return true;

    if (value.getNULL() == null ^ value.getNULL() == null)
        return false;
    if (value.getNULL() != null && value.getNULL().compareTo(comparisonValue.getNULL()) <= 0)
        return true;

    if (value.getBOOL() == null ^ value.getBOOL() == null)
        return false;
    if (value.getBOOL() != null && value.getBOOL().compareTo(comparisonValue.getBOOL()) <= 0)
        return true;

    return false;
  }

  // value >= comparisonValue
  //  true: + 1, 0
  public static boolean greaterThanEqual(AttributeValue value, AttributeValue comparisonValue) {
    if (value == comparisonValue)
      return true;
    if ((value == null) || (comparisonValue == null))
      throw new NullAttributeValueException("Both AttributeValue instances must be non-null.");

    if (value.getS() == null ^ value.getS() == null)
        return false;
    if (value.getS() != null && value.getS().compareTo(comparisonValue.getS()) >= 0)
        return true;

    if (value.getN() == null ^ value.getN() == null)
        return false;
    if (value.getN() != null && value.getN().compareTo(comparisonValue.getN()) >= 0)
        return true;

    if (value.getB() == null ^ value.getB() == null)
        return false;
    if (value.getB() != null && value.getB().compareTo(comparisonValue.getB()) >= 0)
        return true;

//    if (value.getSS() == null ^ comparisonValue.getSS() == null)
//        return false;
//    if (value.getSS() != null && value.getSS().compareTo(comparisonValue.getSS()) >= 0)
//        return true;

//    if (value.getNS() == null ^ comparisonValue.getNS() == null)
//        return false;
//    if (value.getNS() != null && value.getNS().compareTo(comparisonValue.getNS()) >= 0)
//        return true;

//    if (value.getBS() == null ^ comparisonValue.getBS() == null)
//        return false;
//    if (value.getBS() != null && value.getBS().compareTo(comparisonValue.getBS()) >= 0)
//        return true;

//    if (value.getM() == null ^ comparisonValue.getM() == null)
//        return false;
//    if (value.getM() != null && value.getM().compareTo(comparisonValue.getM()) >= 0)
//        return true;

//    if (value.getL() == null ^ comparisonValue.getL() == null)
//        return false;
//    if (value.getL() != null && value.getL().compareTo(comparisonValue.getL()) >= 0)
//        return true;

    if (value.getNULL() == null ^ value.getNULL() == null)
        return false;
    if (value.getNULL() != null && value.getNULL().compareTo(comparisonValue.getNULL()) >= 0)
        return true;

    if (value.getBOOL() == null ^ value.getBOOL() == null)
        return false;
    if (value.getBOOL() != null && value.getBOOL().compareTo(comparisonValue.getBOOL()) >= 0)
        return true;

    return false;
  }
}
