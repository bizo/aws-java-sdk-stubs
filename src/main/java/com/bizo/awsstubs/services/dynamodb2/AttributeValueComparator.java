package com.bizo.awsstubs.services.dynamodb2;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

public class AttributeValueComparator {

  private AttributeValueComparator() {
  }

  // attribute1 == attribute2
  public static boolean equal(AttributeValue attribute1, AttributeValue attribute2) {
    if (attribute1 == attribute2)
      return true;
    if ((attribute1 == null) || (attribute2 == null))
        return false;

    if (attribute2.getS() == null ^ attribute1.getS() == null)
        return false;
    if (attribute2.getS() != null && attribute2.getS().equals(attribute1.getS()) == false)
        return false;
    if (attribute2.getN() == null ^ attribute1.getN() == null)
        return false;
    if (attribute2.getN() != null && attribute2.getN().equals(attribute1.getN()) == false)
        return false;
    if (attribute2.getB() == null ^ attribute1.getB() == null)
        return false;
    if (attribute2.getB() != null && attribute2.getB().equals(attribute1.getB()) == false)
        return false;
    if (attribute2.getSS() == null ^ attribute1.getSS() == null)
        return false;
    if (attribute2.getSS() != null && attribute2.getSS().equals(attribute1.getSS()) == false)
        return false;
    if (attribute2.getNS() == null ^ attribute1.getNS() == null)
        return false;
    if (attribute2.getNS() != null && attribute2.getNS().equals(attribute1.getNS()) == false)
        return false;
    if (attribute2.getBS() == null ^ attribute1.getBS() == null)
        return false;
    if (attribute2.getBS() != null && attribute2.getBS().equals(attribute1.getBS()) == false)
        return false;
    if (attribute2.getM() == null ^ attribute1.getM() == null)
        return false;
    if (attribute2.getM() != null && attribute2.getM().equals(attribute1.getM()) == false)
        return false;
    if (attribute2.getL() == null ^ attribute1.getL() == null)
        return false;
    if (attribute2.getL() != null && attribute2.getL().equals(attribute1.getL()) == false)
        return false;
    if (attribute2.getNULL() == null ^ attribute1.getNULL() == null)
        return false;
    if (attribute2.getNULL() != null && attribute2.getNULL().equals(attribute1.getNULL()) == false)
        return false;
    if (attribute2.getBOOL() == null ^ attribute1.getBOOL() == null)
        return false;
    if (attribute2.getBOOL() != null && attribute2.getBOOL().equals(attribute1.getBOOL()) == false)
        return false;
    return true;
  }

  // attribute1 < attribute2
  public static boolean lessThan(AttributeValue attribute1, AttributeValue attribute2) {
    if ((attribute1 == null) || (attribute2 == null))
      throw new NullAttributeValueException("Both AttributeValue instances must be non-null.");
    if (attribute1 == attribute2)
      return false;

    if (attribute2.getS() == null ^ attribute1.getS() == null)
        return false;
    if (attribute2.getS() != null && attribute2.getS().compareTo(attribute1.getS()) < 0)
        return false;
    if (attribute2.getN() == null ^ attribute1.getN() == null)
        return false;
    if (attribute2.getN() != null && attribute2.getN().compareTo(attribute1.getN()) < 0)
        return false;
    if (attribute2.getB() == null ^ attribute1.getB() == null)
        return false;
    if (attribute2.getB() != null && attribute2.getB().compareTo(attribute1.getB()) < 0)
        return false;
//    if (attribute2.getSS() == null ^ attribute1.getSS() == null)
//        return false;
//    if (attribute2.getSS() != null && attribute2.getSS().compareTo(attribute1.getSS()) < 0)
//        return false;
//    if (attribute2.getNS() == null ^ attribute1.getNS() == null)
//        return false;
//    if (attribute2.getNS() != null && attribute2.getNS().compareTo(attribute1.getNS()) < 0)
//        return false;
//    if (attribute2.getBS() == null ^ attribute1.getBS() == null)
//        return false;
//    if (attribute2.getBS() != null && attribute2.getBS().compareTo(attribute1.getBS()) < 0)
//        return false;
//    if (attribute2.getM() == null ^ attribute1.getM() == null)
//        return false;
//    if (attribute2.getM() != null && attribute2.getM().compareTo(attribute1.getM()) < 0)
//        return false;
//    if (attribute2.getL() == null ^ attribute1.getL() == null)
//        return false;
//    if (attribute2.getL() != null && attribute2.getL().compareTo(attribute1.getL()) < 0)
//        return false;
    if (attribute2.getNULL() == null ^ attribute1.getNULL() == null)
        return false;
    if (attribute2.getNULL() != null && attribute2.getNULL().compareTo(attribute1.getNULL()) < 0)
        return false;
    if (attribute2.getBOOL() == null ^ attribute1.getBOOL() == null)
        return false;
    if (attribute2.getBOOL() != null && attribute2.getBOOL().compareTo(attribute1.getBOOL()) < 0)
        return false;
    return true;
  }

  // attribute1 > attribute2
  public static boolean greaterThan(AttributeValue attribute1, AttributeValue attribute2) {
    if ((attribute1 == null) || (attribute2 == null))
      throw new NullAttributeValueException("Both AttributeValue instances must be non-null.");
    if (attribute1 == attribute2)
      return false;

    if (attribute2.getS() == null ^ attribute1.getS() == null)
        return false;
    if (attribute2.getS() != null && attribute2.getS().compareTo(attribute1.getS()) > 0)
        return false;
    if (attribute2.getN() == null ^ attribute1.getN() == null)
        return false;
    if (attribute2.getN() != null && attribute2.getN().compareTo(attribute1.getN()) > 0)
        return false;
    if (attribute2.getB() == null ^ attribute1.getB() == null)
        return false;
    if (attribute2.getB() != null && attribute2.getB().compareTo(attribute1.getB()) > 0)
        return false;
//    if (attribute2.getSS() == null ^ attribute1.getSS() == null)
//        return false;
//    if (attribute2.getSS() != null && attribute2.getSS().compareTo(attribute1.getSS()) > 0)
//        return false;
//    if (attribute2.getNS() == null ^ attribute1.getNS() == null)
//        return false;
//    if (attribute2.getNS() != null && attribute2.getNS().compareTo(attribute1.getNS()) > 0)
//        return false;
//    if (attribute2.getBS() == null ^ attribute1.getBS() == null)
//        return false;
//    if (attribute2.getBS() != null && attribute2.getBS().compareTo(attribute1.getBS()) > 0)
//        return false;
//    if (attribute2.getM() == null ^ attribute1.getM() == null)
//        return false;
//    if (attribute2.getM() != null && attribute2.getM().compareTo(attribute1.getM()) > 0)
//        return false;
//    if (attribute2.getL() == null ^ attribute1.getL() == null)
//        return false;
//    if (attribute2.getL() != null && attribute2.getL().compareTo(attribute1.getL()) > 0)
//        return false;
    if (attribute2.getNULL() == null ^ attribute1.getNULL() == null)
        return false;
    if (attribute2.getNULL() != null && attribute2.getNULL().compareTo(attribute1.getNULL()) > 0)
        return false;
    if (attribute2.getBOOL() == null ^ attribute1.getBOOL() == null)
        return false;
    if (attribute2.getBOOL() != null && attribute2.getBOOL().compareTo(attribute1.getBOOL()) > 0)
        return false;
    return true;
  }

  // attribute1 <= attribute2
  public static boolean lessThanEqual(AttributeValue attribute1, AttributeValue attribute2) {
    if (attribute1 == attribute2)
      return true;
    if ((attribute1 == null) || (attribute2 == null))
      throw new NullAttributeValueException("Both AttributeValue instances must be non-null.");

    if (attribute2.getS() == null ^ attribute1.getS() == null)
        return false;
    if (attribute2.getS() != null && attribute2.getS().compareTo(attribute1.getS()) <= 0)
        return false;
    if (attribute2.getN() == null ^ attribute1.getN() == null)
        return false;
    if (attribute2.getN() != null && attribute2.getN().compareTo(attribute1.getN()) <= 0)
        return false;
    if (attribute2.getB() == null ^ attribute1.getB() == null)
        return false;
    if (attribute2.getB() != null && attribute2.getB().compareTo(attribute1.getB()) <= 0)
        return false;
//    if (attribute2.getSS() == null ^ attribute1.getSS() == null)
//        return false;
//    if (attribute2.getSS() != null && attribute2.getSS().compareTo(attribute1.getSS()) <= 0)
//        return false;
//    if (attribute2.getNS() == null ^ attribute1.getNS() == null)
//        return false;
//    if (attribute2.getNS() != null && attribute2.getNS().compareTo(attribute1.getNS()) <= 0)
//        return false;
//    if (attribute2.getBS() == null ^ attribute1.getBS() == null)
//        return false;
//    if (attribute2.getBS() != null && attribute2.getBS().compareTo(attribute1.getBS()) <= 0)
//        return false;
//    if (attribute2.getM() == null ^ attribute1.getM() == null)
//        return false;
//    if (attribute2.getM() != null && attribute2.getM().compareTo(attribute1.getM()) <= 0)
//        return false;
//    if (attribute2.getL() == null ^ attribute1.getL() == null)
//        return false;
//    if (attribute2.getL() != null && attribute2.getL().compareTo(attribute1.getL()) <= 0)
//        return false;
    if (attribute2.getNULL() == null ^ attribute1.getNULL() == null)
        return false;
    if (attribute2.getNULL() != null && attribute2.getNULL().compareTo(attribute1.getNULL()) <= 0)
        return false;
    if (attribute2.getBOOL() == null ^ attribute1.getBOOL() == null)
        return false;
    if (attribute2.getBOOL() != null && attribute2.getBOOL().compareTo(attribute1.getBOOL()) <= 0)
        return false;
    return true;
  }

  // attribute1 >= attribute2
  public static boolean greaterThanEqual(AttributeValue attribute1, AttributeValue attribute2) {
    if (attribute1 == attribute2)
      return true;
    if ((attribute1 == null) || (attribute2 == null))
      throw new NullAttributeValueException("Both AttributeValue instances must be non-null.");

    if (attribute2.getS() == null ^ attribute1.getS() == null)
        return false;
    if (attribute2.getS() != null && attribute2.getS().compareTo(attribute1.getS()) >= 0)
        return false;
    if (attribute2.getN() == null ^ attribute1.getN() == null)
        return false;
    if (attribute2.getN() != null && attribute2.getN().compareTo(attribute1.getN()) >= 0)
        return false;
    if (attribute2.getB() == null ^ attribute1.getB() == null)
        return false;
    if (attribute2.getB() != null && attribute2.getB().compareTo(attribute1.getB()) >= 0)
        return false;
//    if (attribute2.getSS() == null ^ attribute1.getSS() == null)
//        return false;
//    if (attribute2.getSS() != null && attribute2.getSS().compareTo(attribute1.getSS()) >= 0)
//        return false;
//    if (attribute2.getNS() == null ^ attribute1.getNS() == null)
//        return false;
//    if (attribute2.getNS() != null && attribute2.getNS().compareTo(attribute1.getNS()) >= 0)
//        return false;
//    if (attribute2.getBS() == null ^ attribute1.getBS() == null)
//        return false;
//    if (attribute2.getBS() != null && attribute2.getBS().compareTo(attribute1.getBS()) >= 0)
//        return false;
//    if (attribute2.getM() == null ^ attribute1.getM() == null)
//        return false;
//    if (attribute2.getM() != null && attribute2.getM().compareTo(attribute1.getM()) >= 0)
//        return false;
//    if (attribute2.getL() == null ^ attribute1.getL() == null)
//        return false;
//    if (attribute2.getL() != null && attribute2.getL().compareTo(attribute1.getL()) >= 0)
//        return false;
    if (attribute2.getNULL() == null ^ attribute1.getNULL() == null)
        return false;
    if (attribute2.getNULL() != null && attribute2.getNULL().compareTo(attribute1.getNULL()) >= 0)
        return false;
    if (attribute2.getBOOL() == null ^ attribute1.getBOOL() == null)
        return false;
    if (attribute2.getBOOL() != null && attribute2.getBOOL().compareTo(attribute1.getBOOL()) >= 0)
        return false;
    return true;
  }
}
