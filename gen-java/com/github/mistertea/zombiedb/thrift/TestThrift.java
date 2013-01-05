/**
 * Autogenerated by Thrift Compiler (0.9.0-dev)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.github.mistertea.zombiedb.thrift;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestThrift implements org.apache.thrift.TBase<TestThrift, TestThrift._Fields>, java.io.Serializable, Cloneable {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("TestThrift");

  private static final org.apache.thrift.protocol.TField ID_FIELD_DESC = new org.apache.thrift.protocol.TField("id", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField I_FIELD_DESC = new org.apache.thrift.protocol.TField("i", org.apache.thrift.protocol.TType.I32, (short)2);
  private static final org.apache.thrift.protocol.TField L_FIELD_DESC = new org.apache.thrift.protocol.TField("l", org.apache.thrift.protocol.TType.I64, (short)3);
  private static final org.apache.thrift.protocol.TField B_FIELD_DESC = new org.apache.thrift.protocol.TField("b", org.apache.thrift.protocol.TType.BOOL, (short)4);
  private static final org.apache.thrift.protocol.TField BY_FIELD_DESC = new org.apache.thrift.protocol.TField("by", org.apache.thrift.protocol.TType.BYTE, (short)5);
  private static final org.apache.thrift.protocol.TField S_FIELD_DESC = new org.apache.thrift.protocol.TField("s", org.apache.thrift.protocol.TType.I16, (short)6);
  private static final org.apache.thrift.protocol.TField D_FIELD_DESC = new org.apache.thrift.protocol.TField("d", org.apache.thrift.protocol.TType.DOUBLE, (short)7);
  private static final org.apache.thrift.protocol.TField ST_FIELD_DESC = new org.apache.thrift.protocol.TField("st", org.apache.thrift.protocol.TType.STRING, (short)8);
  private static final org.apache.thrift.protocol.TField NOT_INDEXED_STRING_FIELD_DESC = new org.apache.thrift.protocol.TField("notIndexedString", org.apache.thrift.protocol.TType.STRING, (short)17);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new TestThriftStandardSchemeFactory());
    schemes.put(TupleScheme.class, new TestThriftTupleSchemeFactory());
  }

  public String id; // required
  public int i; // required
  public long l; // required
  public boolean b; // required
  public byte by; // required
  public short s; // required
  public double d; // required
  public String st; // required
  public String notIndexedString; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    ID((short)1, "id"),
    I((short)2, "i"),
    L((short)3, "l"),
    B((short)4, "b"),
    BY((short)5, "by"),
    S((short)6, "s"),
    D((short)7, "d"),
    ST((short)8, "st"),
    NOT_INDEXED_STRING((short)17, "notIndexedString");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // ID
          return ID;
        case 2: // I
          return I;
        case 3: // L
          return L;
        case 4: // B
          return B;
        case 5: // BY
          return BY;
        case 6: // S
          return S;
        case 7: // D
          return D;
        case 8: // ST
          return ST;
        case 17: // NOT_INDEXED_STRING
          return NOT_INDEXED_STRING;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final int __I_ISSET_ID = 0;
  private static final int __L_ISSET_ID = 1;
  private static final int __B_ISSET_ID = 2;
  private static final int __BY_ISSET_ID = 3;
  private static final int __S_ISSET_ID = 4;
  private static final int __D_ISSET_ID = 5;
  private byte __isset_bitfield = 0;
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.ID, new org.apache.thrift.meta_data.FieldMetaData("id", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.I, new org.apache.thrift.meta_data.FieldMetaData("i", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32)));
    tmpMap.put(_Fields.L, new org.apache.thrift.meta_data.FieldMetaData("l", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.B, new org.apache.thrift.meta_data.FieldMetaData("b", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BOOL)));
    tmpMap.put(_Fields.BY, new org.apache.thrift.meta_data.FieldMetaData("by", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.BYTE)));
    tmpMap.put(_Fields.S, new org.apache.thrift.meta_data.FieldMetaData("s", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I16)));
    tmpMap.put(_Fields.D, new org.apache.thrift.meta_data.FieldMetaData("d", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE)));
    tmpMap.put(_Fields.ST, new org.apache.thrift.meta_data.FieldMetaData("st", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.NOT_INDEXED_STRING, new org.apache.thrift.meta_data.FieldMetaData("notIndexedString", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(TestThrift.class, metaDataMap);
  }

  public TestThrift() {
  }

  public TestThrift(
    String id,
    int i,
    long l,
    boolean b,
    byte by,
    short s,
    double d,
    String st,
    String notIndexedString)
  {
    this();
    this.id = id;
    this.i = i;
    setIIsSet(true);
    this.l = l;
    setLIsSet(true);
    this.b = b;
    setBIsSet(true);
    this.by = by;
    setByIsSet(true);
    this.s = s;
    setSIsSet(true);
    this.d = d;
    setDIsSet(true);
    this.st = st;
    this.notIndexedString = notIndexedString;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public TestThrift(TestThrift other) {
    __isset_bitfield = other.__isset_bitfield;
    if (other.isSetId()) {
      this.id = other.id;
    }
    this.i = other.i;
    this.l = other.l;
    this.b = other.b;
    this.by = other.by;
    this.s = other.s;
    this.d = other.d;
    if (other.isSetSt()) {
      this.st = other.st;
    }
    if (other.isSetNotIndexedString()) {
      this.notIndexedString = other.notIndexedString;
    }
  }

  public TestThrift deepCopy() {
    return new TestThrift(this);
  }

  @Override
  public void clear() {
    this.id = null;
    setIIsSet(false);
    this.i = 0;
    setLIsSet(false);
    this.l = 0;
    setBIsSet(false);
    this.b = false;
    setByIsSet(false);
    this.by = 0;
    setSIsSet(false);
    this.s = 0;
    setDIsSet(false);
    this.d = 0.0;
    this.st = null;
    this.notIndexedString = null;
  }

  public String getId() {
    return this.id;
  }

  public TestThrift setId(String id) {
    this.id = id;
    return this;
  }

  public void unsetId() {
    this.id = null;
  }

  /** Returns true if field id is set (has been assigned a value) and false otherwise */
  public boolean isSetId() {
    return this.id != null;
  }

  public void setIdIsSet(boolean value) {
    if (!value) {
      this.id = null;
    }
  }

  public int getI() {
    return this.i;
  }

  public TestThrift setI(int i) {
    this.i = i;
    setIIsSet(true);
    return this;
  }

  public void unsetI() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __I_ISSET_ID);
  }

  /** Returns true if field i is set (has been assigned a value) and false otherwise */
  public boolean isSetI() {
    return EncodingUtils.testBit(__isset_bitfield, __I_ISSET_ID);
  }

  public void setIIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __I_ISSET_ID, value);
  }

  public long getL() {
    return this.l;
  }

  public TestThrift setL(long l) {
    this.l = l;
    setLIsSet(true);
    return this;
  }

  public void unsetL() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __L_ISSET_ID);
  }

  /** Returns true if field l is set (has been assigned a value) and false otherwise */
  public boolean isSetL() {
    return EncodingUtils.testBit(__isset_bitfield, __L_ISSET_ID);
  }

  public void setLIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __L_ISSET_ID, value);
  }

  public boolean isB() {
    return this.b;
  }

  public TestThrift setB(boolean b) {
    this.b = b;
    setBIsSet(true);
    return this;
  }

  public void unsetB() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __B_ISSET_ID);
  }

  /** Returns true if field b is set (has been assigned a value) and false otherwise */
  public boolean isSetB() {
    return EncodingUtils.testBit(__isset_bitfield, __B_ISSET_ID);
  }

  public void setBIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __B_ISSET_ID, value);
  }

  public byte getBy() {
    return this.by;
  }

  public TestThrift setBy(byte by) {
    this.by = by;
    setByIsSet(true);
    return this;
  }

  public void unsetBy() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __BY_ISSET_ID);
  }

  /** Returns true if field by is set (has been assigned a value) and false otherwise */
  public boolean isSetBy() {
    return EncodingUtils.testBit(__isset_bitfield, __BY_ISSET_ID);
  }

  public void setByIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __BY_ISSET_ID, value);
  }

  public short getS() {
    return this.s;
  }

  public TestThrift setS(short s) {
    this.s = s;
    setSIsSet(true);
    return this;
  }

  public void unsetS() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __S_ISSET_ID);
  }

  /** Returns true if field s is set (has been assigned a value) and false otherwise */
  public boolean isSetS() {
    return EncodingUtils.testBit(__isset_bitfield, __S_ISSET_ID);
  }

  public void setSIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __S_ISSET_ID, value);
  }

  public double getD() {
    return this.d;
  }

  public TestThrift setD(double d) {
    this.d = d;
    setDIsSet(true);
    return this;
  }

  public void unsetD() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __D_ISSET_ID);
  }

  /** Returns true if field d is set (has been assigned a value) and false otherwise */
  public boolean isSetD() {
    return EncodingUtils.testBit(__isset_bitfield, __D_ISSET_ID);
  }

  public void setDIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __D_ISSET_ID, value);
  }

  public String getSt() {
    return this.st;
  }

  public TestThrift setSt(String st) {
    this.st = st;
    return this;
  }

  public void unsetSt() {
    this.st = null;
  }

  /** Returns true if field st is set (has been assigned a value) and false otherwise */
  public boolean isSetSt() {
    return this.st != null;
  }

  public void setStIsSet(boolean value) {
    if (!value) {
      this.st = null;
    }
  }

  public String getNotIndexedString() {
    return this.notIndexedString;
  }

  public TestThrift setNotIndexedString(String notIndexedString) {
    this.notIndexedString = notIndexedString;
    return this;
  }

  public void unsetNotIndexedString() {
    this.notIndexedString = null;
  }

  /** Returns true if field notIndexedString is set (has been assigned a value) and false otherwise */
  public boolean isSetNotIndexedString() {
    return this.notIndexedString != null;
  }

  public void setNotIndexedStringIsSet(boolean value) {
    if (!value) {
      this.notIndexedString = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case ID:
      if (value == null) {
        unsetId();
      } else {
        setId((String)value);
      }
      break;

    case I:
      if (value == null) {
        unsetI();
      } else {
        setI((Integer)value);
      }
      break;

    case L:
      if (value == null) {
        unsetL();
      } else {
        setL((Long)value);
      }
      break;

    case B:
      if (value == null) {
        unsetB();
      } else {
        setB((Boolean)value);
      }
      break;

    case BY:
      if (value == null) {
        unsetBy();
      } else {
        setBy((Byte)value);
      }
      break;

    case S:
      if (value == null) {
        unsetS();
      } else {
        setS((Short)value);
      }
      break;

    case D:
      if (value == null) {
        unsetD();
      } else {
        setD((Double)value);
      }
      break;

    case ST:
      if (value == null) {
        unsetSt();
      } else {
        setSt((String)value);
      }
      break;

    case NOT_INDEXED_STRING:
      if (value == null) {
        unsetNotIndexedString();
      } else {
        setNotIndexedString((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case ID:
      return getId();

    case I:
      return Integer.valueOf(getI());

    case L:
      return Long.valueOf(getL());

    case B:
      return Boolean.valueOf(isB());

    case BY:
      return Byte.valueOf(getBy());

    case S:
      return Short.valueOf(getS());

    case D:
      return Double.valueOf(getD());

    case ST:
      return getSt();

    case NOT_INDEXED_STRING:
      return getNotIndexedString();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case ID:
      return isSetId();
    case I:
      return isSetI();
    case L:
      return isSetL();
    case B:
      return isSetB();
    case BY:
      return isSetBy();
    case S:
      return isSetS();
    case D:
      return isSetD();
    case ST:
      return isSetSt();
    case NOT_INDEXED_STRING:
      return isSetNotIndexedString();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof TestThrift)
      return this.equals((TestThrift)that);
    return false;
  }

  public boolean equals(TestThrift that) {
    if (that == null)
      return false;

    boolean this_present_id = true && this.isSetId();
    boolean that_present_id = true && that.isSetId();
    if (this_present_id || that_present_id) {
      if (!(this_present_id && that_present_id))
        return false;
      if (!this.id.equals(that.id))
        return false;
    }

    boolean this_present_i = true;
    boolean that_present_i = true;
    if (this_present_i || that_present_i) {
      if (!(this_present_i && that_present_i))
        return false;
      if (this.i != that.i)
        return false;
    }

    boolean this_present_l = true;
    boolean that_present_l = true;
    if (this_present_l || that_present_l) {
      if (!(this_present_l && that_present_l))
        return false;
      if (this.l != that.l)
        return false;
    }

    boolean this_present_b = true;
    boolean that_present_b = true;
    if (this_present_b || that_present_b) {
      if (!(this_present_b && that_present_b))
        return false;
      if (this.b != that.b)
        return false;
    }

    boolean this_present_by = true;
    boolean that_present_by = true;
    if (this_present_by || that_present_by) {
      if (!(this_present_by && that_present_by))
        return false;
      if (this.by != that.by)
        return false;
    }

    boolean this_present_s = true;
    boolean that_present_s = true;
    if (this_present_s || that_present_s) {
      if (!(this_present_s && that_present_s))
        return false;
      if (this.s != that.s)
        return false;
    }

    boolean this_present_d = true;
    boolean that_present_d = true;
    if (this_present_d || that_present_d) {
      if (!(this_present_d && that_present_d))
        return false;
      if (this.d != that.d)
        return false;
    }

    boolean this_present_st = true && this.isSetSt();
    boolean that_present_st = true && that.isSetSt();
    if (this_present_st || that_present_st) {
      if (!(this_present_st && that_present_st))
        return false;
      if (!this.st.equals(that.st))
        return false;
    }

    boolean this_present_notIndexedString = true && this.isSetNotIndexedString();
    boolean that_present_notIndexedString = true && that.isSetNotIndexedString();
    if (this_present_notIndexedString || that_present_notIndexedString) {
      if (!(this_present_notIndexedString && that_present_notIndexedString))
        return false;
      if (!this.notIndexedString.equals(that.notIndexedString))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  public int compareTo(TestThrift other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;
    TestThrift typedOther = (TestThrift)other;

    lastComparison = Boolean.valueOf(isSetId()).compareTo(typedOther.isSetId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.id, typedOther.id);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetI()).compareTo(typedOther.isSetI());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetI()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.i, typedOther.i);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetL()).compareTo(typedOther.isSetL());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetL()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.l, typedOther.l);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetB()).compareTo(typedOther.isSetB());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetB()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.b, typedOther.b);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetBy()).compareTo(typedOther.isSetBy());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetBy()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.by, typedOther.by);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetS()).compareTo(typedOther.isSetS());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetS()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.s, typedOther.s);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetD()).compareTo(typedOther.isSetD());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetD()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.d, typedOther.d);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSt()).compareTo(typedOther.isSetSt());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSt()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.st, typedOther.st);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetNotIndexedString()).compareTo(typedOther.isSetNotIndexedString());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetNotIndexedString()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.notIndexedString, typedOther.notIndexedString);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("TestThrift(");
    boolean first = true;

    sb.append("id:");
    if (this.id == null) {
      sb.append("null");
    } else {
      sb.append(this.id);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("i:");
    sb.append(this.i);
    first = false;
    if (!first) sb.append(", ");
    sb.append("l:");
    sb.append(this.l);
    first = false;
    if (!first) sb.append(", ");
    sb.append("b:");
    sb.append(this.b);
    first = false;
    if (!first) sb.append(", ");
    sb.append("by:");
    sb.append(this.by);
    first = false;
    if (!first) sb.append(", ");
    sb.append("s:");
    sb.append(this.s);
    first = false;
    if (!first) sb.append(", ");
    sb.append("d:");
    sb.append(this.d);
    first = false;
    if (!first) sb.append(", ");
    sb.append("st:");
    if (this.st == null) {
      sb.append("null");
    } else {
      sb.append(this.st);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("notIndexedString:");
    if (this.notIndexedString == null) {
      sb.append("null");
    } else {
      sb.append(this.notIndexedString);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class TestThriftStandardSchemeFactory implements SchemeFactory {
    public TestThriftStandardScheme getScheme() {
      return new TestThriftStandardScheme();
    }
  }

  private static class TestThriftStandardScheme extends StandardScheme<TestThrift> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, TestThrift struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // ID
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.id = iprot.readString();
              struct.setIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // I
            if (schemeField.type == org.apache.thrift.protocol.TType.I32) {
              struct.i = iprot.readI32();
              struct.setIIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // L
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.l = iprot.readI64();
              struct.setLIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // B
            if (schemeField.type == org.apache.thrift.protocol.TType.BOOL) {
              struct.b = iprot.readBool();
              struct.setBIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // BY
            if (schemeField.type == org.apache.thrift.protocol.TType.BYTE) {
              struct.by = iprot.readByte();
              struct.setByIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // S
            if (schemeField.type == org.apache.thrift.protocol.TType.I16) {
              struct.s = iprot.readI16();
              struct.setSIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 7: // D
            if (schemeField.type == org.apache.thrift.protocol.TType.DOUBLE) {
              struct.d = iprot.readDouble();
              struct.setDIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 8: // ST
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.st = iprot.readString();
              struct.setStIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 17: // NOT_INDEXED_STRING
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.notIndexedString = iprot.readString();
              struct.setNotIndexedStringIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, TestThrift struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.id != null) {
        oprot.writeFieldBegin(ID_FIELD_DESC);
        oprot.writeString(struct.id);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldBegin(I_FIELD_DESC);
      oprot.writeI32(struct.i);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(L_FIELD_DESC);
      oprot.writeI64(struct.l);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(B_FIELD_DESC);
      oprot.writeBool(struct.b);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(BY_FIELD_DESC);
      oprot.writeByte(struct.by);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(S_FIELD_DESC);
      oprot.writeI16(struct.s);
      oprot.writeFieldEnd();
      oprot.writeFieldBegin(D_FIELD_DESC);
      oprot.writeDouble(struct.d);
      oprot.writeFieldEnd();
      if (struct.st != null) {
        oprot.writeFieldBegin(ST_FIELD_DESC);
        oprot.writeString(struct.st);
        oprot.writeFieldEnd();
      }
      if (struct.notIndexedString != null) {
        oprot.writeFieldBegin(NOT_INDEXED_STRING_FIELD_DESC);
        oprot.writeString(struct.notIndexedString);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class TestThriftTupleSchemeFactory implements SchemeFactory {
    public TestThriftTupleScheme getScheme() {
      return new TestThriftTupleScheme();
    }
  }

  private static class TestThriftTupleScheme extends TupleScheme<TestThrift> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, TestThrift struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetId()) {
        optionals.set(0);
      }
      if (struct.isSetI()) {
        optionals.set(1);
      }
      if (struct.isSetL()) {
        optionals.set(2);
      }
      if (struct.isSetB()) {
        optionals.set(3);
      }
      if (struct.isSetBy()) {
        optionals.set(4);
      }
      if (struct.isSetS()) {
        optionals.set(5);
      }
      if (struct.isSetD()) {
        optionals.set(6);
      }
      if (struct.isSetSt()) {
        optionals.set(7);
      }
      if (struct.isSetNotIndexedString()) {
        optionals.set(8);
      }
      oprot.writeBitSet(optionals, 9);
      if (struct.isSetId()) {
        oprot.writeString(struct.id);
      }
      if (struct.isSetI()) {
        oprot.writeI32(struct.i);
      }
      if (struct.isSetL()) {
        oprot.writeI64(struct.l);
      }
      if (struct.isSetB()) {
        oprot.writeBool(struct.b);
      }
      if (struct.isSetBy()) {
        oprot.writeByte(struct.by);
      }
      if (struct.isSetS()) {
        oprot.writeI16(struct.s);
      }
      if (struct.isSetD()) {
        oprot.writeDouble(struct.d);
      }
      if (struct.isSetSt()) {
        oprot.writeString(struct.st);
      }
      if (struct.isSetNotIndexedString()) {
        oprot.writeString(struct.notIndexedString);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, TestThrift struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(9);
      if (incoming.get(0)) {
        struct.id = iprot.readString();
        struct.setIdIsSet(true);
      }
      if (incoming.get(1)) {
        struct.i = iprot.readI32();
        struct.setIIsSet(true);
      }
      if (incoming.get(2)) {
        struct.l = iprot.readI64();
        struct.setLIsSet(true);
      }
      if (incoming.get(3)) {
        struct.b = iprot.readBool();
        struct.setBIsSet(true);
      }
      if (incoming.get(4)) {
        struct.by = iprot.readByte();
        struct.setByIsSet(true);
      }
      if (incoming.get(5)) {
        struct.s = iprot.readI16();
        struct.setSIsSet(true);
      }
      if (incoming.get(6)) {
        struct.d = iprot.readDouble();
        struct.setDIsSet(true);
      }
      if (incoming.get(7)) {
        struct.st = iprot.readString();
        struct.setStIsSet(true);
      }
      if (incoming.get(8)) {
        struct.notIndexedString = iprot.readString();
        struct.setNotIndexedStringIsSet(true);
      }
    }
  }

}

