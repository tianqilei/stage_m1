/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package avro;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class OrderQuantity extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"OrderQuantity\",\"namespace\":\"avro\",\"fields\":[{\"name\":\"orderid\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"default\":\"\"},{\"name\":\"quantity\",\"type\":\"int\",\"default\":0}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.String orderid;
  @Deprecated public int quantity;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public OrderQuantity() {}

  /**
   * All-args constructor.
   */
  public OrderQuantity(java.lang.String orderid, java.lang.Integer quantity) {
    this.orderid = orderid;
    this.quantity = quantity;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return orderid;
    case 1: return quantity;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: orderid = (java.lang.String)value$; break;
    case 1: quantity = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'orderid' field.
   */
  public java.lang.String getOrderid() {
    return orderid;
  }

  /**
   * Sets the value of the 'orderid' field.
   * @param value the value to set.
   */
  public void setOrderid(java.lang.String value) {
    this.orderid = value;
  }

  /**
   * Gets the value of the 'quantity' field.
   */
  public java.lang.Integer getQuantity() {
    return quantity;
  }

  /**
   * Sets the value of the 'quantity' field.
   * @param value the value to set.
   */
  public void setQuantity(java.lang.Integer value) {
    this.quantity = value;
  }

  /** Creates a new OrderQuantity RecordBuilder */
  public static avro.OrderQuantity.Builder newBuilder() {
    return new avro.OrderQuantity.Builder();
  }
  
  /** Creates a new OrderQuantity RecordBuilder by copying an existing Builder */
  public static avro.OrderQuantity.Builder newBuilder(avro.OrderQuantity.Builder other) {
    return new avro.OrderQuantity.Builder(other);
  }
  
  /** Creates a new OrderQuantity RecordBuilder by copying an existing OrderQuantity instance */
  public static avro.OrderQuantity.Builder newBuilder(avro.OrderQuantity other) {
    return new avro.OrderQuantity.Builder(other);
  }
  
  /**
   * RecordBuilder for OrderQuantity instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<OrderQuantity>
    implements org.apache.avro.data.RecordBuilder<OrderQuantity> {

    private java.lang.String orderid;
    private int quantity;

    /** Creates a new Builder */
    private Builder() {
      super(avro.OrderQuantity.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(avro.OrderQuantity.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.orderid)) {
        this.orderid = (String) data().deepCopy(fields()[0].schema(), other.orderid);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.quantity)) {
        this.quantity = (Integer) data().deepCopy(fields()[1].schema(), other.quantity);
        fieldSetFlags()[1] = true;
      }
    }
    
    /** Creates a Builder by copying an existing OrderQuantity instance */
    private Builder(avro.OrderQuantity other) {
            super(avro.OrderQuantity.SCHEMA$);
      if (isValidValue(fields()[0], other.orderid)) {
        this.orderid = (String) data().deepCopy(fields()[0].schema(), other.orderid);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.quantity)) {
        this.quantity = (Integer) data().deepCopy(fields()[1].schema(), other.quantity);
        fieldSetFlags()[1] = true;
      }
    }

    /** Gets the value of the 'orderid' field */
    public java.lang.String getOrderid() {
      return orderid;
    }
    
    /** Sets the value of the 'orderid' field */
    public avro.OrderQuantity.Builder setOrderid(java.lang.String value) {
      validate(fields()[0], value);
      this.orderid = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'orderid' field has been set */
    public boolean hasOrderid() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'orderid' field */
    public avro.OrderQuantity.Builder clearOrderid() {
      orderid = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'quantity' field */
    public java.lang.Integer getQuantity() {
      return quantity;
    }
    
    /** Sets the value of the 'quantity' field */
    public avro.OrderQuantity.Builder setQuantity(int value) {
      validate(fields()[1], value);
      this.quantity = value;
      fieldSetFlags()[1] = true;
      return this; 
    }
    
    /** Checks whether the 'quantity' field has been set */
    public boolean hasQuantity() {
      return fieldSetFlags()[1];
    }
    
    /** Clears the value of the 'quantity' field */
    public avro.OrderQuantity.Builder clearQuantity() {
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public OrderQuantity build() {
      try {
        OrderQuantity record = new OrderQuantity();
        record.orderid = fieldSetFlags()[0] ? this.orderid : (java.lang.String) defaultValue(fields()[0]);
        record.quantity = fieldSetFlags()[1] ? this.quantity : (java.lang.Integer) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
