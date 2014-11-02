/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package avro;  
@SuppressWarnings("all")
@org.apache.avro.specific.AvroGenerated
public class ItemLine extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"ItemLine\",\"namespace\":\"avro\",\"fields\":[{\"name\":\"itemId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"default\":\"\"},{\"name\":\"quantity\",\"type\":\"int\",\"default\":0}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  @Deprecated public java.lang.String itemId;
  @Deprecated public int quantity;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>. 
   */
  public ItemLine() {}

  /**
   * All-args constructor.
   */
  public ItemLine(java.lang.String itemId, java.lang.Integer quantity) {
    this.itemId = itemId;
    this.quantity = quantity;
  }

  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call. 
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return itemId;
    case 1: return quantity;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }
  // Used by DatumReader.  Applications should not call. 
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: itemId = (java.lang.String)value$; break;
    case 1: quantity = (java.lang.Integer)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'itemId' field.
   */
  public java.lang.String getItemId() {
    return itemId;
  }

  /**
   * Sets the value of the 'itemId' field.
   * @param value the value to set.
   */
  public void setItemId(java.lang.String value) {
    this.itemId = value;
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

  /** Creates a new ItemLine RecordBuilder */
  public static avro.ItemLine.Builder newBuilder() {
    return new avro.ItemLine.Builder();
  }
  
  /** Creates a new ItemLine RecordBuilder by copying an existing Builder */
  public static avro.ItemLine.Builder newBuilder(avro.ItemLine.Builder other) {
    return new avro.ItemLine.Builder(other);
  }
  
  /** Creates a new ItemLine RecordBuilder by copying an existing ItemLine instance */
  public static avro.ItemLine.Builder newBuilder(avro.ItemLine other) {
    return new avro.ItemLine.Builder(other);
  }
  
  /**
   * RecordBuilder for ItemLine instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<ItemLine>
    implements org.apache.avro.data.RecordBuilder<ItemLine> {

    private java.lang.String itemId;
    private int quantity;

    /** Creates a new Builder */
    private Builder() {
      super(avro.ItemLine.SCHEMA$);
    }
    
    /** Creates a Builder by copying an existing Builder */
    private Builder(avro.ItemLine.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.itemId)) {
        this.itemId = (String) data().deepCopy(fields()[0].schema(), other.itemId);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.quantity)) {
        this.quantity = (Integer) data().deepCopy(fields()[1].schema(), other.quantity);
        fieldSetFlags()[1] = true;
      }
    }
    
    /** Creates a Builder by copying an existing ItemLine instance */
    private Builder(avro.ItemLine other) {
            super(avro.ItemLine.SCHEMA$);
      if (isValidValue(fields()[0], other.itemId)) {
        this.itemId = (String) data().deepCopy(fields()[0].schema(), other.itemId);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.quantity)) {
        this.quantity = (Integer) data().deepCopy(fields()[1].schema(), other.quantity);
        fieldSetFlags()[1] = true;
      }
    }

    /** Gets the value of the 'itemId' field */
    public java.lang.String getItemId() {
      return itemId;
    }
    
    /** Sets the value of the 'itemId' field */
    public avro.ItemLine.Builder setItemId(java.lang.String value) {
      validate(fields()[0], value);
      this.itemId = value;
      fieldSetFlags()[0] = true;
      return this; 
    }
    
    /** Checks whether the 'itemId' field has been set */
    public boolean hasItemId() {
      return fieldSetFlags()[0];
    }
    
    /** Clears the value of the 'itemId' field */
    public avro.ItemLine.Builder clearItemId() {
      itemId = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /** Gets the value of the 'quantity' field */
    public java.lang.Integer getQuantity() {
      return quantity;
    }
    
    /** Sets the value of the 'quantity' field */
    public avro.ItemLine.Builder setQuantity(int value) {
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
    public avro.ItemLine.Builder clearQuantity() {
      fieldSetFlags()[1] = false;
      return this;
    }

    @Override
    public ItemLine build() {
      try {
        ItemLine record = new ItemLine();
        record.itemId = fieldSetFlags()[0] ? this.itemId : (java.lang.String) defaultValue(fields()[0]);
        record.quantity = fieldSetFlags()[1] ? this.quantity : (java.lang.Integer) defaultValue(fields()[1]);
        return record;
      } catch (Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }
}
