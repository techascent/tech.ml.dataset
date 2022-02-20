package uk.ac.bristol.star.feather;

/**
 * Reads a value from a given slot of a feather column.
 * The <code>getObject(int)</code> method will supply a typed value;
 * multiple other primitive-typed methods are provided so that object
 * creation can be avoided if the primitive type is known, but
 * note that not all of these will give sensible results for all column types.
 *
 * @author   Mark Taylor
 * @since    26 Feb 2020
 */
public interface Reader<T> {

    /**
     * Returns the class of object that will be returned by the
     * <code>getObject</code> method.
     *
     * @return  value class
     */
    Class<T> getValueClass();

    /**
     * Indicates whether a given row is flagged as null by a validity mask
     * associated with the column.
     *
     * @param  ix  row index
     * @return  true for null, false for non-null
     */
    boolean isNull( long ix );

    /**
     * Returns a typed object value for a given row.
     * This may be a primitive wrapper object.
     * It should always return a sensible result. 
     *
     * @param  ix  row index
     * @return  value as a typed object
     */
    T getObject( long ix );

    /**
     * Returns a byte view of the value for a given row.
     * May or may not make sense, depending on column type.
     *
     * @param  ix  row index
     * @return  value as byte
     */
    byte getByte( long ix );

    /**
     * Returns a short integer view of the value for a given row.
     * May or may not make sense, depending on column type.
     *
     * @param  ix  row index
     * @return  value as short
     */
    short getShort( long ix );

    /**
     * Returns an integer view of the value for a given row.
     * May or may not make sense, depending on column type.
     *
     * @param  ix  row index
     * @return  value as int
     */
    int getInt( long ix );

    /**
     * Returns a long integer view of the value for a given row.
     * May or may not make sense, depending on column type.
     *
     * @param  ix  row index
     * @return  value as long
     */
    long getLong( long ix );

    /**
     * Returns a float view of the value for a given row.
     * May or may not make sense, depending on column type.
     *
     * @param  ix  row index
     * @return  value as float
     */
    float getFloat( long ix );

    /**
     * Returns a double view of the value for a given row.
     * May or may not make sense, depending on column type.
     *
     * @param  ix  row index
     * @return  value as double
     */
    double getDouble( long ix );
}
