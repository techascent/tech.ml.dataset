package uk.ac.bristol.star.feather;

import uk.ac.bristol.star.fbs.feather.Type;

/**
 * Enumeration class for Feather-format type constants.
 * This is just a type-safe copy of {@link uk.ac.bristol.star.fbs.feather.Type}.
 *
 * @author   Mark Taylor
 * @since    26 Feb 2020
 */
public class FeatherType {

    private final byte tbyte_;
    private final String name_;

    public static final FeatherType BOOL;
    public static final FeatherType INT8;
    public static final FeatherType INT16;
    public static final FeatherType INT32;
    public static final FeatherType INT64;
    public static final FeatherType UINT8;
    public static final FeatherType UINT16;
    public static final FeatherType UINT32;
    public static final FeatherType UINT64;
    public static final FeatherType FLOAT;
    public static final FeatherType DOUBLE;
    public static final FeatherType UTF8;
    public static final FeatherType BINARY;
    public static final FeatherType CATEGORY;
    public static final FeatherType TIMESTAMP;
    public static final FeatherType DATE;
    public static final FeatherType TIME;
    public static final FeatherType LARGE_UTF8;
    public static final FeatherType LARGE_BINARY;

    static final FeatherType[] ALL_TYPES = {
        BOOL = new FeatherType( Type.BOOL, "BOOL" ),
        INT8 = new FeatherType( Type.INT8, "INT8" ),
        INT16 = new FeatherType( Type.INT16, "INT16" ),
        INT32 = new FeatherType( Type.INT32, "INT32" ),
        INT64 = new FeatherType( Type.INT64, "INT64" ),
        UINT8 = new FeatherType( Type.UINT8, "UINT8" ),
        UINT16 = new FeatherType( Type.UINT16, "UINT16" ),
        UINT32 = new FeatherType( Type.UINT32, "UINT32" ),
        UINT64 = new FeatherType( Type.UINT64, "UINT64" ),
        FLOAT = new FeatherType( Type.FLOAT, "FLOAT" ),
        DOUBLE = new FeatherType( Type.DOUBLE, "DOUBLE" ),
        UTF8 = new FeatherType( Type.UTF8, "UTF8" ),
        BINARY = new FeatherType( Type.BINARY, "BINARY" ),
        CATEGORY = new FeatherType( Type.CATEGORY, "CATEGORY" ),
        TIMESTAMP = new FeatherType( Type.TIMESTAMP, "TIMESTAMP" ),
        DATE = new FeatherType( Type.DATE, "DATE" ),
        TIME = new FeatherType( Type.TIME, "TIME" ),
        LARGE_UTF8 = new FeatherType( Type.LARGE_UTF8, "LARGE_UTF8" ),
        LARGE_BINARY = new FeatherType( Type.LARGE_BINARY, "LARGE_BINARY" ),
    };
 
    /**
     * Constructor.
     *
     * @param   tbyte   type code byte
     */
    private FeatherType( byte tbyte, String name ) {
        tbyte_ = tbyte;
        name_ = name;
    }

    /**
     * Returns the byte value associated with this type.
     *
     * @return  type code byte
     */
    public byte getTypeByte() {
        return tbyte_;
    }

    /**
     * Returns the feathe type instance corresponding to a given byte value.
     *
     * @param  fbyte  byte code
     * @return  feather type, or null if not known
     */
    public static FeatherType fromByte( byte fbyte ) {
        for ( FeatherType ftype : ALL_TYPES ) {
            if ( ftype.getTypeByte() == fbyte ) {
                return ftype;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return name_;
    }
}
