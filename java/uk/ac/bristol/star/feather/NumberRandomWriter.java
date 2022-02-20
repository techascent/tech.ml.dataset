package uk.ac.bristol.star.feather;

import java.io.IOException;
import java.io.OutputStream;

/**
 * FeatherColumnWriter implementations for random access primitive-valued
 * columns.
 *
 * @author  Mark Taylor
 * @since   26 Feb 2020
 */
public abstract class NumberRandomWriter extends AbstractColumnWriter {

    private final long nbyte_;

    /**
     * Constructor.
     *
     * @param  name  column name
     * @param  ftype  data type
     * @param  isNullable  whether the column may contain values that need
     *                     to be flagged as null in the validity mask
     * @param  userMeta  optional user metadata string, probably should be JSON
     * @param  size   number of bytes per serialized element
     * @param  nrow   number of rows
     */
    protected NumberRandomWriter( String name, FeatherType ftype,
                                  boolean isNullable, String userMeta,
                                  int size, long nrow ) {
        super( name, ftype, nrow, isNullable, userMeta );
        nbyte_ = size * nrow;
    }

    /**
     * Writes the data bytes for this column.
     *
     * @param  out  destination stream
     */
    protected abstract void writeData( OutputStream out ) throws IOException;

    /**
     * Returns false, but may be overridden if isNullable is true.
     */
    public boolean isNull( long irow ) {
        return false;
    }

    public long writeDataBytes( OutputStream out ) throws IOException {
        writeData( out );
        return nbyte_;
    }

    /**
     * Utility method to create a column writer from a double array.
     *
     * @param  name  column name
     * @param  data   data array
     * @param  userMeta  optional user metadata string, probably should be JSON
     * @return  column writer
     */
    public static FeatherColumnWriter
            createDoubleWriter( String name, final double[] data,
                                String userMeta ) {
        final int nrow = data.length;
        return new NumberRandomWriter( name, FeatherType.DOUBLE, false,
                                       userMeta, 8, nrow ) {
            protected void writeData( OutputStream out ) throws IOException {
                for ( int i = 0; i < nrow; i++ ) {
                    BufUtils.writeLittleEndianDouble( out, data[ i ] );
                }
            }
        };
    }

    /**
     * Utility method to create a column writer from a float array.
     *
     * @param  name  column name
     * @param  data   data array
     * @param  userMeta  optional user metadata string, probably should be JSON
     * @return  column writer
     */
    public static FeatherColumnWriter
            createFloatWriter( String name, final float[] data,
                               String userMeta ) {
        final int nrow = data.length;
        return new NumberRandomWriter( name, FeatherType.FLOAT, false,
                                       userMeta, 4, nrow ) {
            protected void writeData( OutputStream out ) throws IOException {
                for ( int i = 0; i < nrow; i++ ) {
                    BufUtils.writeLittleEndianFloat( out, data[ i ] );
                }
            }
        };
    }

    /**
     * Utility method to create a column writer from a long array.
     *
     * @param  name  column name
     * @param  data   data array
     * @param  userMeta  optional user metadata string, probably should be JSON
     * @return  column writer
     */
    public static FeatherColumnWriter
            createLongWriter( String name, final long[] data,
                              String userMeta ) {
        final int nrow = data.length;
        return new NumberRandomWriter( name, FeatherType.INT64, false,
                                       userMeta, 8, nrow ) {
            protected void writeData( OutputStream out ) throws IOException {
                for ( int i = 0; i < nrow; i++ ) {
                    BufUtils.writeLittleEndianLong( out, data[ i ] );
                }
            }
        };
    }

    /**
     * Utility method to create a column writer from an int array.
     *
     * @param  name  column name
     * @param  data   data array
     * @param  userMeta  optional user metadata string, probably should be JSON
     * @return  column writer
     */
    public static FeatherColumnWriter
            createIntWriter( String name, final int[] data, String userMeta ) {
        final int nrow = data.length;
        return new NumberRandomWriter( name, FeatherType.INT32, false,
                                       userMeta, 4, nrow ) {
            protected void writeData( OutputStream out ) throws IOException {
                for ( int i = 0; i < nrow; i++ ) {
                    BufUtils.writeLittleEndianInt( out, data[ i ] );
                }
            }
        };
    }

    /**
     * Utility method to create a column writer from a short array.
     *
     * @param  name  column name
     * @param  data   data array
     * @param  userMeta  optional user metadata string, probably should be JSON
     * @return  column writer
     */
    public static FeatherColumnWriter
            createShortWriter( String name, final short[] data,
                               String userMeta ) {
        final int nrow = data.length;
        return new NumberRandomWriter( name, FeatherType.INT16, false,
                                       userMeta, 2, nrow ) {
            protected void writeData( OutputStream out ) throws IOException {
                for ( int i = 0; i < nrow; i++ ) {
                    BufUtils.writeLittleEndianShort( out, data[ i ] );
                }
            }
        };
    }

    /**
     * Utility method to create a column writer from a byte array.
     *
     * @param  name  column name
     * @param  data   data array
     * @param  userMeta  optional user metadata string, probably should be JSON
     * @return  column writer
     */
    public static FeatherColumnWriter
            createByteWriter( String name, final byte[] data,
                              String userMeta ) {
        final int nrow = data.length;
        return new NumberRandomWriter( name, FeatherType.INT8, false,
                                       userMeta, 1, nrow ) {
            protected void writeData( OutputStream out ) throws IOException {
                out.write( data );
            }
        };
    }
}
