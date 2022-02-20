package uk.ac.bristol.star.feather;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Partial FeatherColumnWriter for random access boolean columns.
 *
 * @author   Mark Taylor
 * @since    26 Feb 2020
 */
public abstract class BooleanRandomWriter extends AbstractColumnWriter {

    /**
     * Constructor.
     *
     * @param  name  column name
     * @param  nrow  number of rows
     * @param  isNullable  whether the column may contain values that need
     *                     to be flagged as null in the validity mask
     * @param  userMeta  optional user metadata string, probably should be JSON
     */
    protected BooleanRandomWriter( String name, long nrow, boolean isNullable,
                                   String userMeta ) {
        super( name, FeatherType.BOOL, nrow, isNullable, userMeta );
    }

    /**
     * Returns the boolean value for a given row.
     *
     * @param  ix  row index
     * @return  boolean value
     */
    public abstract boolean getValue( long ix );

    /**
     * Returns false, but may be overridden if isNullable is true.
     */
    public boolean isNull( long irow ) {
        return false;
    }

    public long writeDataBytes( OutputStream out ) throws IOException {
        long nrow = getRowCount();
        int outByte = 0;
        int ibit = 0;
        for ( long ir = 0; ir < nrow; ir++ ) {
            if ( getValue( ir ) ) {
                outByte |= 1 << ibit;
            }
            if ( ++ibit == 8 ) {
                out.write( outByte );
                ibit = 0;
                outByte = 0;
            }
        }
        if ( ibit > 0 ) {
            out.write( outByte );
        }
        return ( nrow + 7 ) / 8;
    }

    /**
     * Utility method to create a column writer from a boolean array.
     *
     * @param  name  column name
     * @param  data   data array
     * @param  userMeta  optional user metadata string, probably should be JSON
     * @return  column writer
     */
    public static FeatherColumnWriter
            createBooleanWriter( String name, final boolean[] data,
                                 String userMeta ) {
        return new BooleanRandomWriter( name, data.length, false, userMeta ) {
            public boolean getValue( long ix ) {
                return data[ longToInt( ix ) ];
            }
            private int longToInt( long lrow ) {
                int irow = (int) lrow;
                if ( irow == lrow ) {
                    return irow;
                }
                else {
                    throw new IllegalArgumentException( "integer overflow" );
                }
            }
        };
    }
}
