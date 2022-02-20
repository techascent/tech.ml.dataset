package uk.ac.bristol.star.feather;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Partial FeatherColumnWriter implementation that handles
 * generic aspects including writing the optional validity mask.
 *
 * @author   Mark Taylor
 * @since    26 Feb 2020
 */
public abstract class AbstractColumnWriter implements FeatherColumnWriter {

    private final String name_;
    private final FeatherType featherType_;
    private final long nrow_;
    private final boolean isNullable_;
    private final String userMeta_;

    /**
     * Constructor.
     *
     * @param  name  column name
     * @param  featherType  column data type
     * @param  nrow  number of rows
     * @param  isNullable  whether the column may contain values that need
     *                     to be flagged as null in the validity mask
     * @param  userMeta  optional user metadata string, probably should be JSON
     */
    protected AbstractColumnWriter( String name, FeatherType featherType,
                                    long nrow, boolean isNullable,
                                    String userMeta ) {
        name_ = name;
        featherType_ = featherType;
        nrow_ = nrow;
        isNullable_ = isNullable;
        userMeta_ = userMeta;
    }

    public String getName() {
        return name_;
    }

    public FeatherType getFeatherType() {
        return featherType_;
    }

    public long getRowCount() {
        return nrow_;
    }

    public boolean isNullable() {
        return isNullable_;
    }

    public String getUserMetadata() {
        return userMeta_;
    }

    public ColStat writeColumnBytes( OutputStream out ) throws IOException {
        long nNull = 0;
        final long maskBytes;
        if ( isNullable_ ) {
            int mask = 0;
            int ibit = 0;
            for ( long ir = 0; ir < nrow_; ir++ ) {
                if ( isNull( ir ) ) {
                    nNull++;
                }
                else {
                    mask |= 1 << ibit;
                }
                if ( ++ibit == 8 ) {
                    out.write( mask );
                    ibit = 0;
                    mask = 0;
                }
            }
            if ( ibit > 0 ) {
                out.write( mask );
            }
            long mb = ( nrow_ + 7 ) / 8;
            maskBytes = mb + BufUtils.align8( out, mb );
        }
        else {
            maskBytes = 0;
        }
        long dataBytes = writeDataBytes( out );
        dataBytes += BufUtils.align8( out, dataBytes );
        boolean hasNull = nNull > 0;
        final long byteCount = maskBytes + dataBytes;
        final long dataOffset = maskBytes;
        final long nullCount = nNull;
        return new ColStat() {
            public long getRowCount() {
                return nrow_;
            }
            public long getByteCount() {
                return byteCount;
            }
            public long getDataOffset() {
                return dataOffset;
            }
            public long getNullCount() {
                return nullCount;
            }
        };
    }

    /**
     * Tests the value at a given row for nullness
     * (whether it needs to be flagged as null in the validity mask).
     * This method is only ever called for nullable columns
     * (if the <code>isNullable</code> flag was set true at construction time).
     *
     * @param   irow  row index to test
     * @return   true iff the value at row irow is to be flagged null
     */
    public abstract boolean isNull( long irow );

    /**
     * Writes the bytes constituting the data stream for this column,
     * excluding any optional validity mask.
     * Note the output does not need to be aligned on an 8-byte boundary.
     *
     * @param   out  destination stream
     * @return   number of bytes written
     */
    public abstract long writeDataBytes( OutputStream out ) throws IOException;
}
