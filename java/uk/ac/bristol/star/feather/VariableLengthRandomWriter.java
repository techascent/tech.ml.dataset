package uk.ac.bristol.star.feather;

import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Logger;

/**
 * Partial FeatherColumnWriter implementation for random access
 * variable-length data types.
 *
 * @author   Mark Taylor
 * @since    26 Feb 2020
 */
public abstract class VariableLengthRandomWriter extends AbstractColumnWriter {

    private final long nrow_;
    private static final Logger logger_ =
        Logger.getLogger( VariableLengthRandomWriter.class.getName() );

    /**
     * Constructor.
     *
     * @param  name  column name
     * @param  ftype   feather data type
     * @param  nrow  number of rows
     * @param  isNullable  whether the column may contain values that need
     *                     to be flagged as null in the validity mask
     * @param  userMeta  optional user metadata string, probably should be JSON
     */
    protected VariableLengthRandomWriter( String name, FeatherType ftype,
                                          long nrow, boolean isNullable,
                                          String userMeta ) {
        super( name, ftype, nrow, isNullable, userMeta );
        nrow_ = nrow;
    }

    /**
     * Returns the number of bytes that will be written for
     * the element at a given row.
     *
     * @param  irow  row index
     * @return   extent of cell in bytes
     */
    protected abstract int getByteSize( long irow );

    /**
     * Writes the bytes representing the element at a given row.
     *
     * @param  irow  row index
     * @param  out  destination stream
     */
    protected abstract void writeItem( long irow, OutputStream out )
            throws IOException;

    public long writeDataBytes( OutputStream out ) throws IOException {
        IndexStatus status = writeOffsets( out );
        long nbyteData = status.byteCount_;
        long nbyteOffsets = 4 * ( nrow_ + 1 );
        nbyteOffsets += BufUtils.align8( out, nbyteOffsets );
        long nentry = status.entryCount_;
        for ( long ir = 0; ir < nentry; ir++ ) {
            writeItem( ir, out );
        }
        nbyteData += BufUtils.align8( out, nbyteData );
        return nbyteOffsets + nbyteData;
    }

    /**
     * Writes the offset table giving the nrow+1 offsets of the column's
     * variable-length values in the data block.
     *
     * @param  out  desination stream
     * @return  result of writing
     */
    private IndexStatus writeOffsets( OutputStream out ) throws IOException {
        long ioff = 0;
        for ( long ir = 0; ir < nrow_; ir++ ) {
            BufUtils.writeLittleEndianInt( out, (int) ioff );
            long ioff1 = ioff + getByteSize( ir );
            if ( ioff1 >= Integer.MAX_VALUE ) {
                logger_.warning( "Pointer overflow - empty values in column "
                               + getName() + " past row " + ir );
                IndexStatus status = new IndexStatus( ir, ioff );
                for ( ; ir < nrow_; ir++ ) {
                    BufUtils.writeLittleEndianInt( out, (int) ioff );
                }
                return status;
            }
            ioff = ioff1;
        }
        BufUtils.writeLittleEndianInt( out, (int) ioff );
        return new IndexStatus( nrow_, ioff );
    }

    /**
     * Utility method to create a column writer from a String array.
     *
     * @param  name  column name
     * @param  data   data array
     * @param  userMeta  optional user metadata string, probably should be JSON
     * @param  isNullable   if true, null values will be flagged;
     *                      if false, they will just be written as zero length
     * @return  column writer
     */
    public static FeatherColumnWriter
            createStringWriter( String name, final String[] data,
                                String userMeta, boolean isNullable ) {
        return new VariableLengthRandomWriter( name, FeatherType.UTF8,
                                               data.length, isNullable,
                                               userMeta ) {
            public boolean isNull( long irow ) {
                return data[ longToInt( irow ) ] == null;
            }
            public int getByteSize( long irow ) {
                String str = data[ longToInt( irow ) ];
                return str == null ? 0 : BufUtils.utf8Length( str );
            }
            public void writeItem( long irow, OutputStream out )
                    throws IOException {
                String str = data[ longToInt( irow ) ];
                if ( str != null ) {
                    out.write( str.getBytes( BufUtils.UTF8 ) );
                }
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

    /**
     * Encapsulates the outcome of the index writing.
     */
    private static class IndexStatus {
        final long entryCount_;
        final long byteCount_;

        /**
         * Constructor.
         *
         * @param  entryCount  number of entries to actually write;
         *                     this may not be all of them in case of
         *                     pointer overflow
         * @param  byteCount   size of data block in bytes
         */
        IndexStatus( long entryCount, long byteCount ) {
            entryCount_ = entryCount;
            byteCount_ = byteCount;
        }
    }
}
