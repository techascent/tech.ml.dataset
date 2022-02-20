package uk.ac.bristol.star.feather;

import java.io.IOException;
import uk.ac.bristol.star.fbs.feather.Column;

/**
 * Represents a column in a readable Feather-format table.
 *
 * @author   Mark Taylor
 * @since    26 Feb 2020
 */
public class FeatherColumn {

    private final String name_;
    private final long nrow_;
    private final BufMapper mapper_;
    private final Decoder<?> decoder_;
    private final long nNull_;
    private final String userMeta_;

    /**
     * Constructor.
     *
     * @param  name  column name
     * @param  nrow  number of entries
     * @param  mapper   mapper object for access to this column's data
     * @param  decoder  object that can turn bytes into typed values
     *                  for this column
     * @param  nNull   number of flagged null values in this column
     * @param  userMeta  user metadata string, probably JSON
     */
    public FeatherColumn( String name, long nrow, BufMapper mapper,
                          Decoder<?> decoder, long nNull, String userMeta ) {
        name_ = name;
        nrow_ = nrow;
        mapper_ = mapper;
        decoder_ = decoder;
        nNull_ = nNull;
        userMeta_ = userMeta;
    }

    /**
     * Returns the column name.
     *
     * @return  column name
     */
    public String getName() {
        return name_;
    }

    /**
     * Returns the decoder used to read this column's data.
     *
     * @return  decoder
     */
    public Decoder<?> getDecoder() {
        return decoder_;
    }

    /**
     * Returns the number of rows in this column.
     *
     * @return  row count
     */
    public long getRowCount() {
        return nrow_;
    }

    /**
     * Returns the optional user metadata string from this column.
     * May be JSON.
     *
     * @return  metadata string, maybe JSON, or null
     */
    public String getUserMeta() {
        return userMeta_;
    }

    /**
     * Returns the number of null values in this column.
     *
     * @return  null count
     */
    public long getNullCount() {
        return nNull_;
    }

    /**
     * Creates a reader that can be used to read the data for this column.
     * These readers are <em>probably</em> thread-safe
     * (if {@link java.nio.MappedByteBuffer#get(int)} is thread-safe),
     * but it's not a bad idea to acquire one for each thread where
     * feasible.
     *
     * @return  column data reader
     */
    public Reader<?> createReader() throws IOException {
        if ( nNull_ == 0 ) {
            return decoder_.createReader( mapper_.mapBuffer(), nrow_ );
        }
        else {
            // The Feather docs say this is byte aligned, but it looks like
            // it's aligned on 64-bit boundaries.
            long dataOffset = ( ( nrow_ + 63 ) / 64 ) * 8;
            Buf maskBuf = mapper_.mapBuffer( 0, dataOffset );
            Buf dataBuf = mapper_.mapBuffer( dataOffset,
                                             mapper_.getLength() - dataOffset );
            return createMaskReader( decoder_.createReader( dataBuf, nrow_ ),
                                     maskBuf );
        }
    }

    @Override
    public String toString() {
        StringBuffer sbuf = new StringBuffer()
            .append( name_ ) 
            .append( "(" )
            .append( decoder_ );
        if ( nNull_ > 0 ) {
            sbuf.append( ",nulls=" )
                .append( nNull_ );
        }
        if ( userMeta_ != null && userMeta_.trim().length() > 0 ) {
            sbuf.append( ":" )
                .append( '"' )
                .append( userMeta_ )
                .append( '"' );
        }
        sbuf.append( ")" );
        return sbuf.toString();
    }

    /**
     * Returns a reader that applies a validity mask to an underlying
     * data reader.
     *
     * @param  basicReader  reader supplying unmasked data values
     * @param  maskBuf   data buffer containing validity bitmask
     * @return  masked reader
     */
    private static <T> Reader<T> createMaskReader( final Reader<T> basicReader,
                                                   final Buf maskBuf ) {
        return new Reader<T>() {
            private boolean isMask( long ix ) {
                return maskBuf.isBitSet( ix );
            }
            public boolean isNull( long ix ) {
                return ! isMask( ix );
            }
            public T getObject( long ix ) {
                return isMask( ix ) ? basicReader.getObject( ix ) : null;
            }
            public byte getByte( long ix ) {
                return isMask( ix ) ? basicReader.getByte( ix ) : null;
            }
            public short getShort( long ix ) {
                return isMask( ix ) ? basicReader.getShort( ix ) : null;
            }
            public int getInt( long ix ) {
                return isMask( ix ) ? basicReader.getInt( ix ) : null;
            }
            public long getLong( long ix ) {
                return isMask( ix ) ? basicReader.getLong( ix ) : null;
            }
            public float getFloat( long ix ) {
                return isMask( ix ) ? basicReader.getFloat( ix ) : Float.NaN;
            }
            public double getDouble( long ix ) {
                return isMask( ix ) ? basicReader.getDouble( ix ) : Double.NaN;
            }
            public Class<T> getValueClass() {
                return basicReader.getValueClass();
            }
        };
    }
}
