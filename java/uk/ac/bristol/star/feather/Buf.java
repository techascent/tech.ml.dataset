package uk.ac.bristol.star.feather;

/**
 * Provides methods to work with a potentially large byte buffer.
 *
 * <p>Methods are similar to those of {@link java.nio.ByteBuffer},
 * but:
 * <ol>
 * <li>this works with long-valued indices</li>
 * <li>the primitive access methods assume little-endian encodings</li>
 * </ol>
 *
 * @author   Mark Taylor
 * @since    25 Feb 2020
 */
public class Buf {

    private final BufAccess access_;

    /**
     * Constructor.
     *
     * @param  access  byte access implementation
     */
    public Buf( BufAccess access ) {
        access_ = access;
    }

    /**
     * Returns the byte at a given offset.
     *
     * @param  ix  index
     * @return  byte value
     */
    public byte get( long ix ) {
        return access_.get( ix );
    }

    /**
     * Returns the 16-bit integer stored little-endian at a given offset.
     *
     * @param  ix  index
     * @return  short value
     */
    public short getLittleEndianShort( long ix ) {
        return (short)
               ( ( ( get( ix + 0 ) & 0xff ) <<  0 )
               | ( ( get( ix + 1 ) & 0xff ) <<  8 ) );
    }

    /**
     * Returns the 32-bit integer stored little-endian at a given offset.
     *
     * @param  ix  index
     * @return  integer value
     */
    public int getLittleEndianInt( long ix ) {
        return ( ( get( ix + 0 ) & 0xff ) <<  0 )
             | ( ( get( ix + 1 ) & 0xff ) <<  8 )
             | ( ( get( ix + 2 ) & 0xff ) << 16 )
             | ( ( get( ix + 3 ) & 0xff ) << 24 );
    }

    /**
     * Returns the 64-bit integer stored little-endian at a given offset.
     *
     * @param  ix  index
     * @return  long value
     */
    public long getLittleEndianLong( long ix ) {
        return ( ( get( ix + 0 ) & 0xffL ) <<  0 )
             | ( ( get( ix + 1 ) & 0xffL ) <<  8 )
             | ( ( get( ix + 2 ) & 0xffL ) << 16 )
             | ( ( get( ix + 3 ) & 0xffL ) << 24 )
             | ( ( get( ix + 4 ) & 0xffL ) << 32 )
             | ( ( get( ix + 5 ) & 0xffL ) << 40 )
             | ( ( get( ix + 6 ) & 0xffL ) << 48 )
             | ( ( get( ix + 7 ) & 0xffL ) << 56 );
    }

    /**
     * Returns the 32-bit IEEE 754 floating point value
     * stored little-endian at a given offset.
     *
     * @param  ix  index
     * @return  float value
     */
    public float getLittleEndianFloat( long ix ) {
        return Float.intBitsToFloat( getLittleEndianInt( ix ) );
    }

    /**
     * Returns the 64-bit IEEE 754 floating point value
     * stored little-endian at a given offset.
     *
     * @param  ix  index
     * @return  double value
     */
    public double getLittleEndianDouble( long ix ) {
        return Double.longBitsToDouble( getLittleEndianLong( ix ) );
    }

    /**
     * Copies bytes from a given offset into a supplied byte array.
     *
     * @param  ix  index into buffer
     * @param  dbytes  buffer which will be fully populated with copies
     *                 of bytes starting at offset <code>ix</code>
     *                 in this buffer
     */
    public void get( long ix, byte[] dbytes ) {
        int nb = dbytes.length;
        for ( int ib = 0; ib < nb; ib++ ) {
            dbytes[ ib ] = get( ix + ib );
        }
    }

    /**
     * Indicates whether a given bit is set.
     *
     * @param  bitIndex   index of a bit offset from the start of this buffer
     * @return   true iff the given byte is set
     */
    public boolean isBitSet( long bitIndex ) {
        return ( get( bitIndex / 8 ) & ( 1 << ((int) bitIndex) % 8 ) )
             != 0;
    }
}
