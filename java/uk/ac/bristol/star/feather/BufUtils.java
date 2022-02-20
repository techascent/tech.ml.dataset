package uk.ac.bristol.star.feather;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * Miscellaneous utilities for working with bytes.
 *
 * @author   Mark Taylor
 * @since    25 Feb 2020
 */
public class BufUtils {

    /** UTF-8 charset instance. */
    public static final Charset UTF8 = Charset.forName( "UTF-8" );

    /**
     * Private constructor prevents instantiation.
     */
    private BufUtils() {
    }

    /**
     * Reads a little-endian 32-bit integer value from the current
     * position of a file.  File position is adavanced 4 bytes.
     *
     * @param  raf  file
     * @return  integer value
     */
    public static int readLittleEndianInt( RandomAccessFile raf )
            throws IOException {
        return ( raf.read() & 0xff ) <<  0
             | ( raf.read() & 0xff ) <<  8
             | ( raf.read() & 0xff ) << 16
             | ( raf.read() & 0xff ) << 24;
    }

    /**
     * Writes a little-endian 64-bit integer value to an output stream.
     *
     * @param  out   output stream
     * @param  l   value to write
     */
    public static void writeLittleEndianLong( OutputStream out, long l )
            throws IOException {
        out.write( ( (int) ( l >>  0 ) ) & 0xff );
        out.write( ( (int) ( l >>  8 ) ) & 0xff );
        out.write( ( (int) ( l >> 16 ) ) & 0xff );
        out.write( ( (int) ( l >> 24 ) ) & 0xff );
        out.write( ( (int) ( l >> 32 ) ) & 0xff );
        out.write( ( (int) ( l >> 40 ) ) & 0xff );
        out.write( ( (int) ( l >> 48 ) ) & 0xff );
        out.write( ( (int) ( l >> 56 ) ) & 0xff );
    }

    /**
     * Writes a little-endian 32-bit integer value to an output stream.
     *
     * @param  out   output stream
     * @param  i   value to write
     */
    public static void writeLittleEndianInt( OutputStream out, int i )
            throws IOException {
        out.write( ( i >>  0 ) & 0xff );
        out.write( ( i >>  8 ) & 0xff );
        out.write( ( i >> 16 ) & 0xff );
        out.write( ( i >> 24 ) & 0xff );
    }

    /**
     * Writes a little-endian 16-bit integer value to an output stream.
     *
     * @param  out   output stream
     * @param  s   value to write
     */
    public static void writeLittleEndianShort( OutputStream out, short s )
            throws IOException {
        out.write( ( s >> 0 ) & 0xff );
        out.write( ( s >> 8 ) & 0xff );
    }

    /**
     * Writes a little-endian 64-bit IEEE 754 floating point value
     * to an output stream.
     *
     * @param  out   output stream
     * @param  d   value to write
     */
    public static void writeLittleEndianDouble( OutputStream out, double d )
            throws IOException {
        writeLittleEndianLong( out, Double.doubleToLongBits( d ) );
    }

    /**
     * Writes a little-endian 32-bit IEEE 754 floating point value
     * to an output stream.
     *
     * @param  out   output stream
     * @param  f   value to write
     */
    public static void writeLittleEndianFloat( OutputStream out, float f )
            throws IOException {
        writeLittleEndianInt( out, Float.floatToIntBits( f ) );
    }

    /**
     * Pads an output stream to an 8-byte boundary.
     * On exit, a number of bytes 0&lt;=n&lt;8 will have been written
     * to the supplied output stream so that its length is a multiple of 8.
     * The number of bytes written is returned.
     *
     * @param  out  output stream
     * @param  nb   number of bytes currently written to stream
     *              (only 3 lsbs are significant)
     * @return  number of byte that this routine has written
     */
    public static int align8( OutputStream out, long nb ) throws IOException {
        int over = ((int) nb) & 0x7;
        int pad;
        if ( over > 0 ) {
            pad = 8 - over;
            for ( int i = 0; i < pad; i++ ) {
                out.write( 0 );
            }
        }
        else {
            pad = 0;
        }
        return pad;
    }

    /**
     * Returns the smallest multiple of 8 that is greater than or
     * equal to the argument.
     *
     * @param  nb  non-negative value
     * @return  multiple of 8
     */
    public static long ceil8( long nb ) {
        return ( ( nb + 7 ) / 8 ) * 8;
    }

    /**
     * Returns the number of UTF-8 encoded bytes that correspond to
     * a java string.  This is supposed to be faster than actually
     * doing the encoding.
     *
     * @param  txt  string
     * @return   value of <code>txt.getBytes(UTF8).length()</code>
     */
    public static int utf8Length( String txt ) {

        // copied from https://stackoverflow.com/questions/8511490
        int count = 0;
        int nc = txt.length();
        for ( int i = 0; i < nc; i++ ) {
            char ch = txt.charAt( i );
            if ( ch <= 0x7F ) {
                count++;
            }
            else if ( ch <= 0x7FF ) {
                count += 2;
            }
            else if ( Character.isHighSurrogate( ch ) ) {
                count += 4;
                ++i;
            }
            else {
                count += 3;
            }
        }
        assert count == txt.getBytes( UTF8 ).length;
        return count;
    }
}
