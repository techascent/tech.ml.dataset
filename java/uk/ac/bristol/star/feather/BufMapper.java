package uk.ac.bristol.star.feather;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Provides memory-mapped random access to potentially large files.
 *
 * <p>I *think* the buffers returned by this object are thread-safe.
 * The ByteBuffer class on which they are based warns that it is not
 * thread-safe, but the only ByteBuffer method I'm using is
 * the absolute {@link java.nio.ByteBuffer#get(int)}, and I can't see
 * why that would suffer from threading issues.
 *
 * @author  Mark Taylor
 * @since   25 Feb 2020
 */
public class BufMapper {

    private final FileChannel channel_;
    private final long start_;
    private final long length_;
    private static final FileChannel.MapMode RO_MODE =
        FileChannel.MapMode.READ_ONLY;

    /**
     * Constructor.
     *
     * @param   channel   file channel
     * @param   start    start offset into file of mappable region
     * @param   length   length of mappable region
     */
    public BufMapper( FileChannel channel, long start, long length ) {
        channel_ = channel;
        start_ = start;
        length_ = length;
    }

    /**
     * Returns the starting offset of the file region represented by this
     * mapper.
     *
     * @return  file region start offset
     */
    public long getStart() {
        return start_;
    }

    /**
     * Returns the length of the file region represented by this mapper.
     *
     * @return  file region length
     */
    public long getLength() {
        return length_;
    }

    /**
     * Maps the whole of this mapper's file region as a buffer.
     *
     * @return   mapped buffer
     */
    public Buf mapBuffer() throws IOException {
        return mapBuffer( 0, length_ );
    }

    /**
     * Maps part of this mapper's file region as a buffer.
     *
     * @param  offset  additional offset from the start of this mapper's offset
     * @param  leng   length of the region to map
     * @return   mapped region or subregion
     */
    public Buf mapBuffer( long offset, long leng ) throws IOException {
        long off = start_ + offset;
        BufAccess access =
            leng < Integer.MAX_VALUE
          ? new SimpleAccess( channel_, off, leng )
          : new MultiAccess( channel_, off, leng );
        return new Buf( access );
    }

    /**
     * BufAccess implementation based on a single MappedByteBuffer.
     */
    private static class SimpleAccess implements BufAccess {

        private final ByteBuffer bbuf_;

        /**
         * Constructor.
         *
         * @param  channel  file channel
         * @param  offset  offset into file of mapped region start
         * @param  leng    length of mapped region
         */
        public SimpleAccess( FileChannel channel, long offset, long leng )
                throws IOException {
            bbuf_ = channel.map( RO_MODE, offset, leng );
        }
    
        public byte get( long ix ) {
            return bbuf_.get( longToInt( ix ) );
        } 

        private static int longToInt( long ix ) {
            return (int) ix;
        }
    }

    /**
     * BufAccess implementation based on a bank of MappedByteBuffers.
     */
    private static class MultiAccess implements BufAccess {

        private final FileChannel channel_;
        private final long offset_;
        private final long leng_;
        private final int pow2_;
        private final long bankSize_;
        private final int mask_;
        private final ByteBuffer[] bbufs_;

        /**
         * Constructs a MultiAccess with configurable bank size.
         *
         * @param  channel  file channel
         * @param  offset  offset into file of mapped region start
         * @param  leng    length of mapped region
         * @param  pow2    logarithm to base 2 of bank size
         */
        public MultiAccess( FileChannel channel, long offset, long leng,
                            int pow2 ) {
            if ( pow2 < 1 || pow2 > 31 ) {
                throw new IllegalArgumentException( "Bad pow2: " + pow2 );
            }
            channel_ = channel;
            offset_ = offset;
            leng_ = leng;
            pow2_ = pow2;
            bankSize_ = 1L << pow2_;
            mask_ = (int) bankSize_ - 1;
            bbufs_ = new ByteBuffer[ bankIndex( leng ) + 1 ];
        }

        /**
         * Constructs a MultiAccess with default bank size of 1 Gb.
         *
         * @param  channel  file channel
         * @param  offset  offset into file of mapped region start
         * @param  leng    length of mapped region
         */
        public MultiAccess( FileChannel channel, long offset, long leng ) {
            this( channel, offset, leng, 30 );
        }

        public byte get( long ix ) {
            return getByteBuffer( bankIndex( ix ) ).get( bankOffset( ix ) );
        }

        /**
         * Returns the buffer containing a given data bank.
         * Buffers are lazily mapped as required.
         *
         * @param  ibuf  bank index
         * @return  buffer
         */
        public ByteBuffer getByteBuffer( int ibuf ) {
            ByteBuffer bbuf = bbufs_[ ibuf ];
            if ( bbuf != null ) {
                return bbuf;
            }
            else {
                long boff1 = offset_ + ibuf * bankSize_;
                long leng1 = Math.min( bankSize_, leng_ - ibuf * bankSize_ );
                ByteBuffer bbuf1;
                try {
                    bbuf1 = channel_.map( RO_MODE, boff1, leng1 );
                }
                catch ( IOException e ) {
                    throw new RuntimeException( "File mapping failure: " + e,
                                                e );
                }
                bbufs_[ ibuf ] = bbuf1;
                return bbuf1;
            }
        }

        /**
         * Returns the bank index for a given byte offset.
         *
         * @param  ix  byte offset
         * @return  bank index
         */
        private int bankIndex( long ix ) {
            return (int) ( ix >> pow2_ );
        }

        /**
         * Returns the offset into a bank for a given byte offset.
         *
         * @param  ix  global byte offset
         * @return  offset into relevant bank
         */
        private int bankOffset( long ix ) {
            return (int) ix & mask_;
        }
    }
}
