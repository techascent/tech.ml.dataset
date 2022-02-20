package uk.ac.bristol.star.feather;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Turns feather-encoded byte data into typed data values.
 *
 * @author   Mark Taylor
 * @since    26 Feb 2020
 */
@SuppressWarnings("cast")
public abstract class Decoder<T> {

    private final Class<T> clazz_;
    private final FeatherType ftype_;

    private static final Map<FeatherType,Decoder<?>> TYPE_DECODERS =
        createTypeDecoders();
    private static final Logger logger_ =
        Logger.getLogger( Decoder.class.getName() );

    /**
     * Constructor.
     * 
     * @param  clazz  output data class
     * @param  ftype  feather data type
     */
    private Decoder( Class<T> clazz, FeatherType ftype ) {
        clazz_ = clazz;
        ftype_ = ftype;
    }

    /**
     * Returns a typed reader for a given stored data buffer.
     * The {@link Reader#isNull(long)} method from readers
     * returned by this method will always return false,
     * since the decoder does not deal with validity masks.
     *
     * @param  buf  byte buffer
     * @param  nrow  number of rows in buffer
     */
    public abstract Reader<T> createReader( Buf buf, long nrow );

    /**
     * Returns the type of object values that this decoder will decode to.
     *
     * @return  value class
     */
    public Class<T> getValueClass() {
        return clazz_;
    }

    /**
     * Returns the type of feather storage that this decoder can decode.
     *
     * @return  feather data type
     */
    public FeatherType getFeatherType() {
        return ftype_;
    }

    @Override
    public String toString() {
        return ftype_.toString();
    }

    /**
     * Acquires a decoder for a given data type.
     *
     * @param  ftype   data type
     * @return  decoder
     */
    public static Decoder<?> getDecoder( FeatherType ftype ) {
        Decoder<?> decoder = TYPE_DECODERS.get( ftype );
        if ( decoder != null ) {
            return decoder;
        }
        else {
            logger_.warning( "No decoder for data type " + ftype );
            return new UnsupportedDecoder( ftype );
        }
    }

    /**
     * Generates a map of all standard decoders.
     *
     * @return  FeatherType-&gt;Decoder map
     */
    private static Map<FeatherType,Decoder<?>> createTypeDecoders() {
        Decoder<?>[] decoders = new Decoder<?>[] {
            new Decoder<Boolean>( Boolean.class, FeatherType.BOOL ) {
                public Reader<Boolean> createReader( final Buf buf,
                                                     long nrow ) {
                    return new AbstractReader<Boolean>( Boolean.class ) {
                        private boolean get( long ix ) {
                            return buf.isBitSet( ix );
                        }
                        public Boolean getObject( long ix ) {
                            return Boolean.valueOf( get( ix ) );
                        }
                        public byte getByte( long ix ) {
                            return get( ix ) ? (byte) 1 : (byte) 0;
                        }
                        public short getShort( long ix ) {
                            return get( ix ) ? (short) 1 : (short) 0;
                        }
                        public int getInt( long ix ) {
                            return get( ix ) ? 1 : 0;
                        }
                        public long getLong( long ix ) {
                            return get( ix ) ? 1L : 0L;
                        }
                        public float getFloat( long ix ) {
                            return get( ix ) ? 1f : 0f;
                        }
                        public double getDouble( long ix ) {
                            return get( ix ) ? 1. : 0.;
                        }
                    };
                };
            },
            new Decoder<Byte>( Byte.class, FeatherType.INT8 ) {
                public Reader<Byte> createReader( final Buf buf, long nrow ) {
                    return new AbstractReader<Byte>( Byte.class ) {
                        private byte get( long ix ) {
                            return buf.get( ix );
                        }
                        public Byte getObject( long ix ) {
                            return Byte.valueOf( get( ix ) );
                        }
                        public byte getByte( long ix ) {
                            return (byte) get( ix );
                        }
                        public short getShort( long ix ) {
                            return (short) get( ix );
                        }
                        public int getInt( long ix ) {
                            return (int) get( ix );
                        }
                        public long getLong( long ix ) {
                            return (long) get( ix );
                        }
                        public float getFloat( long ix ) {
                            return (float) get( ix );
                        }
                        public double getDouble( long ix ) {
                            return (double) get( ix );
                        }
                    };
                }
            },
            new Decoder<Short>( Short.class, FeatherType.INT16 ) {
                public Reader<Short> createReader( final Buf buf, long nrow ) {
                    return new ShortReader() {
                        short get( long ix ) {
                            return buf.getLittleEndianShort( 2 * ix );
                        }
                    };
                }
            },
            new Decoder<Integer>( Integer.class, FeatherType.INT32 ) {
                public Reader<Integer> createReader( final Buf buf,
                                                     long nrow ) {
                    return new IntReader() {
                        int get( long ix ) {
                            return buf.getLittleEndianInt( 4 * ix );
                        }
                    };
                }
            },
            new Decoder<Long>( Long.class, FeatherType.INT64 ) {
                public Reader<Long> createReader( final Buf buf, long nrow ) {
                    return new LongReader() {
                        long get( long ix ) {
                            return buf.getLittleEndianLong( 8 * ix );
                        }
                    };
                }
            },
            new Decoder<Short>( Short.class, FeatherType.UINT8 ) {
                public Reader<Short> createReader( final Buf buf, long nrow ) {
                    return new ShortReader() {
                        short get( long ix ) {
                            return (short) ( 0xff & buf.get( ix ) );
                        }
                    };
                }
            },
            new Decoder<Integer>( Integer.class, FeatherType.UINT16 ) {
                public Reader<Integer> createReader( final Buf buf,
                                                     long nrow ) {
                    return new IntReader() {
                        int get( long ix ) {
                            return 0xffff & buf.getLittleEndianShort( 2 * ix );
                        }
                    };
                }
            },
            new Decoder<Long>( Long.class, FeatherType.UINT32 ) {
                public Reader<Long> createReader( final Buf buf, long nrow ) {
                    return new LongReader() {
                        long get( long ix ) {
                            return 0xffffffffL
                                 & buf.getLittleEndianInt( 4 * ix );
                        }
                    };
                }
            },
            new UnsupportedDecoder( FeatherType.UINT64 ),
            new Decoder<Float>( Float.class, FeatherType.FLOAT ) {
                public Reader<Float> createReader( final Buf buf, long nrow ) {
                    return new AbstractReader<Float>( Float.class ) {
                        private float get( long ix ) {
                            return buf.getLittleEndianFloat( 4 * ix );
                        }
                        public Float getObject( long ix ) {
                            return Float.valueOf( get( ix ) );
                        }
                        public byte getByte( long ix ) {
                            return (byte) get( ix );
                        }
                        public short getShort( long ix ) {
                            return (short) get( ix );
                        }
                        public int getInt( long ix ) {
                            return (int) get( ix );
                        }
                        public long getLong( long ix ) {
                            return (long) get( ix );
                        }
                        public float getFloat( long ix ) {
                            return (float) get( ix );
                        }
                        public double getDouble( long ix ) {
                            return (double) get( ix );
                        }
                    };
                }
            },
            new Decoder<Double>( Double.class, FeatherType.DOUBLE ) {
                public Reader<Double> createReader( final Buf buf, long nrow ) {
                    return new AbstractReader<Double>( Double.class ) {
                        private double get( long ix ) {
                            return buf.getLittleEndianDouble( 8 * ix );
                        }
                        public Double getObject( long ix ) {
                            return Double.valueOf( get( ix ) );
                        }
                        public byte getByte( long ix ) {
                            return (byte) get( ix );
                        }
                        public short getShort( long ix ) {
                            return (short) get( ix );
                        }
                        public int getInt( long ix ) {
                            return (int) get( ix );
                        }
                        public long getLong( long ix ) {
                            return (long) get( ix );
                        }
                        public float getFloat( long ix ) {
                            return (float) get( ix );
                        }
                        public double getDouble( long ix ) {
                            return (double) get( ix );
                        }
                    };
                }
            },
            new Decoder<String>( String.class, FeatherType.UTF8 ) {
                public Reader<String> createReader( Buf buf, long nrow ) {
                    return new VariableLengthReader32<String>( String.class,
                                                               buf, nrow ) {
                        public String getObject( long ix ) {
                            return new String( getBytes( ix ), BufUtils.UTF8 );
                        }
                    };
                }
            },
            new Decoder<byte[]>( byte[].class, FeatherType.BINARY ) {
                public Reader<byte[]> createReader( Buf buf, long nrow ) {
                    return new VariableLengthReader32<byte[]>( byte[].class,
                                                               buf, nrow ) {
                        public byte[] getObject( long ix ) {
                            return getBytes( ix );
                        }
                    };
                }
            },
            new Decoder<String>( String.class, FeatherType.LARGE_UTF8 ) {
                public Reader<String> createReader( Buf buf, long nrow ) {
                    return new VariableLengthReader64<String>( String.class,
                                                               buf, nrow ) {
                        public String getObject( long ix ) {
                            return new String( getBytes( ix ), BufUtils.UTF8 );
                        }
                    };
                }
            },
            new Decoder<byte[]>( byte[].class, FeatherType.LARGE_BINARY ) {
                public Reader<byte[]> createReader( Buf buf, long nrow ) {
                    return new VariableLengthReader64<byte[]>( byte[].class,
                                                               buf, nrow ) {
                        public byte[] getObject( long ix ) {
                            return getBytes( ix );
                        }
                    };
                }
            },

            /* I could do this, but (1) I don't know if there are any instances
             * of this data type in feather files out there and (2) it's not
             * clear to me how to interpret the feather format documentation. */
            new UnsupportedDecoder( FeatherType.CATEGORY ),
            new Decoder<Long>( Long.class, FeatherType.TIMESTAMP ) {
                public Reader<Long> createReader( final Buf buf, long nrow ) {
                    return new LongReader() {
                        long get( long ix ) {
                            return buf.getLittleEndianLong( 8 * ix );
                        }
                    };
                }
            },
            new Decoder<Integer>( Integer.class, FeatherType.DATE ) {
                public Reader<Integer> createReader( final Buf buf,
                                                     long nrow ) {
                    return new IntReader() {
                        int get( long ix ) {
                            return buf.getLittleEndianInt( 4 * ix );
                        }
                    };
                }
            },
            new Decoder<Long>( Long.class, FeatherType.TIME ) {
                public Reader<Long> createReader( final Buf buf, long nrow ) {
                    return new LongReader() {
                        long get( long ix ) {
                            return buf.getLittleEndianLong( 8 * ix );
                        }
                    };
                }
            },
        };
        Map<FeatherType,Decoder<?>> map =
            new LinkedHashMap<FeatherType,Decoder<?>>();
        for ( Decoder<?> decoder : decoders ) {
            FeatherType ftype = decoder.ftype_;
            assert ! map.containsKey( ftype );
            map.put( ftype, decoder );
        }
        assert map.keySet()
              .equals( new HashSet<FeatherType>( Arrays.asList( FeatherType
                                                               .ALL_TYPES ) ) );
        return Collections.unmodifiableMap( map );
    }

    /**
     * Skeleton reader implementation.
     * The isNull method always returns false, since Decoder doesn't
     * work with validity masks.
     */
    private static abstract class AbstractReader<T> implements Reader<T> {
        final Class<T> clazz_;
        AbstractReader( Class<T> clazz ) {
            clazz_ = clazz;
        }
        public Class<T> getValueClass() {
            return clazz_;
        }
        public boolean isNull( long ix ) {
            return false;
        }
    }

    /**
     * Partial Reader implementation for non-numeric values.
     * All numeric accessor methods return a boring constant.
     */
    private static abstract class NonNumericReader<T>
            extends AbstractReader<T> {
        public NonNumericReader( Class<T> clazz ) {
            super( clazz );
        }
        public byte getByte( long ix ) {
            return (byte) 0;
        }
        public short getShort( long ix ) {
            return (short) 0;
        }
        public int getInt( long ix ) {
            return 0;
        }
        public long getLong( long ix ) {
            return 0L;
        }
        public float getFloat( long ix ) {
            return Float.NaN;
        }
        public double getDouble( long ix ) {
            return Double.NaN;
        }
    }

    /**
     * Partial reader implementation for columns whose output type is Short.
     * Concrete subclasses just need to implement one method.
     */
    private static abstract class ShortReader extends AbstractReader<Short> {
        ShortReader() {
            super( Short.class );
        }

        /**
         * Returns the value at a given index as a short integer.
         *
         * @param ix  row index
         * @return  short value
         */
        abstract short get( long ix );

        public Short getObject( long ix ) {
            return Short.valueOf( get( ix ) );
        }
        public byte getByte( long ix ) {
            return (byte) get( ix );
        }
        public short getShort( long ix ) {
            return (short) get( ix );
        }
        public int getInt( long ix ) {
            return (int) get( ix );
        }
        public long getLong( long ix ) {
            return (long) get( ix );
        }
        public float getFloat( long ix ) {
            return (float) get( ix );
        }
        public double getDouble( long ix ) {
            return (double) get( ix );
        }
    }

    /**
     * Partial reader implementation for columns whose output type is Integer.
     * Concrete subclasses just need to implement one method.
     */
    private static abstract class IntReader extends AbstractReader<Integer> {
        IntReader() {
            super( Integer.class );
        }

        /**
         * Returns the value at a given index as an integer.
         *
         * @param ix  row index
         * @return  int value
         */
        abstract int get( long ix );

        public Integer getObject( long ix ) {
            return Integer.valueOf( get( ix ) );
        }
        public byte getByte( long ix ) {
            return (byte) get( ix );
        }
        public short getShort( long ix ) {
            return (short) get( ix );
        }
        public int getInt( long ix ) {
            return (int) get( ix );
        }
        public long getLong( long ix ) {
            return (long) get( ix );
        }
        public float getFloat( long ix ) {
            return (float) get( ix );
        }
        public double getDouble( long ix ) {
            return (double) get( ix );
        }
    }

    /**
     * Partial reader implementation for columns whose output type is Short.
     * Concrete subclasses just need to implement one method.
     */
    private static abstract class LongReader extends AbstractReader<Long> {
        LongReader() {
            super( Long.class );
        }

        /**
         * Returns the value at a given index as a long integer.
         *
         * @param ix  row index
         * @return  long value
         */
        abstract long get( long ix );

        public Long getObject( long ix ) {
            return Long.valueOf( get( ix ) );
        }
        public byte getByte( long ix ) {
            return (byte) get( ix );
        }
        public short getShort( long ix ) {
            return (short) get( ix );
        }
        public int getInt( long ix ) {
            return (int) get( ix );
        }
        public long getLong( long ix ) {
            return (long) get( ix );
        }
        public float getFloat( long ix ) {
            return (float) get( ix );
        }
        public double getDouble( long ix ) {
            return (double) get( ix );
        }
    }

    /**
     * Partial reader implementatio nfor variable length columns.
     * These have an nrow+1-element pointer array up front,
     * followed (on an 8-byte boundary) by the data bytes.
     */
    private static abstract class VariableLengthReader<T>
            extends NonNumericReader<T> {
        final int ptrSize_;
        final Buf buf_;
        final long data0_;

        /**
         * Constructor.
         *
         * @param  clazz  output class
         * @param  ptrSize  number of bytes in each pointer integer
         * @param  buf  buffer containing offset values first,
         *              and data values after
         * @param  nrow  row count
         */
        VariableLengthReader( Class<T> clazz, Buf buf, long nrow,
                              int ptrSize ) {
            super( clazz );
            buf_ = buf;
            ptrSize_ = ptrSize;
            data0_ = BufUtils.ceil8( ( nrow + 1 ) * ptrSize_ );
        }

        /**
         * Returns the pointer value stored at a given buffer offset.
         *
         * @param  ioff  buffer offset in bytes
         * @return   pointer value stored at offset
         */
        abstract long readPointer( long ioff );

        /**
         * Returns an array containing the bytes corresponding to
         * the value at a given row index.
         *
         * @param  ix  row index
         * @return   cell value as byte array
         */
        byte[] getBytes( long ix ) {
            long ioff0 = ix * ptrSize_;
            long doff0 = readPointer( ioff0 );
            long doff1 = readPointer( ioff0 + ptrSize_ );
            int leng = (int) Math.min( doff1 - doff0, Integer.MAX_VALUE );
            byte[] dbytes = new byte[ leng ];
            buf_.get( data0_ + doff0, dbytes );
            return dbytes;
        }
    }

    /**
     * Partial reader implementation for variable length value columns
     * using 4-byte offset arrays.
     */
    private static abstract class VariableLengthReader32<T>
            extends VariableLengthReader<T> {

        /**
         * Constructor.
         *
         * @param  clazz  output class
         * @param  buf  buffer containing offset values first,
         *              and data values after
         * @param  nrow  row count
         */
        VariableLengthReader32( Class<T> clazz, Buf buf, long nrow ) {
            super( clazz, buf, nrow, 4 );
        }

        long readPointer( long ioff ) {
            return buf_.getLittleEndianInt( ioff );
        }
    }

    /**
     * Partial reader implementation for variable length columns
     * using 8-byte offset arrays.
     */
    private static abstract class VariableLengthReader64<T>
            extends VariableLengthReader<T> {

        /**
         * Constructor.
         *
         * @param  clazz  output class
         * @param  buf  buffer containing offset values first,
         *              and data values after
         * @param  nrow  row count
         */
        VariableLengthReader64( Class<T> clazz, Buf buf, long nrow ) {
            super( clazz, buf, nrow, 8 );
        }

        long readPointer( long ioff ) {
            return buf_.getLittleEndianLong( ioff );
        }
    }

    /**
     * Dummy decoder implementation that doesn't work.
     */
    private static class UnsupportedDecoder extends Decoder<Void> {
        private static Reader<Void> dummyReader_;
        UnsupportedDecoder( FeatherType ftype ) {
            super( Void.class, ftype );
            dummyReader_ = new NonNumericReader<Void>( Void.class ) {
                public Void getObject( long ix ) {
                    return null;
                }
            };
        }
        public Reader<Void> createReader( Buf buf, long nrow ) {
            return dummyReader_;
        }
        @Override
        public String toString() {
            return getFeatherType() + "(unsupported)";
        }
    }
}
