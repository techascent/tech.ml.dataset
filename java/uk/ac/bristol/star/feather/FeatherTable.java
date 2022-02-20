package uk.ac.bristol.star.feather;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import uk.ac.bristol.star.fbs.feather.CTable;
import uk.ac.bristol.star.fbs.feather.Column;
import uk.ac.bristol.star.fbs.feather.PrimitiveArray;

/**
 * Presents a readable view of a feather-format table on disk.
 *
 * @author   Mark Taylor
 * @since    26 Feb 2020
 */
public class FeatherTable {

    private final CTable ctable_;
    private final FileChannel channel_;
    private final int ncol_;
    private final long nrow_;
    private final FeatherColumn[] columns_;

    /** Feather magic number as a little-endian integer. */
    public static final int MAGIC = ( 'F' & 0xff ) <<  0
                                  | ( 'E' & 0xff ) <<  8
                                  | ( 'A' & 0xff ) << 16
                                  | ( '1' & 0xff ) << 24;

    /**
     * Constructor.
     *
     * @param  ctable  flatbuffer metadata object describing file layout
     * @param  channel   file containing feather-format data
     */
    public FeatherTable( CTable ctable, FileChannel channel ) {
        ctable_ = ctable;
        channel_ = channel;
        ncol_ = ctable.columnsLength();
        nrow_ = ctable.numRows();
        columns_ = new FeatherColumn[ ncol_ ];
        for ( int ic = 0; ic < ncol_; ic++ ) {
            Column col = ctable.columns( ic );
            String cname = col.name();
            PrimitiveArray parr = col.values();
            byte type = parr.type();
            long nNull = parr.nullCount();
            String userMeta = col.userMetadata();
            byte ptype = parr.type();
            FeatherType ftype = FeatherType.fromByte( ptype );
            Decoder<?> decoder = Decoder.getDecoder( ftype );
            BufMapper mapper =
                new BufMapper( channel, parr.offset(), parr.totalBytes() );
            columns_[ ic ] = new FeatherColumn( cname, nrow_, mapper, decoder,
                                                nNull, userMeta );
        }
    }

    /**
     * Returns the number of columns in this table.
     *
     * @return  column count
     */
    public int getColumnCount() {
        return ncol_;
    }

    /**
     * Returns the number of rows in this table.
     *
     * @return  row count
     */
    public long getRowCount() {
        return nrow_;
    }

    /**
     * Returns one of the columns.
     *
     * @param  icol  column index
     * @return   column object
     */
    public FeatherColumn getColumn( int icol ) {
        return columns_[ icol ];
    }

    /**
     * Returns the table description.
     *
     * @return  description
     */
    public String getDescription() {
        return ctable_.description();
    }

    /**
     * Returns the table user metadata string, possibly in JSON format.
     *
     * @return   metadata string, possibly JSON, or null
     */
    public String getMetadata() {
        return ctable_.metadata();
    }

    /**
     * Returns the version of the Feather format used by this table.
     *
     * @return  feather version
     */
    public int getFeatherVersion() {
        return ctable_.version();
    }

    /**
     * Frees resources associated with this table.
     */
    public void close() throws IOException {
        channel_.close();
    }

    @Override
    public String toString() {
        StringBuffer sbuf = new StringBuffer()
           .append( "Feather" )
           .append( " (" )
           .append( ncol_ )
           .append( "x" )
           .append( nrow_ )
           .append( ")" )
           .append( ": " );
        for ( int ic = 0; ic < ncol_; ic++ ) {
            if ( ic > 0 ) {
                sbuf.append( ", " );
            }
            sbuf.append( getColumn( ic ) );
        }
        return sbuf.toString();
    }

    /**
     * Constructs a FeatherTable from a feather-format file on disk.
     *
     * @param  file  file
     * @return   table
     */
    public static FeatherTable fromFile( File file ) throws IOException {
        RandomAccessFile raf = new RandomAccessFile( file, "r" );
        long leng = raf.length();
        int magic1 = BufUtils.readLittleEndianInt( raf );
        if ( magic1 != MAGIC ) {
            throw new IOException( "Not FEA1 magic number at file start" );
        }
        raf.seek( leng - 8 );
        int metaLeng = BufUtils.readLittleEndianInt( raf );
        int magic2 = BufUtils.readLittleEndianInt( raf );
        if ( magic2 != MAGIC ) {
            throw new IOException( "Not FEA1 magic number at file start" );
        }
        long metaStart = leng - 8 - metaLeng;
        byte[] metabuf = new byte[ metaLeng ];
        raf.seek( metaStart );
        raf.readFully( metabuf );
        CTable ctable = CTable.getRootAsCTable( ByteBuffer.wrap( metabuf ) );
        return new FeatherTable( ctable, raf.getChannel() );
    }

    /**
     * Indicates whether the first 4 bytes of a given byte array
     * spell out the FEA1 magic number required at the start and end
     * of Feather-format files.
     *
     * @param  intro  start of file
     * @return  true iff intro starts with FEA1 magic number
     */
    public static boolean isMagic( byte[] intro ) {
        if ( intro.length < 4 ) {
            return false;
        }
        return intro.length >= 4
            && MAGIC == ( ( intro[ 0 ] & 0xff ) <<  0
                        | ( intro[ 1 ] & 0xff ) <<  8
                        | ( intro[ 2 ] & 0xff ) << 16
                        | ( intro[ 3 ] & 0xff ) << 24 );
    }

    /**
     * Utility main method: writes the content of a feather file named
     * on the command line to standard output.
     */
    public static void main( String[] args ) throws IOException {
        FeatherTable ft = FeatherTable.fromFile( new File( args[ 0 ] ) );
        System.out.println( ft );
        int ncol = ft.getColumnCount();
        Reader<?>[] rdrs = new Reader<?>[ ncol ];
        System.out.print( "#" );
        for ( int ic = 0; ic < ncol; ic++ ) {
            FeatherColumn fcol = ft.getColumn( ic );
            rdrs[ ic ] = fcol.createReader();
            System.out.print( "\t" + fcol.getName() );
        }
        System.out.println();
        long nrow = ft.getRowCount();
        for ( long ir = 0; ir < nrow; ir++ ) {
            for ( int ic = 0; ic < ncol; ic++ ) {
                System.out.print( "\t" + rdrs[ ic ].getObject( ir ) );
            }
            System.out.println();
        }
    }
}
