package uk.ac.bristol.star.feather;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import uk.ac.bristol.star.fbs.feather.CTable;
import uk.ac.bristol.star.fbs.feather.Column;
import uk.ac.bristol.star.fbs.feather.PrimitiveArray;
import uk.ac.bristol.star.fbs.google.FlatBufferBuilder;

/**
 * Can write table data to a Feather-format file.
 *
 * @author   Mark Taylor
 * @since    26 Feb 2020
 */
public class FeatherWriter {

    /** Supported version of the Feather format. */
    public static final int FEATHER_VERSION = 1;

    private final String description_;
    private final String tableUserMeta_;
    private final FeatherColumnWriter[] colWriters_;

    /**
     * Constructor.
     *
     * @param  description  optional table description string
     * @param  tableMeta  optional user metadata string, should probably be JSON
     * @param  colWriters  objects that can write column data
     */
    public FeatherWriter( String description, String tableMeta,
                          FeatherColumnWriter[] colWriters ) {
        description_ = description;
        tableUserMeta_ = tableMeta;
        colWriters_ = colWriters;
    }

    /**
     * Serializes this writer's table to a given output stream.
     *
     * @param out  destination stream
     */
    public void write( OutputStream out ) throws IOException {
        out = new BufferedOutputStream( out );
        long baseOffset = 0;
        baseOffset += writeLittleEndianInt( out, FeatherTable.MAGIC );
        baseOffset += writeLittleEndianInt( out, 0 );
        assert baseOffset % 8 == 0;
        int nc = colWriters_.length;
        ColStat[] colStats = new ColStat[ nc ];
        for ( int ic = 0; ic < nc; ic++ ) {
            colStats[ ic ] = colWriters_[ ic ].writeColumnBytes( out );
            assert colStats[ ic ].getByteCount() % 8 == 0;
        }
        byte[] metablock = createMetadataBlock( colStats, baseOffset );
        int metaSize = metablock.length;
        assert metaSize % 8 == 0;
        out.write( metablock );
        writeLittleEndianInt( out, metaSize );
        writeLittleEndianInt( out, FeatherTable.MAGIC );
        out.flush();
    }

    /**
     * Constructs the metadata block for this writer's table
     * as required by Feather format.
     *
     * @param  colStats  details about how the table's columns were
     *                   serialised
     * @param  baseOffset  offset of the start of the table data area
     *                     within the file
     * @return   metadata block as a byte array
     */
    private byte[] createMetadataBlock( ColStat[] colStats, long baseOffset ) {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int descriptionTag = createStringTag( builder, description_ );
        int tableUserMetaTag = createStringTag( builder, tableUserMeta_ );
        int nc = colWriters_.length;
        int[] colTags = new int[ nc ];
        long streamOffset = baseOffset;
        long nrow = 0;
        for ( int ic = 0; ic < nc; ic++ ) {
            FeatherColumnWriter colWriter = colWriters_[ ic ];
            ColStat colStat = colStats[ ic ];
            long nr = colStat.getRowCount();
            long nnull = colStat.getNullCount();
            nrow = ic == 0 ? nr : Math.min( nr, nrow );
            long internalOffset = nnull == 0
                                ? colStat.getDataOffset()
                                : 0;
            long offset = streamOffset + internalOffset;
            long nbyte = colStat.getByteCount() - internalOffset;
            streamOffset += colStat.getByteCount();
            PrimitiveArray.startPrimitiveArray( builder );
            PrimitiveArray.addType( builder,
                                    colWriter.getFeatherType().getTypeByte() );
            PrimitiveArray.addOffset( builder, offset );
            PrimitiveArray.addLength( builder, nr );
            PrimitiveArray.addNullCount( builder, nnull );
            PrimitiveArray.addTotalBytes( builder, nbyte );
            int valuesTag = PrimitiveArray.endPrimitiveArray( builder );

            int colNameTag = createStringTag( builder, colWriter.getName() );
            int colUserMetaTag =
                createStringTag( builder, colWriter.getUserMetadata() );
            Column.startColumn( builder );
            Column.addName( builder, colNameTag );
            Column.addUserMetadata( builder, colUserMetaTag );
            Column.addValues( builder, valuesTag );
            colTags[ ic ] = Column.endColumn( builder );
        }
        int columnsTag = CTable.createColumnsVector( builder, colTags );
        CTable.startCTable( builder );
        CTable.addVersion( builder, FEATHER_VERSION );
        CTable.addNumRows( builder, nrow );
        CTable.addDescription( builder, descriptionTag );
        CTable.addMetadata( builder, tableUserMetaTag );
        CTable.addColumns( builder, columnsTag );
        int ctableTag = CTable.endCTable( builder );
        CTable.finishCTableBuffer( builder, ctableTag );
        return builder.sizedByteArray();
    }

    /**
     * Returns a flatbuffer tag for a string that may be null.
     *
     * @param  builder  flatbuffer builder
     * @param  txt   string value, may be null
     * @return  tag suitable for insersion in flatbuffer
     */
    private static int createStringTag( FlatBufferBuilder builder,
                                        String txt ) {
        return txt == null ? 0 : builder.createString( txt );
    }

    /**
     * Writes a little-endian integer and returns the number of bytes written.
     *
     * @param  out  destination stream
     * @param  ivalue  value to write
     * @return  4
     */
    private static int writeLittleEndianInt( OutputStream out, int ivalue )
            throws IOException {
        BufUtils.writeLittleEndianInt( out, ivalue );
        return 4;
    }

    /**
     * Test method.  Writes a short feather table to standard output.
     */
    public static void main( String[] args ) throws IOException {
        int nrow = args.length > 0 ? Integer.parseInt( args[ 0 ] ) : 5;
        short[] sdata = new short[ nrow ];
        int[] idata = new int[ nrow ];
        long[] ldata = new long[ nrow ];
        float[] fdata = new float[ nrow ];
        double[] ddata = new double[ nrow ];
        String[] tdata = new String[ nrow ];
        String[] txts = { "zero", "one", null, "three", "", };
        for ( int ir = 0; ir < nrow; ir++ ) {
            sdata[ ir ] = (short) ir;
            idata[ ir ] = -ir;
            ldata[ ir ] = 4000000000L + ir;
            fdata[ ir ] = ir == 2 ? Float.NaN : 0.25f + ir;
            ddata[ ir ] = ir == 2 ? Double.NaN : 0.5 + ir;
            tdata[ ir ] = txts[ ir % txts.length ];
        }
        FeatherColumnWriter[] writers = {
            NumberRandomWriter.createShortWriter( "scol", sdata, null ),
            NumberRandomWriter.createIntWriter( "icol", idata, "int col" ),
            NumberRandomWriter.createLongWriter( "lcol", ldata, null ),
            NumberRandomWriter.createFloatWriter( "fcol", fdata, null ),
            NumberRandomWriter.createDoubleWriter( "dcol", ddata, null ),
            VariableLengthRandomWriter.createStringWriter( "tcol", tdata,
                                                           null, false ),
        };
        new FeatherWriter( "test table", null, writers )
           .write( System.out );
    }
}
