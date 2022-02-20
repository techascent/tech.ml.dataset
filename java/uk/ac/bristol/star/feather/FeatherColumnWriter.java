package uk.ac.bristol.star.feather;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Supplies the metadata and byte stream associated with serializing
 * a column of a Feather-format table.
 * The best way to implement this interface will depend on how
 * your data is laid out.
 *
 * @author   Mark Taylor
 * @since    26 Feb 2020
 */
public interface FeatherColumnWriter {

    /**
     * Returns the column name.
     *
     * @return  column name
     */
    String getName();

    /**
     * Returns the column type code.
     *
     * @return  data type
     */
    FeatherType getFeatherType();

    /**
     * Returns text to use as column user metadata.
     * The feather format definition notes:
     * "<em>This should (probably) be JSON</em>".
     *
     * @return  user metadata (JSON?) string, or null
     */
    String getUserMetadata();

    /**
     * Writes the byte stream containing the data for this column,
     * according to the Feather file format specification.
     * This may include a prepended validity byte mask.
     *
     * <p><b>Note:</b> the number of bytes written must be a multiple of 8,
     * in accordance with Feather/Arrow's alignment requirements.
     *
     * @param  out  destination stream
     * @return  information about what was actually written
     */
    ColStat writeColumnBytes( OutputStream out ) throws IOException;
}
