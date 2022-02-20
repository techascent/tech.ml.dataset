package uk.ac.bristol.star.feather;

/**
 * Characterises what was written by a column writer.
 *
 * @author   Mark Taylor
 * @since    26 Feb 2020
 */
public interface ColStat {

    /**
     * Returns the number of rows that were written.
     *
     * @return  row count
     */
    long getRowCount();

    /**
     * Returns the total number of bytes that were written.
     * Note this must be a multiple of 8.
     *
     * @return   byte count
     */
    long getByteCount();

    /**
     * Returns the number of items written that were identified as
     * nulls by the prepended validity mask.
     *
     * @return  null count
     */
    long getNullCount();

    /**
     * Returns the offset of the data block from the start of the total
     * bytes written.  This offset skips the validity mask if present,
     * but does not skip the length table if present.
     * Note this must be a multiple of 8.
     *
     * @return  data offset
     */
    long getDataOffset();
}
