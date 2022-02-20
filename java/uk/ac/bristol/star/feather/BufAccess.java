package uk.ac.bristol.star.feather;

/**
 * Basic interface for access to a potentially large byte storage array.
 *
 * @author   Mark Taylor
 * @since    25 Feb 2020
 */
public interface BufAccess {

    /**
     * Retrieves a byte from a given buffer offset.
     *
     * @param  ix  offset
     * @return  byte value
     */
    byte get( long ix );
}
