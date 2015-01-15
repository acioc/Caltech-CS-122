package edu.caltech.nanodb.storage;

import java.io.IOException;
import java.util.List;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.relations.Tuple;


/**
 * This interface extends the {@link TupleFile} interface, adding operations
 * that can be provided on files of tuples that are hashed on a specific key.
 * The key used to hash tuples is returned by the {@link #getKeySpec} method.
 */
public interface HashedTupleFile extends TupleFile {
    /**
     * Returns the column(s) that comprise the hash key in this tuple file.
     *
     * @return the column(s) that comprise the hash key in this tuple file.
     */
    List<Expression> getKeySpec();


    /**
     * Returns the first tuple in the file that has the same hash-key value,
     * or {@code null} if there are no tuples with this hash-key value in
     * the tuple file.
     *
     * @param searchKey
     * @return
     * @throws IOException
     */
    Tuple getFirstTupleEquals(Tuple searchKey) throws IOException;


    /**
     * Returns the next entry in the index that has the same hash-key value,
     * or {@code null} if there are no more entries with this hash-key value
     * in the index.
     *
     * @return
     * @throws IOException
     */
    Tuple getNextTupleEquals(Tuple indexEntry) throws IOException;
}
