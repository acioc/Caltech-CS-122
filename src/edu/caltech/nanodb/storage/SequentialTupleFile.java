package edu.caltech.nanodb.storage;


import java.io.IOException;
import java.util.List;

import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.relations.Tuple;


/**
 * This interface extends the {@link TupleFile} interface, adding operations
 * that can be provided on files of tuples that are stored in a specific
 * sequential order.  The {@link #getFirstTuple} and {@link #getNextTuple}
 * methods have an additional constraint that they traverse the file's tuples
 * in a specific order, as specified by the {@link #getOrderSpec} method.
 */
public interface SequentialTupleFile extends TupleFile {

    /**
     * Returns the column(s) that are used to order the tuples in this
     * sequential file.
     *
     * @return the column(s) that are used to order the tuples in this
     *         sequential file.
     */
    List<OrderByExpression> getOrderSpec();


    /**
     * Returns the first tuple in the file that has the same search-key value,
     * or {@code null} if there are no tuples with this search-key value in
     * the tuple file.
     *
     * @param searchKey
     * @return
     * @throws IOException
     */
    Tuple getFirstTupleEquals(Tuple searchKey) throws IOException;


    /**
     * Returns the first tuple in the file that has a search-key value that
     * is greater than the specified search-key value, or {@code null} if
     * there are no tuples greater than this search-key value in the index.
     *
     * @param searchKey
     * @return
     * @throws IOException
     */
    Tuple getFirstTupleGreaterThan(Tuple searchKey) throws IOException;
}
