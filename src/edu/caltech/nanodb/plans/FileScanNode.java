package edu.caltech.nanodb.plans;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.qeval.ColumnStats;
import edu.caltech.nanodb.qeval.PlanCost;
import edu.caltech.nanodb.qeval.SelectivityEstimator;
import edu.caltech.nanodb.qeval.TableStats;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.TupleFile;
import edu.caltech.nanodb.storage.InvalidFilePointerException;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.OrderByExpression;

import edu.caltech.nanodb.qeval.SelectivityEstimator;

/**
 * A select plan-node that scans a tuple file, checking the optional predicate
 * against each tuple in the file.  Note that there are no optimizations used
 * if the tuple file is a sequential tuple file or a hashed tuple file.
 */
public class FileScanNode extends SelectNode {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(FileScanNode.class);


    private TableInfo tableInfo;


    /** The table to select from if this node is a leaf. */
    private TupleFile tupleFile;


    /**
     * This field allows the file-scan node to mark a particular tuple in the
     * tuple-stream and then rewind to that point in the tuple-stream.
     */
    private FilePointer markedTuple;


    private boolean jumpToMarkedTuple;


    /**
     * Construct a file scan node that traverses a table file.
     *
     * @param tableInfo the information about the table being scanned
     * @param predicate an optional predicate for selection, or {@code null}
     *        if all rows in the table should be included in the output
     */
    public FileScanNode(TableInfo tableInfo, Expression predicate) {
        super(predicate);

        if (tableInfo == null)
            throw new IllegalArgumentException("tableInfo cannot be null");

        this.tableInfo = tableInfo;
        tupleFile = tableInfo.getTupleFile();
    }


    /**
     * Returns true if the passed-in object is a <tt>FileScanNode</tt> with
     * the same predicate and table.
     *
     * @param obj the object to check for equality
     *
     * @return true if the passed-in object is equal to this object; false
     *         otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof FileScanNode) {
            FileScanNode other = (FileScanNode) obj;
            return tupleFile.equals(other.tupleFile) &&
                   predicate.equals(other.predicate);
        }

        return false;
    }


    /**
     * Computes the hashcode of a PlanNode.  This method is used to see if two
     * plan nodes CAN be equal.
     **/
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (predicate != null ? predicate.hashCode() : 0);
        hash = 31 * hash + tupleFile.hashCode();
        return hash;
    }


    /**
     * Creates a copy of this simple filter node node and its subtree.  This
     * method is used by {@link PlanNode#duplicate} to copy a plan tree.
     */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        FileScanNode node = (FileScanNode) super.clone();

        // The tuple file doesn't need to be copied since it's immutable.
        node.tupleFile = tupleFile;

        return node;
    }


    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("FileScan[");
        buf.append("table:  ").append(tableInfo.getTableName());

        if (predicate != null)
            buf.append(", pred:  ").append(predicate.toString());

        buf.append("]");

        return buf.toString();
    }


    /**
     * Currently we will always say that the file-scan node produces unsorted
     * results.  In actuality, a file scan's results will be sorted if the table
     * file uses a sequential format, but currently we don't have any sequential
     * file formats.
     */
    public List<OrderByExpression> resultsOrderedBy() {
        return null;
    }


    /** This node supports marking. */
    public boolean supportsMarking() {
        return true;
    }


    /** This node has no children so of course it doesn't require marking. */
    public boolean requiresLeftMarking() {
        return false;
    }


    /** This node has no children so of course it doesn't require marking. */
    public boolean requiresRightMarking() {
        return false;
    }


    protected void prepareSchema() {
        // Grab the schema from the table.
        schema = tupleFile.getSchema();
    }


    // Inherit javadocs from base class.
    public void prepare() {
        // Grab the schema and statistics from the table file.

        schema = tupleFile.getSchema();

        TableStats tableStats = tupleFile.getStats();
        stats = tableStats.getAllColumnStats();

        // Compute the cost of a filescan
        float totalTuples = tableStats.numTuples;
        float cpuCost = totalTuples;
        // If we have a predicate, we multiply by this value
        if (predicate != null) {
            // We use a selectivity estimator if necessary
            totalTuples *= SelectivityEstimator.estimateSelectivity(
                predicate, 
                schema, 
                stats);
        }
        cost = new PlanCost(
            // The number of tuples we will produce
            totalTuples, 
            // The size of a tuple
            tableStats.avgTupleSize, 
            // The total number of tuples (1 tuple = 1 CPU cost)
            cpuCost, 
            // numBlockIOs 
            tableStats.numDataPages);

        // TODO:  We should also update the table statistics based on the
        //        predicate, but that's too complicated, so we'll leave them
        //        unchanged for now.
    }


    public void initialize() {
        super.initialize();

        // Reset our marking state.
        markedTuple = null;
        jumpToMarkedTuple = false;
    }


    public void cleanUp() {
        // Nothing to do!
    }


    /**
     * Advances the current tuple forward for a file scan. Grabs the first tuple
     * if current is null. Otherwise gets the next tuple.
     *
     * @throws java.io.IOException if the TableManager failed to open the table.
     */
    protected void advanceCurrentTuple() throws IOException {

        if (jumpToMarkedTuple) {
            logger.debug("Resuming at previously marked tuple.");
            try {
                currentTuple = tupleFile.getTuple(markedTuple);
            }
            catch (InvalidFilePointerException e) {
                throw new IOException(
                    "Couldn't resume at previously marked tuple!", e);
            }
            jumpToMarkedTuple = false;

            return;
        }

        if (currentTuple == null)
            currentTuple = tupleFile.getFirstTuple();
        else
            currentTuple = tupleFile.getNextTuple(currentTuple);
    }


    public void markCurrentPosition() {
        if (currentTuple == null)
            throw new IllegalStateException("There is no current tuple!");

        logger.debug("Marking current position in tuple-stream.");
        markedTuple = currentTuple.getExternalReference();
    }


    public void resetToLastMark() {
        if (markedTuple == null)
            throw new IllegalStateException("There is no last-marked tuple!");

        logger.debug("Resetting to previously marked position in tuple-stream.");
        jumpToMarkedTuple = true;
    }
}
