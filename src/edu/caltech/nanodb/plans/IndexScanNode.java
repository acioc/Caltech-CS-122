package edu.caltech.nanodb.plans;


import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.ObjectUtils;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.expressions.TupleComparator;
import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.indexes.IndexInfo;
import edu.caltech.nanodb.indexes.IndexManager;
import edu.caltech.nanodb.indexes.IndexUtils;
import edu.caltech.nanodb.qeval.TableStats;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.InvalidFilePointerException;
import edu.caltech.nanodb.storage.SequentialTupleFile;
import edu.caltech.nanodb.storage.TupleFile;


/**
 * Created by donnie on 3/9/15.
 */
public class IndexScanNode extends PlanNode {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(IndexScanNode.class);


    /** The type of the index scan. */
    public static enum ScanType {
        /** The scan type is an equality scan. */
        EQUALITY,

        /** The scan type is a range scan. */
        RANGE
    }


    /**
     * The index-info for the index being scanned, or {@code null} if the node
     * is performing a scan over a table.
     */
    private IndexInfo indexInfo;


    /** The index being used for the index scan. */
    private TupleFile indexTupleFile;


    /** The table that the index is built against. */
    private TupleFile tableTupleFile;


    private ScanType scanType;


    private TupleLiteral value1 = null;


    private TupleLiteral value2 = null;


    private boolean includeValue1;


    private boolean includeValue2;


    /**
     * The current tuple from the index that is being used.  Note that this
     * tuple is not what {@link #getNextTuple} returns; rather, it's the tuple
     * that this index record refers to.
     */
    protected Tuple currentIndexTuple;


    /**
     * The index must have one column named "<tt>#TUPLE_PTR</tt>"; this is the
     * column-index of that column in the index's schema.
     */
    private int idxTuplePtr;


    /** True if we have finished scanning or pulling tuples from children. */
    private boolean done;


    /**
     * This field allows the index-scan node to mark a particular tuple in the
     * tuple-stream and then rewind to that point in the tuple-stream.
     */
    private FilePointer markedTuple;


    private boolean jumpToMarkedTuple;


    /**
     * Construct an index scan node that performs an equality-based lookup on
     * an index.
     *
     * @param indexInfo the information about the index being used
     * @param searchKey the search key to use for equality-based lookup on the
     *        index
     */
    public IndexScanNode(IndexInfo indexInfo, TupleLiteral searchKey) {
        super(OperationType.SELECT);

        if (indexInfo == null)
            throw new IllegalArgumentException("indexInfo cannot be null");

        this.indexInfo = indexInfo;
        indexTupleFile = indexInfo.getTupleFile();
        tableTupleFile = indexInfo.getTableInfo().getTupleFile();

        Schema idxSchema = indexTupleFile.getSchema();
        idxTuplePtr = idxSchema.getColumnIndex(IndexManager.COLNAME_TUPLEPTR);
        if (idxTuplePtr == -1) {
            throw new IllegalArgumentException("Index must have a column " +
                "named " + IndexManager.COLNAME_TUPLEPTR);
        }

        this.scanType = ScanType.EQUALITY;

        this.value1 = searchKey;
    }


    public IndexScanNode(IndexInfo indexInfo, TupleLiteral lowerValue,
                         boolean includeLower, TupleLiteral upperValue,
                         boolean includeUpper) {
        super(OperationType.SELECT);

        if (indexInfo == null)
            throw new IllegalArgumentException("indexInfo cannot be null");

        this.indexInfo = indexInfo;
        indexTupleFile = indexInfo.getTupleFile();

        if (!(indexTupleFile instanceof SequentialTupleFile)) {
            throw new IllegalArgumentException("For RANGE scans, index " +
                "must use a sequential tuple file.");
        }

        tableTupleFile = indexInfo.getTableInfo().getTupleFile();

        Schema idxSchema = indexTupleFile.getSchema();
        idxTuplePtr = idxSchema.getColumnIndex(IndexManager.COLNAME_TUPLEPTR);
        if (idxTuplePtr == -1) {
            throw new IllegalArgumentException("Index must have a column " +
                "named " + IndexManager.COLNAME_TUPLEPTR);
        }

        this.scanType = ScanType.RANGE;

        this.value1 = lowerValue;
        this.value2 = upperValue;

        this.includeValue1 = includeLower;
        this.includeValue2 = includeUpper;
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
        if (obj instanceof IndexScanNode) {
            IndexScanNode other = (IndexScanNode) obj;
            // We don't include the table-info or the index-info since each
            // table or index is in its own tuple file.
            return indexTupleFile.equals(other.indexTupleFile) &&
                scanType == other.scanType &&
                ObjectUtils.equals(value1, other.value1) &&
                ObjectUtils.equals(value2, other.value2) &&
                includeValue1 == other.includeValue1 &&
                includeValue2 == other.includeValue2;
        }

        return false;
    }


    /**
     * Computes the hashcode of a PlanNode.  This method is used to see if two
     * plan nodes CAN be equal.
     **/
    public int hashCode() {
        int hash = 7;
        // We don't include the index-info since each index is in its own
        // tuple file.
        hash = 31 * hash + indexTupleFile.hashCode();
        hash = 31 * hash + scanType.hashCode();
        hash = 31 * hash + ObjectUtils.hashCode(value1);
        hash = 31 * hash + ObjectUtils.hashCode(value2);
        hash = 31 * hash + (includeValue1 ? 1 : 0);
        hash = 31 * hash + (includeValue2 ? 1 : 0);
        return hash;
    }


    /**
     * Creates a copy of this simple filter node node and its subtree.  This
     * method is used by {@link PlanNode#duplicate} to copy a plan tree.
     */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        IndexScanNode node = (IndexScanNode) super.clone();

        // TODO:  Should we clone these?
        node.indexInfo = indexInfo;

        // The tuple file doesn't need to be copied since it's immutable.
        node.indexTupleFile = indexTupleFile;

        return node;
    }


    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("IndexScan[");
        buf.append("index:  ").append(indexInfo.getTableName());
        buf.append('.').append(indexInfo.getIndexName());

        buf.append(", type:  ").append(scanType);

        if (scanType == ScanType.EQUALITY) {
            buf.append(", value=").append(value1);
        }
        else if (scanType == ScanType.RANGE) {
            if (value1 != null) {
                buf.append(", lower=").append(value1);
                buf.append(includeValue1 ? " (inclusive)" : " (exclusive)");
            }

            if (value2 != null) {
                buf.append(", upper=").append(value2);
                buf.append(includeValue2 ? " (inclusive)" : " (exclusive)");
            }
        }

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
        schema = indexTupleFile.getSchema();
    }


    // Inherit javadocs from base class.
    public void prepare() {
        // Grab the schema and statistics from the table file.

        schema = tableTupleFile.getSchema();

        // TODO:  We should also update the table statistics based on what the
        //        index scan is going to do, but that's too complicated, so
        //        we'll leave them unchanged for now.
        TableStats tableStats = tableTupleFile.getStats();
        stats = tableStats.getAllColumnStats();

        // TODO:  Cost the plan node
        cost = null;
    }


    @Override
    public void initialize() {
        super.initialize();

        currentIndexTuple = null;
        done = false;

        // Reset our marking state.
        markedTuple = null;
        jumpToMarkedTuple = false;
    }


    @Override
    public Tuple getNextTuple() throws IllegalStateException, IOException {

        if (done)
            return null;

        if (jumpToMarkedTuple) {
            logger.debug("Resuming at previously marked tuple.");
            try {
                currentIndexTuple = indexTupleFile.getTuple(markedTuple);
            }
            catch (InvalidFilePointerException e) {
                throw new IOException(
                    "Couldn't resume at previously marked tuple!", e);
            }
            jumpToMarkedTuple = false;
        }
        else if (currentIndexTuple == null) {
            // Navigate to the first tuple.
            currentIndexTuple = findFirstTuple();
        }
        else {
            // Go ahead and navigate to the next tuple.
            currentIndexTuple = findNextTuple(currentIndexTuple);
            if (currentIndexTuple == null)
                done = true;
        }

        // Now, look up the table tuple based on the index tuple's
        // file-pointer.
        FilePointer tuplePtr =
            (FilePointer) currentIndexTuple.getColumnValue(idxTuplePtr);
        Tuple tableTuple;
        try {
            tableTuple = tableTupleFile.getTuple(tuplePtr);
        }
        catch (InvalidFilePointerException e) {
            throw new IOException(
                "Couldn't retrieve table-tuple referenced by index!", e);
        }

        return tableTuple;
    }


    private Tuple findFirstTuple() throws IOException {
        Tuple firstTuple;

        if (scanType == ScanType.EQUALITY) {
            firstTuple = IndexUtils.findTupleInIndex(value1, indexTupleFile);
        }
        else if (scanType == ScanType.RANGE) {
            SequentialTupleFile seqTupleFile = (SequentialTupleFile) indexTupleFile;
            if (includeValue1)
                firstTuple = seqTupleFile.findFirstTupleEquals(value1);
            else
                firstTuple = seqTupleFile.findFirstTupleGreaterThan(value1);
        }
        else {
            throw new IllegalStateException(
                "scanType must be EQUALITY or RANGE!  Got " + scanType);
        }

        return firstTuple;
    }


    private Tuple findNextTuple(Tuple tuple) throws IOException {
        Tuple nextTuple = indexTupleFile.getNextTuple(tuple);
        if (nextTuple != null) {
            if (scanType == ScanType.EQUALITY) {
                // Make sure the next tuple is equal to the search-key
                // value.
                int cmp = TupleComparator.comparePartialTuples(nextTuple, value1);
                if (cmp != 0)
                    nextTuple = null;
            }
            else if (scanType == ScanType.RANGE) {
                int cmp = TupleComparator.comparePartialTuples(nextTuple, value2);
                if (cmp > 0 || (cmp == 0 && !includeValue2))
                    nextTuple = null;
            }
            else {
                throw new IllegalStateException(
                    "scanType must be EQUALITY or RANGE!  Got " + scanType);
            }
        }

        return nextTuple;
    }


    public void cleanUp() {
        // Nothing to do!
    }


    public void markCurrentPosition() {
        if (currentIndexTuple == null)
            throw new IllegalStateException("There is no current tuple!");

        logger.debug("Marking current position in tuple-stream.");
        markedTuple = currentIndexTuple.getExternalReference();
    }


    public void resetToLastMark() {
        if (markedTuple == null)
            throw new IllegalStateException("There is no last-marked tuple!");

        logger.debug("Resetting to previously marked position in tuple-stream.");
        jumpToMarkedTuple = true;
    }
}
