package edu.caltech.nanodb.plans;


import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.Tuple;


/**
 * This class implements the basic sort-merge join algorithm for use in join
 * evaluation.  This join node is only useful for equijoins, but it has the
 * benefit that it can compute full outer joins easily, where the nested-loops
 * join algorithm is unable to do so.
 */
public class SortMergeJoinNode extends ThetaJoinNode {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(SortMergeJoinNode.class);


    /** Most recently retrieved tuple of the left relation. */
    private Tuple leftTuple;


    /** Most recently retrieved tuple of the right relation. */
    private Tuple rightTuple;


    /** Set to true when we have exhausted all tuples from our subplans. */
    private boolean done;


    public SortMergeJoinNode(PlanNode leftChild, PlanNode rightChild,
                             JoinType joinType, Expression predicate) {

        super(leftChild, rightChild, joinType, predicate);

        // Normally we would allow a join algorithm to have a null predicate,
        // but since sort-merge is an equijoin, and we can generate a cross
        // product easily with a nested-loop join, require that a predicate
        // be present.
        if (predicate == null) {
            throw new IllegalArgumentException("predicate cannot be null on" +
                " a sort-merge join");
        }
    }


    /**
     * Checks if the argument is a plan node tree with the same structure,
     * but not necessarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {

        if (obj instanceof SortMergeJoinNode) {
            SortMergeJoinNode other = (SortMergeJoinNode) obj;

            return predicate.equals(other.predicate) &&
                leftChild.equals(other.leftChild) &&
                rightChild.equals(other.rightChild);
        }

        return false;
    }


    /** Computes the hash-code of the sort-merge join plan node. */
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + predicate.hashCode();
        hash = 31 * hash + leftChild.hashCode();
        hash = 31 * hash + rightChild.hashCode();
        return hash;
    }


    /**
     * Returns a string representing this sort-merge join's vital information.
     *
     * @return a string representing this plan-node.
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("SortMergeJoin[");

        // The predicate is expected to be non-null.
        buf.append("pred:  ").append(predicate);

        if (schemaSwapped)
            buf.append(" (schema swapped)");

        buf.append(']');

        return buf.toString();
    }


    /** Creates a copy of this plan node and its subtrees. */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        SortMergeJoinNode node = (SortMergeJoinNode) super.clone();

        // Clone the predicate.
        node.predicate = predicate.duplicate();

        return node;
    }


    /**
     * Sort-merge join produces results in the same order as the children.
     * (That's kinda the point.)
     *
     * @return true always
     */
    @Override
    public List<OrderByExpression> resultsOrderedBy() {
        return leftChild.resultsOrderedBy();
    }


    /** This plan-node does not support marking. */
    @Override
    public boolean supportsMarking() {
        return false;
    }


    /** This plan-node does not require marking on the left child-plan. */
    @Override
    public boolean requiresLeftMarking() {
        return false;
    }


    /** This plan-node requires marking on the right child-plan. */
    @Override
    public boolean requiresRightMarking() {
        return true;
    }


    @Override
    public void prepare() {
        leftChild.prepare();
        rightChild.prepare();

        if (!rightChild.supportsMarking()) {
            throw new IllegalStateException("Sort-merge join requires the " +
                "right child-plan to support marking.");
        }

        // Get the schemas and the result-orderings so that we can analyze the
        // join-expressions.

        Schema leftSchema = leftChild.getSchema();
        List<OrderByExpression> leftOrder = leftChild.resultsOrderedBy();

        Schema rightSchema = rightChild.getSchema();
        List<OrderByExpression> rightOrder = rightChild.resultsOrderedBy();

        /* TODO:  Analyze the predicate and the result ordering of the child
         *        plans.  The constraints are as follows:
         *
         *        1)  The predicate must be a series of ANDed equality
         *            conditions.
         *
         *        2)  The columns in the predicate that reference the left
         *            child should be a prefix of the left child's order-by
         *            expression.  The same should hold for the right child.
         *            Further, each pair of values compared <c1> == <c2>
         *            should appear at the same index in the order-by lists.
         */

        throw new UnsupportedOperationException("Not yet implemented!");
    }


    @Override
    public void initialize() {
        super.initialize();

        done = false;
        leftTuple = null;
        rightTuple = null;
    }


    @Override
    public Tuple getNextTuple() throws IllegalStateException, IOException {
        // TODO:  Implement
        return null;
    }


    @Override
    public void markCurrentPosition() {
        throw new UnsupportedOperationException(
            "Sort-merge join plan-node doesn't support marking.");
    }


    @Override
    public void resetToLastMark() {
        throw new UnsupportedOperationException(
            "Sort-merge join plan-node doesn't support marking.");
    }


    @Override
    public void cleanUp() {
        leftChild.cleanUp();
        rightChild.cleanUp();
    }
}
