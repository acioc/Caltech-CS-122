package edu.caltech.nanodb.plans;


import java.io.IOException;
import java.util.List;

import edu.caltech.nanodb.expressions.TupleLiteral;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.qeval.PlanCost;
import edu.caltech.nanodb.qeval.SelectivityEstimator;
import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.Tuple;


/**
 * This plan node implements a nested-loops join operation, which can support
 * arbitrary join conditions but is also the slowest join implementation.
 */
public class NestedLoopsJoinNode extends ThetaJoinNode {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(SortMergeJoinNode.class);


    /** Most recently retrieved tuple of the left relation. */
    private Tuple leftTuple;

    /** Most recently retrieved tuple of the right relation. */
    private Tuple rightTuple;


    /** Set to true when we have exhausted all tuples from our subplans. */
    private boolean done;

    /** set to true if the join type is semi or anti*/
    private boolean sem_ant;

    /** Set to true if the join type is an outer join*/
    private boolean outer;

    /** Set in the prepare method to a all null tuple to be joined to null-pad tuples*/
    private TupleLiteral nullPad;

    /** state variable used to keep track of if corresponding tuples have been joined*/
    private boolean found;

    public NestedLoopsJoinNode(PlanNode leftChild, PlanNode rightChild,
                JoinType joinType, Expression predicate) {

        super(leftChild, rightChild, joinType, predicate);
    }


    /**
     * Checks if the argument is a plan node tree with the same structure, but not
     * necessarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {

        if (obj instanceof NestedLoopsJoinNode) {
            NestedLoopsJoinNode other = (NestedLoopsJoinNode) obj;

            return predicate.equals(other.predicate) &&
                leftChild.equals(other.leftChild) &&
                rightChild.equals(other.rightChild);
        }

        return false;
    }


    /** Computes the hash-code of the nested-loops plan node. */
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (predicate != null ? predicate.hashCode() : 0);
        hash = 31 * hash + leftChild.hashCode();
        hash = 31 * hash + rightChild.hashCode();
        return hash;
    }


    /**
     * Returns a string representing this nested-loop join's vital information.
     *
     * @return a string representing this plan-node.
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("NestedLoops[");

        if (predicate != null)
            buf.append("pred:  ").append(predicate);
        else
            buf.append("no pred");

        if (schemaSwapped)
            buf.append(" (schema swapped)");

        buf.append(']');

        return buf.toString();
    }


    /**
     * Creates a copy of this plan node and its subtrees.
     */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        NestedLoopsJoinNode node = (NestedLoopsJoinNode) super.clone();

        // Clone the predicate.
        if (predicate != null)
            node.predicate = predicate.duplicate();
        else
            node.predicate = null;

        return node;
    }


    /**
     * Nested-loop joins can conceivably produce sorted results in situations
     * where the outer relation is ordered, but we will keep it simple and just
     * report that the results are not ordered.
     */
    @Override
    public List<OrderByExpression> resultsOrderedBy() {
        return null;
    }


    /** True if the node supports position marking. **/
    public boolean supportsMarking() {
        return leftChild.supportsMarking() && rightChild.supportsMarking();
    }


    /** True if the node requires that its left child supports marking. */
    public boolean requiresLeftMarking() {
        return false;
    }


    /** True if the node requires that its right child supports marking. */
    public boolean requiresRightMarking() {
        return false;
    }


    @Override
    public void prepare() {
        // Need to prepare the left and right child-nodes before we can do
        // our own work.
        leftChild.prepare();
        rightChild.prepare();

        // Use the parent class' helper-function to prepare the schema.
        prepareSchemaStats();

        /** if the join is an anti or semi join, must change the schema to that of the left child*/
        if (joinType == JoinType.ANTIJOIN || joinType == JoinType.SEMIJOIN) {
            schema = leftChild.getSchema();
            sem_ant = true;
            outer = false;
        }
        /** If the join type is an outer, sets state variables and creates the null-pad tuple*/
        else if (joinType == JoinType.LEFT_OUTER || joinType == JoinType.RIGHT_OUTER) {
            outer = true;
            sem_ant = false;
            if (joinType == JoinType.LEFT_OUTER)
                nullPad = new TupleLiteral(rightChild.getSchema().numColumns());
            else {
                nullPad = new TupleLiteral(leftChild.getSchema().numColumns());
            }
        }
        /** if the join is an inner join*/
        else {
            sem_ant = false;
            outer = false;
        }
        
        // We obtain the cost of our children
        PlanCost lChildCost = leftChild.getCost();
        PlanCost rChildCost = rightChild.getCost();
        
        // Our base number of tuples is the two tuples multiplied together
        float totalTuples = lChildCost.numTuples * rChildCost.numTuples;
        
        // If we have a predicate, we multiply by this value
        if (predicate != null) {
        	// We use a selectivity estimator if necessary
        	float selValue = SelectivityEstimator.estimateSelectivity(
        			predicate, 
        			schema, 
        			stats);
        	
        	// Anti-joins will have the opposite of our estimated selectivity
            if (joinType == JoinType.ANTIJOIN || 
            		joinType == JoinType.SEMIJOIN) {
            	selValue = 1 - selValue;
            }
            totalTuples *= selValue;
        }
        // Outer joins require extra nodes for their bounds
        if (joinType == JoinType.LEFT_OUTER) {
        	totalTuples += lChildCost.numTuples;
        }
        else if (joinType == JoinType.RIGHT_OUTER) {
        	totalTuples += rChildCost.numTuples;
        }
        
        cost = new PlanCost(
        		// We have our total number of tuples
        		totalTuples,
        		// Our overall tuple size is now the size of the two
        		// tuples added together
        		lChildCost.tupleSize + rChildCost.tupleSize,
        		// Our CPU cost is the cost of going through every tuple
        		// on the right child for every tuple on the left
        		// and for going through every left child tuple
        		lChildCost.numTuples * rChildCost.cpuCost + lChildCost.cpuCost,
                // Our block cost is the cost of going through the blocks
                // in the two children
                lChildCost.numBlockIOs + rChildCost.numBlockIOs);
    }


    public void initialize() {
        super.initialize();

        /** instantiated the state variable whenever this node is called*/
        found = false;
        done = false;
        leftTuple = null;
        rightTuple = null;
    }


    /**
     * Returns the next joined tuple that satisfies the join condition.
     *
     * @return the next joined tuple that satisfies the join condition.
     *
     * @throws IOException if a db file failed to open at some point
     */
    public Tuple getNextTuple() throws IOException {
        if (done)
            return null;
        /** splits the program based on the type of join with default case being antijoin*/
        switch(joinType) {
            case INNER:
                /** This statement and variants for other joins will only execute the first
                 *  time the method is called.  The statement moves to the first tuple from
                 *  the left child if such a tuple exists.*/
                if (leftTuple == null) {
                    leftTuple = leftChild.getNextTuple();
                    if (leftTuple == null) {
                        done = true;
                        return null;
                    }
                }
                /** gets tuples from helper methods and returns them*/
                while (getInner())
                    if (canJoinTuples())
                        return joinTuples(leftTuple, rightTuple);
                break;
            case LEFT_OUTER:
                if (leftTuple == null) {
                    leftTuple = leftChild.getNextTuple();
                    if (leftTuple == null) {
                        done = true;
                        return null;
                    }
                }
                while (getLeftOuter()) {
                    if (canJoinTuples()) {
                        /** if a tuple was found to join the current left tuple, set the state
                         * varaible to true.*/
                        found = true;
                        return joinTuples(leftTuple, rightTuple);
                    }
                    /** this statement executes if the current left tuple was not joined with any
                     * tuple from the right child. Then it is null padded and returned.
                     */
                    else if (rightTuple == nullPad) {
                        found = false;
                        return joinTuples(leftTuple, rightTuple);
                    }
                }
                break;
            case RIGHT_OUTER:
                /** This section works exactly as the LeftOuter section with tuples reversed*/
                if (rightTuple == null) {
                    rightTuple = rightChild.getNextTuple();
                    if (rightTuple == null) {
                        done = true;
                        return null;
                    }
                }
                while (getRightOuter()) {
                    if (canJoinTuples()) {
                        found = true;
                        return joinTuples(leftTuple, rightTuple);
                    } else if (leftTuple == nullPad) {
                        found = false;
                        return joinTuples(leftTuple, rightTuple);
                    }
                }
                break;
            case SEMIJOIN:
                /** this this method, when called, will immediately proceed to the next tuple on
                 * the left child (as the previous call to this method will have returned
                 * the current tuple and semi join does not return multisets.
                 */
                while (leftChild.getNextTuple() != null) {
                    leftTuple = leftChild.getNextTuple();
                    /** restarts the right child to check for matching tuples*/
                    rightChild.initialize();
                    while (rightChild.getNextTuple() != null) {
                        rightTuple = rightChild.getNextTuple();
                        /** if there is a possible join, return the left tuple*/
                        if (canJoinTuples())
                            return leftTuple;
                    }
                }
                break;
            default:
                /** this will execute if the join is an anitjoin*/
                while (leftChild.getNextTuple() != null) {
                    /** state variable represents if the current tuple can join from any tuple in right child.*/
                    found = false;
                    leftTuple = leftChild.getNextTuple();
                    rightChild.initialize();
                    while (rightChild.getNextTuple() != null) {
                        rightTuple = rightChild.getNextTuple();
                        if (canJoinTuples())
                            found = true;
                    }
                    if (!found)
                        return leftTuple;
                }
                done = true;
                break;
        }
        return null;
    }

    /** This method uses a nested-loop search with modification that if no tuple is found for a given right tuple
     * represented by the state variable 'found', the left tuple is passed back with the right tuple set
     * to a null-column tuple.  The method returns false when there are no more tuples to process.
     */
    private boolean getLeftOuter() throws IOException {
        while(true) {
            if(rightTuple == nullPad) {
                found = false;
                leftTuple = leftChild.getNextTuple();
                if (leftTuple == null) {
                    done = true;
                    return false;
                }
            }
            rightTuple = rightChild.getNextTuple();
            if (rightTuple == null) {
                if (!found) {
                    rightTuple = nullPad;
                    rightChild.initialize();
                    return true;
                }
                leftTuple = leftChild.getNextTuple();
                if (leftTuple == null) {
                    done = true;
                    return false;
                }
                found = false;
                rightChild.initialize();
            }
            else
                return true;
        }
    }

    /** works exactly as getLeftOuter with side reversed.  Uses a nested loop search with modification
     * that if no tuple is found for a right tuple, the right tuple is returned with a null-column
     * left tuple. Returns false when there are no more tuples.
     */
    private boolean getRightOuter() throws IOException {
        while(true) {
            if(leftTuple == nullPad) {
                found = false;
                rightTuple = rightChild.getNextTuple();
                if (rightTuple == null) {
                    done = true;
                    return false;
                }
            }
            leftTuple = leftChild.getNextTuple();
            if (leftTuple == null) {
                if (!found) {
                    leftTuple = nullPad;
                    leftChild.initialize();
                    return true;
                }
                rightTuple = rightChild.getNextTuple();
                if (rightTuple == null) {
                    done = true;
                    return false;
                }
                found = false;
                leftChild.initialize();
            }
            else
                return true;
        }
    }

    /** This method executes a natural nested-loop searching for each tuple in the right child
     * before advancing the next tuple of the left child and restarting the right child.
     * @return
     * @throws IOException
     */
    private boolean getInner() throws IOException {
        while(true) {
            rightTuple = rightChild.getNextTuple();
            if(rightTuple == null) {
                leftTuple = leftChild.getNextTuple();
                if(leftTuple == null)
                    return false;
                rightChild.initialize();
            }
            else
                return true;
        }
    }

    private boolean canJoinTuples() {
        // If the predicate was not set, we can always join them!
        if (predicate == null)
            return true;

        environment.clear();
        environment.addTuple(leftSchema, leftTuple);
        environment.addTuple(rightSchema, rightTuple);

        return predicate.evaluatePredicate(environment);
    }


    public void markCurrentPosition() {
        leftChild.markCurrentPosition();
        rightChild.markCurrentPosition();
    }


    public void resetToLastMark() throws IllegalStateException {
        leftChild.resetToLastMark();
        rightChild.resetToLastMark();
        // TODO:  Prepare to reevaluate the join operation for the tuples.
        //        (Just haven't gotten around to implementing this.)
    }


    public void cleanUp() {
        leftChild.cleanUp();
        rightChild.cleanUp();
    }
}
