package edu.caltech.nanodb.qeval;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.caltech.nanodb.commands.SelectValue;
import edu.caltech.nanodb.expressions.AggregateProcessor;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.commands.FromClause;
import edu.caltech.nanodb.commands.FromClause.ClauseType;
import edu.caltech.nanodb.commands.SelectClause;
import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.plans.FileScanNode;
import edu.caltech.nanodb.plans.NestedLoopsJoinNode;
import edu.caltech.nanodb.plans.PlanNode;
import edu.caltech.nanodb.plans.ProjectNode;
import edu.caltech.nanodb.plans.RenameNode;
import edu.caltech.nanodb.plans.SelectNode;
import edu.caltech.nanodb.plans.SimpleFilterNode;
import edu.caltech.nanodb.plans.SortNode;
import edu.caltech.nanodb.plans.HashedGroupAggregateNode;
import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This class generates execution plans for performing SQL queries.  The
 * primary responsibility is to generate plans for SQL <tt>SELECT</tt>
 * statements, but <tt>UPDATE</tt> and <tt>DELETE</tt> expressions will also
 * use this class to generate simple plans to identify the tuples to update
 * or delete.
 */
public class SimplePlanner implements Planner {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(SimplePlanner.class);


    private StorageManager storageManager;


    public void setStorageManager(StorageManager storageManager) {
        this.storageManager = storageManager;
    }


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    @Override
    public PlanNode makePlan(SelectClause selClause,
        List<SelectClause> enclosingSelects) throws IOException {

        if (enclosingSelects != null && !enclosingSelects.isEmpty()) {
            throw new UnsupportedOperationException(
                "Not yet implemented:  enclosing queries!");
        }

        // We start by working with our FROM clause
        FromClause fromClause = selClause.getFromClause();
        
        // We establish a plan node
        PlanNode finalPlan;

        // We check if we have aggregates
        AggregateProcessor processor = new AggregateProcessor();

        for( SelectValue sv : selClause.getSelectValues()) {
            if(!sv.isExpression()) continue;

            Expression e = sv.getExpression().traverse(processor);
            sv.setExpression(e);
        }

        // If our FROM clause is not NULL, we handle the different cases
        // by using our helper function
        if (fromClause != null) {
        	finalPlan = fromClauseHelper(fromClause);
    		// We rename if necessary
        	if (fromClause.getClauseType() != ClauseType.JOIN_EXPR){
	    		if (fromClause.isRenamed()) {
	    			finalPlan = new RenameNode(
	    					finalPlan, 
	    					fromClause.getResultName());
	    		}
        	}
        }
        // Otherwise, we have no from clause (I.E. "SELECT 3 + 2 AS five;")
        else {
        	// We just project and return our plan
        	finalPlan = new ProjectNode(selClause.getSelectValues());
        	finalPlan.prepare();
        	return finalPlan;
        }

        Expression whereExpr = selClause.getWhereExpr();
        // We handle a simple select
        if (whereExpr != null) {
            finalPlan = new SimpleFilterNode(finalPlan, whereExpr);
        }

        // We handle our grouping and aggregation
        List<Expression> groupExp = selClause.getGroupByExprs();
        if(processor.hasAggregate() || !groupExp.isEmpty()) {
            finalPlan = new HashedGroupAggregateNode(
            		finalPlan,
            		groupExp, 
            		processor.getMap());
            // We deal with our having expressions
            Expression haveExpr =  selClause.getHavingExpr();
            if (haveExpr != null) {
                haveExpr.traverse(processor);
                finalPlan = new SimpleFilterNode(finalPlan, haveExpr);
            }
        }
        
        // We handle non-trivial projects
        if (!selClause.isTrivialProject()) {
        	// We use a project from our current finalPlan
            finalPlan = new ProjectNode(
            		finalPlan, 
            		selClause.getSelectValues());     
        }
        
        // We deal with our order by expressions
        List<OrderByExpression> orderByExprs = selClause.getOrderByExprs();
        if (!orderByExprs.isEmpty()) {
        	finalPlan = new SortNode(finalPlan, orderByExprs);
        }

        // We return our plan
        finalPlan.prepare();
        return finalPlan;
    }
    
    /**
     * Acts as a helper function for dealing with the FROM clause of a query.
     * 
     * @param fromClause The FROM clause we are working with
     * 
     * @throws IOException if the switch does not handle the from clause type.
     */
    public PlanNode fromClauseHelper(FromClause fromClause) 
    		throws IOException {
    	
    	PlanNode finalPlan;
    	switch (fromClause.getClauseType()) {
	    	// If we have a BASE_TABLE...
	    	case BASE_TABLE:
	    		// We have a simple select FROM clause
	    		finalPlan = makeSimpleSelect(
	    				fromClause.getTableName(), null, null);
	    		break;
	    		
	    	// If we have a SELECT_SUBQUERY...
	    	case SELECT_SUBQUERY:
	    		// We call makePlan our select subquery 
	    		finalPlan = makePlan(fromClause.getSelectClause(), null);
	    		break;
	    		
	    	// If we have a JOIN_EXPR...	
	    	case JOIN_EXPR:
	    		// We get our child expressions
	    		FromClause leftChild = fromClause.getLeftChild();
	    		FromClause rightChild = fromClause.getRightChild();
	    		// We create a new nested loop join node
	    		finalPlan = new NestedLoopsJoinNode(
	    				fromClauseHelper(leftChild),
	    				fromClauseHelper(rightChild),
	    				fromClause.getJoinType(), 
	    				fromClause.getPreparedJoinExpr());
	    		// We use a project node to avoid duplicate column names
	    		ArrayList<SelectValue> prepSelVal = 
	    				fromClause.getPreparedSelectValues();
	    		if (!prepSelVal.isEmpty()) {
	    			finalPlan = new ProjectNode(finalPlan, prepSelVal);
	    		}
	    		break;
	    		
	    	// We can currently throw an exception for this last case
	    	default:
	    		throw new UnsupportedOperationException(
	    				"Given FROM clause not supported");
    	}
		return finalPlan;
    }

    /**
     * Constructs a simple select plan that reads directly from a table, with
     * an optional predicate for selecting rows.
     * <p>
     * While this method can be used for building up larger <tt>SELECT</tt>
     * queries, the returned plan is also suitable for use in <tt>UPDATE</tt>
     * and <tt>DELETE</tt> command evaluation.  In these cases, the plan must
     * only generate tuples of type {@link edu.caltech.nanodb.storage.PageTuple},
     * so that the command can modify or delete the actual tuple in the file's
     * page data.
     *
     * @param tableName The name of the table that is being selected from.
     *
     * @param predicate An optional selection predicate, or {@code null} if
     *        no filtering is desired.
     *
     * @return A new plan-node for evaluating the select operation.
     *
     * @throws IOException if an error occurs when loading necessary table
     *         information.
     */
    public SelectNode makeSimpleSelect(String tableName, Expression predicate,
        List<SelectClause> enclosingSelects) throws IOException {
        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        if (enclosingSelects != null) {
            // If there are enclosing selects, this subquery's predicate may
            // reference an outer query's value, but we don't detect that here.
            // Therefore we will probably fail with an unrecognized column
            // reference.
            logger.warn("Currently we are not clever enough to detect " +
                "correlated subqueries, so expect things are about to break...");
        }

        // Open the table.
        TableInfo tableInfo = storageManager.getTableManager().openTable(tableName);

        // Make a SelectNode to read rows from the table, with the specified
        // predicate.
        SelectNode selectNode = new FileScanNode(tableInfo, predicate);
        selectNode.prepare();
        return selectNode;
    }
}
