package edu.caltech.nanodb.expressions;

import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.functions.Function;
import edu.caltech.nanodb.plans.GroupAggregateNode;
import edu.caltech.nanodb.plans.HashedGroupAggregateNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * An implementation of ExpressionProcessor. Takes an input query,
 * replaces every aggregate function in that query with a custom
 * column value, generating a unique name for each column.
 * Stores a map of all the replaced functions and the columns
 * they were replaced with.
 */
public class AggregateProcessor implements ExpressionProcessor {

    /**
     *    The number of aggregate functions processed, used to
     *    generate unique column names.
     */
    private int aggregateCount = 0;

    /**
     * Flag for checking if an aggregate has been processed from the input
     */
    private boolean hasAggregate;

    /**
     * The map of strings of new column names, and the
     * function that column was created to replace.
     */
    private Map<String, FunctionCall> renameMap;

    /**
     * Default constructor, initializes some private variables.
     */
    public AggregateProcessor() {
        aggregateCount = 0;
        renameMap = new HashMap();
    }



    @Override
    public Expression leave(Expression node) {
        // Checks if the expression is a function call,
        // and then if that call is to an aggregate function
        if (node instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) node;
            Function f = call.getFunction();
            if (f instanceof AggregateFunction) {
                hasAggregate = true;
                ColumnName newName = new ColumnName("#" + aggregateCount);
                renameMap.put("#" + aggregateCount, call);
                aggregateCount += 1;
                ColumnValue newNode = new ColumnValue(newName);

                return newNode;
            }
            else {
                return node;
            }
        }
        else {
            return node;
        }
    }

    @Override
    public void enter(Expression node) {

    }

    /**
     * Accessor method for the map of string/function pairs.
     * @return the map of pairs of column names and
     * the type of aggregate function each one replaced.
     */
    public Map<String, FunctionCall> getMap() {
        return renameMap;
    }

    /**
     * Accessor method for the hasAggregate flag.
     * @return The flag representing whether or not
     * the processor has encountered an aggregate function.
     */
    public boolean hasAggregate() {
        return hasAggregate;
    }

}
