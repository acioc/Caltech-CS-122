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
 * Created by dinglis on 2/2/15.
 */
public class AggregateProcessor implements ExpressionProcessor {

    // The number of aggregate functions processed
    private int aggregateCount = 0;

    // Flag for whether or not an aggregate has been processed
    private boolean hasAggregate;


    private List<Expression> aggExpressions;

    private Map<String, FunctionCall> renameMap;

    public AggregateProcessor() {

        aggExpressions = new ArrayList<Expression>();
        aggregateCount = 0;
        renameMap = new HashMap();
    }



    @Override
    public Expression leave(Expression node) {
        if (node instanceof FunctionCall) {
            FunctionCall call = (FunctionCall) node;
            Function f = call.getFunction();
            if (f instanceof AggregateFunction) {
                aggExpressions.add(node);
                hasAggregate = true;
                ColumnName newName = new ColumnName("#" + aggregateCount);
                renameMap.put("#" + aggregateCount, call);
                aggregateCount += 1;
                ColumnValue newNode = new ColumnValue(newName);

                System.out.println("returning column value:");
                System.out.println(newNode.toString());

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

    public Map<String, FunctionCall> getMap() {
        return renameMap;
    }

    public boolean hasAggregate() {
        return hasAggregate;
    }

    public List<Expression> getList() {
        return aggExpressions;
    }
}
