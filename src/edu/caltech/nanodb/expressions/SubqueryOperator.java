package edu.caltech.nanodb.expressions;

import edu.caltech.nanodb.commands.SelectClause;
import edu.caltech.nanodb.plans.PlanNode;

/**
 * This class is the superclass of all expressions that can hold subqueries,
 * such as the <tt>IN</tt> operator, the <tt>EXISTS</tt> operator, and scalar
 * subqueries.
 */
public abstract class SubqueryOperator extends Expression {

    /**
     * This is the actual subquery that the operator uses for its operation.
     */
    protected SelectClause subquery;


    /**
     * The execution plan for the subquery that is used by the operator.  This
     * will be {@code null} until the subquery has actually been analyzed and
     * planned by the query planner.
     */
    protected PlanNode subqueryPlan;


    public SelectClause getSubquery() {
        return subquery;
    }


    public void setSubqueryPlan(PlanNode plan) {
        subqueryPlan = plan;
    }
}
