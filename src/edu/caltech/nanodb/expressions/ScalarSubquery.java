package edu.caltech.nanodb.expressions;


import edu.caltech.nanodb.commands.SelectClause;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;


/**
 * This class represents a scalar subquery embedded in another query's
 * predicate.
 */
public class ScalarSubquery extends SubqueryOperator {

    public ScalarSubquery(SelectClause subquery) {
        if (subquery == null)
            throw new IllegalArgumentException("subquery cannot be null");

        this.subquery = subquery;
    }


    public ColumnInfo getColumnInfo(Schema schema) throws SchemaNameException {
        // TODO:  IMPLEMENT
        return null;
    }


    public Object evaluate(Environment env) {
        // TODO:  IMPLEMENT

        // TODO:  Don't forget to try to get *two* tuples from the subquery;
        //        it should only return ONE tuple!
        return null;
    }


    @Override
    public Expression traverse(ExpressionProcessor p) {
        p.enter(this);
        return p.leave(this);
    }


    @Override
    public String toString() {
        return "(" + subquery.toString() + ")";
    }


    /**
     * Checks if the argument is an expression with the same structure, but not
     * necessarily the same references.
     *
     * @design This function operates correctly on the assumption that all
     *         supported types override Object.equals().
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ScalarSubquery) {
            ScalarSubquery other = (ScalarSubquery) obj;
            return subquery.equals(other.subquery);
        }
        return false;
    }


    /**
     * Computes the hash-code of this scalar subquery expression.
     */
    @Override
    public int hashCode() {
        return subquery.hashCode();
    }


    /**
     * Creates a copy of expression.
     *
     * @design The reference of the internal value is simply copied since the
     *         value types are all immutable.
     */
    @Override
    protected Object clone() throws CloneNotSupportedException {
        ScalarSubquery expr = (ScalarSubquery) super.clone();

        // We don't clone SelectClause expressions at this point since they are
        // currently not cloneable.

        return expr;
    }
}

