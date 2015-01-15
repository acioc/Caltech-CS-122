package edu.caltech.nanodb.expressions;

import java.util.Collection;


/**
 * Created by donnie on 10/31/14.
 */
public class PredicateUtils {



    /**
     * This helper function takes a collection of conjuncts that should comprise
     * a predicate, and creates a predicate for evaluating these conjuncts.  The
     * exact nature of the predicate depends on the conjuncts:
     * <ul>
     *   <li>If the collection contains only one conjunct, the method simply
     *       returns that one conjunct.</li>
     *   <li>If the collection contains two or more conjuncts, the method
     *       returns a {@link BooleanOperator} that performs an <tt>AND</tt> of
     *       all conjuncts.</li>
     *   <li>If the collection contains <em>no</em> conjuncts then the method
     *       returns <tt>null</tt>.
     * </ul>
     *
     * @param conjuncts the collection of conjuncts to combine into a predicate.
     *
     * @return a predicate for evaluating the conjuncts, or <tt>null</tt> if the
     *         input collection contained no conjuncts.
     */
    public static Expression makePredicate(Collection<Expression> conjuncts) {
        Expression predicate = null;
        if (conjuncts.size() == 1) {
            predicate = conjuncts.iterator().next();
        }
        else if (conjuncts.size() > 1) {
            predicate = new BooleanOperator(
                BooleanOperator.Type.AND_EXPR, conjuncts);
        }
        return predicate;
    }
}
