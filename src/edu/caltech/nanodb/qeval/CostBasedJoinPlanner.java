package edu.caltech.nanodb.qeval;


import java.io.IOException;
import java.util.*;

import edu.caltech.nanodb.commands.SelectValue;
import edu.caltech.nanodb.expressions.*;
import edu.caltech.nanodb.plans.*;
import edu.caltech.nanodb.relations.JoinType;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.commands.FromClause;
import edu.caltech.nanodb.commands.SelectClause;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This planner implementation uses dynamic programming to devise an optimal
 * join strategy for the query.  As always, queries are optimized in units of
 * <tt>SELECT</tt>-<tt>FROM</tt>-<tt>WHERE</tt> subqueries; optimizations
 * don't currently span multiple subqueries.
 */
public class CostBasedJoinPlanner implements Planner {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(CostBasedJoinPlanner.class);


    private StorageManager storageManager;


    public void setStorageManager(StorageManager storageManager) {
        this.storageManager = storageManager;
    }


    /**
     * This helper class is used to keep track of one "join component" in the
     * dynamic programming algorithm.  A join component is simply a query plan
     * for joining one or more leaves of the query.
     * <p>
     * In this context, a "leaf" may either be a base table or a subquery in
     * the <tt>FROM</tt>-clause of the query.  However, the planner will
     * attempt to push conjuncts down the plan as far as possible, so even if
     * a leaf is a base table, the plan may be a bit more complex than just a
     * single file-scan.
     */
    private static class JoinComponent {
        /**
         * This is the join plan itself, that joins together all leaves
         * specified in the {@link #leavesUsed} field.
         */
        public PlanNode joinPlan;

        /**
         * This field specifies the collection of leaf-plans that are joined by
         * the plan in this join-component.
         */
        public HashSet<PlanNode> leavesUsed;

        /**
         * This field specifies the collection of all conjuncts use by this join
         * plan.  It allows us to easily determine what join conjuncts still
         * remain to be incorporated into the query.
         */
        public HashSet<Expression> conjunctsUsed;

        /**
         * Constructs a new instance for a <em>leaf node</em>.  It should not
         * be used for join-plans that join together two or more leaves.  This
         * constructor simply adds the leaf-plan into the {@link #leavesUsed}
         * collection.
         *
         * @param leafPlan the query plan for this leaf of the query.
         *
         * @param conjunctsUsed the set of conjuncts used by the leaf plan.
         *        This may be an empty set if no conjuncts apply solely to
         *        this leaf, or it may be nonempty if some conjuncts apply
         *        solely to this leaf.
         */
        public JoinComponent(PlanNode leafPlan, HashSet<Expression> conjunctsUsed) {
            leavesUsed = new HashSet<PlanNode>();
            leavesUsed.add(leafPlan);

            joinPlan = leafPlan;

            this.conjunctsUsed = conjunctsUsed;
        }

        /**
         * Constructs a new instance for a <em>non-leaf node</em>.  It should
         * not be used for leaf plans!
         *
         * @param joinPlan the query plan that joins together all leaves
         *        specified in the <tt>leavesUsed</tt> argument.
         *
         * @param leavesUsed the set of two or more leaf plans that are joined
         *        together by the join plan.
         *
         * @param conjunctsUsed the set of conjuncts used by the join plan.
         *        Obviously, it is expected that all conjuncts specified here
         *        can actually be evaluated against the join plan.
         */
        public JoinComponent(PlanNode joinPlan, HashSet<PlanNode> leavesUsed,
                             HashSet<Expression> conjunctsUsed) {
            this.joinPlan = joinPlan;
            this.leavesUsed = leavesUsed;
            this.conjunctsUsed = conjunctsUsed;
        }
    }


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     *
     * @throws java.io.IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    public PlanNode makePlan(SelectClause selClause,
        List<SelectClause> enclosingSelects) throws IOException {

        // We want to take a simple SELECT a, b, ... FROM A, B, ... WHERE ...
        // and turn it into a tree of plan nodes.
        PlanNode finalPlan;
        FromClause fromClause = selClause.getFromClause();
        if (fromClause == null) {
            finalPlan = new ProjectNode(selClause.getSelectValues());
            finalPlan.prepare();
            return finalPlan;
        }

        // Where expression of our query
        Expression whereExpr = selClause.getWhereExpr();

        // Processes expressions for aggregate functions
        AggregateProcessor processor = new AggregateProcessor();

        // Check if where clause contains aggregate functions (it shouldn't!)
        if(whereExpr != null) {
            whereExpr.traverse(processor);
            if(processor.hasAggregate()) {
                throw new IllegalArgumentException(
                        "Where clause cannot contain aggregate functions."
                );
            }
        }

        // Check if on clauses contains aggregate functions (they shouldn't)
        if(fromClause != null) {
            checkFromClause(fromClause, processor);
        }

        // Parses query, replaces aggregate functions with custom columns
        for( SelectValue sv : selClause.getSelectValues()) {
            if(!sv.isExpression()) continue;

            Expression e = sv.getExpression().traverse(processor);
            sv.setExpression(e);
        }

        // the top-level conjuncts in the where & having clauses
        HashSet<Expression> extraConjuncts = new HashSet<Expression>();
        PredicateUtils.collectConjuncts(
                selClause.getWhereExpr(), 
                extraConjuncts);
        // THE LINE BELOW IS NOT NEEDED
        // IT IS COMMENTED OUT SO WE CAN MAINTAIN IT FOR FUTURE CHANGES
        /*PredicateUtils.collectConjuncts(
                selClause.getHavingExpr(), 
                extraConjuncts);*/

        // obtain the optimal join plan
        JoinComponent optimalComponent = makeJoinPlan(
                fromClause, 
                extraConjuncts);
        finalPlan = optimalComponent.joinPlan;

        // if there are any unused conjuncts...
        extraConjuncts.removeAll(optimalComponent.conjunctsUsed);
        if (!extraConjuncts.isEmpty()) {
            Expression predicate = PredicateUtils.makePredicate(
                    extraConjuncts);
            finalPlan = new SimpleFilterNode(finalPlan, predicate);
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
     * Recursively checks a from clause and all its children for aggregate 
     * functions in the on expression
     * @param fromClause the from clause being checked for aggregate functions
     * @param processor the processor processing the on clause expressions
     *
     * @throws IllegalArgumentException if the clause contains an aggregate 
     * function
     */
    public void checkFromClause(FromClause fromClause, 
            AggregateProcessor processor) {
        switch (fromClause.getClauseType()) {
            
            case JOIN_EXPR:
                if(fromClause.getOnExpression() != null) {
                    fromClause.getOnExpression().traverse(processor);
                    if(processor.hasAggregate()){
                        throw new IllegalArgumentException(
                                "On clause cannot contain aggregate function."
                        );
                    }
                    else {
                        checkFromClause(fromClause.getLeftChild(), processor);
                        checkFromClause(fromClause.getRightChild(), processor);
                    }
                }
                break;
                
            default:
                break;
        }
    }

    /**
     * Given the top-level {@code FromClause} for a SELECT-FROM-WHERE block,
     * this helper generates an optimal join plan for the {@code FromClause}.
     *
     * @param fromClause the top-level {@code FromClause} of a
     *        SELECT-FROM-WHERE block.
     * @param extraConjuncts any extra conjuncts (e.g. from the WHERE clause,
     *        or HAVING clause)
     * @return a {@code JoinComponent} object that represents the optimal plan
     *         corresponding to the FROM-clause
     * @throws IOException if an IO error occurs during planning.
     */
    private JoinComponent makeJoinPlan(FromClause fromClause,
        Collection<Expression> extraConjuncts) throws IOException {

        // These variables receive the leaf-clauses and join conjuncts found
        // from scanning the sub-clauses.  Initially, we put the extra
        // conjuncts into the collection of conjuncts.
        HashSet<Expression> conjuncts = new HashSet<Expression>();
        ArrayList<FromClause> leafFromClauses = new ArrayList<FromClause>();

        collectDetails(fromClause, conjuncts, leafFromClauses);

        logger.debug("Making join-plan for " + fromClause);
        logger.debug("    Collected conjuncts:  " + conjuncts);
        logger.debug("    Collected FROM-clauses:  " + leafFromClauses);
        logger.debug("    Extra conjuncts:  " + extraConjuncts);

        if (extraConjuncts != null)
            conjuncts.addAll(extraConjuncts);

        // Make a read-only set of the input conjuncts, to avoid bugs due to
        // unintended side-effects.
        Set<Expression> roConjuncts = Collections.unmodifiableSet(conjuncts);

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.

        logger.debug("Generating plans for all leaves");
        ArrayList<JoinComponent> leafComponents = generateLeafJoinComponents(
            leafFromClauses, roConjuncts);

        // Print out the results, for debugging purposes.
        if (logger.isDebugEnabled()) {
            for (JoinComponent leaf : leafComponents) {
                logger.debug("    Leaf plan:  " +
                    PlanNode.printNodeTreeToString(leaf.joinPlan, true));
            }
        }

        // Build up the full query-plan using a dynamic programming approach.

        JoinComponent optimalJoin =
            generateOptimalJoin(leafComponents, roConjuncts);

        PlanNode plan = optimalJoin.joinPlan;
        logger.info("Optimal join plan generated:\n" +
            PlanNode.printNodeTreeToString(plan, true));

        return optimalJoin;
    }


    /**
     * This helper method pulls the essential details for join optimization
     * out of a <tt>FROM</tt> clause.
     *
     * It determines the clause type. If the clause is a leaf it adds that 
     * clause to the Array List of leaf clauses. Otherwise it adds 
     * conjuncts (if necessary) to the hash set of conjunctions and recursively
     * calls the function on the clause's children.
     *
     * @param fromClause the from-clause to collect details from
     *
     * @param conjuncts the collection to add all conjuncts to
     *
     * @param leafFromClauses the collection to add all leaf from-clauses to
     */
    private void collectDetails(FromClause fromClause,
        HashSet<Expression> conjuncts, ArrayList<FromClause> leafFromClauses) {
        switch(fromClause.getClauseType()) {

            // handling base tables (leaf)
            case BASE_TABLE:
            case SELECT_SUBQUERY:
                leafFromClauses.add(fromClause);

                break;

            // handling join expressions (not necessarily a leaf)
            case JOIN_EXPR:
                if(fromClause.isOuterJoin()) {
                    leafFromClauses.add(fromClause);
                }
                else {
                    Expression onExpr = fromClause.getPreparedJoinExpr();
                    if(onExpr != null) {
                        PredicateUtils.collectConjuncts(onExpr, conjuncts);
                    }
                    collectDetails(
                            fromClause.getLeftChild(), 
                            conjuncts, 
                            leafFromClauses);
                    collectDetails(
                            fromClause.getRightChild(), 
                            conjuncts, 
                            leafFromClauses);
                }

                break;

            default:
                throw new UnsupportedOperationException(
                        "Given FROM clause not supported");
        }
    }


    /**
     * This helper method performs the first step of the dynamic programming
     * process to generate an optimal join plan, by generating a plan for every
     * leaf from-clause identified from analyzing the query.  Leaf plans are
     * usually very simple; they are built either from base-tables or
     * <tt>SELECT</tt> subqueries.  The most complex detail is that any
     * conjuncts in the query that can be evaluated solely against a particular
     * leaf plan-node will be associated with the plan node.  <em>This is a
     * heuristic</em> that usually produces good plans (and certainly will for
     * the current state of the database), but could easily interfere with
     * indexes or other plan optimizations.
     *
     * @param leafFromClauses the collection of from-clauses found in the query
     *
     * @param conjuncts the collection of conjuncts that can be applied at this
     *                  level
     *
     * @return a collection of {@link JoinComponent} object containing the plans
     *         and other details for each leaf from-clause
     *
     * @throws IOException if a particular database table couldn't be opened or
     *         schema loaded, for some reason
     */
    private ArrayList<JoinComponent> generateLeafJoinComponents(
        Collection<FromClause> leafFromClauses, Collection<Expression> conjuncts)
        throws IOException {

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.
        ArrayList<JoinComponent> leafComponents = 
                new ArrayList<JoinComponent>();
        for (FromClause leafClause : leafFromClauses) {
            HashSet<Expression> leafConjuncts = new HashSet<Expression>();

            PlanNode leafPlan =
                makeLeafPlan(leafClause, conjuncts, leafConjuncts);

            JoinComponent leaf = new JoinComponent(leafPlan, leafConjuncts);
            leafComponents.add(leaf);
        }

        return leafComponents;
    }


    /**
     * Constructs a plan tree for evaluating the specified from-clause.
     *
     * Handles different types of leaves in the from clause. We handle base 
     * tables, select sub-queries, and join expressions.  Join expressions 
     * need to handle different conjuncts depending on what type of
     * outer join is being performed.
     *
     * @param fromClause the select nodes that need to be joined.
     *
     * @param conjuncts additional conjuncts that can be applied when
     *        constructing the from-clause plan.
     *
     * @param leafConjuncts this is an output-parameter.  Any conjuncts applied
     *        in this plan from the <tt>conjuncts</tt> collection should be added
     *        to this out-param.
     *
     * @return a plan tree for evaluating the specified from-clause
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     *
     * @throws IllegalArgumentException if the specified from-clause is a join
     *         expression that isn't an outer join, or has some other
     *         unrecognized type.
     */
    private PlanNode makeLeafPlan(FromClause fromClause,
        Collection<Expression> conjuncts, HashSet<Expression> leafConjuncts)
        throws IOException {

        PlanNode finalPlan;
        switch(fromClause.getClauseType()) {
            // handle the base table
            case BASE_TABLE:
                // Get a simple select so we can prepare it (for a schema)
                SelectNode tempNode = makeSimpleSelect(
                        fromClause.getTableName(),
                        null,
                        null);
                tempNode.prepare();
                // Use findExprsUsingSchemas to get the appropriate conjuncts 
                // for our simple select
                PredicateUtils.findExprsUsingSchemas(
                    conjuncts,
                    false,
                    leafConjuncts,
                    tempNode.getSchema());
                // Actually get our simple select using the correct predicate
                Expression predicate = PredicateUtils.makePredicate(
                        leafConjuncts);
                finalPlan = makeSimpleSelect(
                        fromClause.getTableName(), 
                        predicate, 
                        null);
                break;

            // handle derived tables
            // the subquery has it's own unique set of conjuncts so we don't
            // need to handle them here
            case SELECT_SUBQUERY:
                finalPlan = makePlan(fromClause.getSelectClause(), null);
                PredicateUtils.findExprsUsingSchemas(
                        conjuncts,
                        false,
                        leafConjuncts,
                        finalPlan.getSchema());
                // Actually get our simple select using the correct predicate
                Expression selectPredicates = PredicateUtils.makePredicate(
                        leafConjuncts);
                if (selectPredicates != null) {
                	finalPlan = PlanUtils.addPredicateToPlan(
            			finalPlan, 
            			selectPredicates);
                }
                // Rename if necessary 
                if (fromClause.isRenamed()) {
                	finalPlan = new RenameNode(
                			finalPlan, 
                			fromClause.getResultName());	
                }
                
                break;

            // handle outer joins
            case JOIN_EXPR:
                if (!fromClause.isOuterJoin()) {
                    throw new UnsupportedOperationException(
                            "All JOIN leaf clases must be outer joins");
                }

                FromClause lChild = fromClause.getLeftChild();
                FromClause rChild = fromClause.getRightChild();
                JoinComponent joinComponentLeft;
                JoinComponent joinComponentRight;
                HashSet<Expression> leftLeafConjuncts = null;
                HashSet<Expression> rightLeafConjuncts = null;
                // We handle right outer joins
                if (!fromClause.hasOuterJoinOnLeft()) {
                    // We prepare the right child's schema
                    PredicateUtils.findExprsUsingSchemas(
                            conjuncts,
                            false,
                            leafConjuncts,
                            fromClause.getRightChild().getPreparedSchema());
                    rightLeafConjuncts = leafConjuncts;
                }
                // We handle left outer joins
                else if(!fromClause.hasOuterJoinOnRight()) {
                    // We prepare the left child's schema
                    PredicateUtils.findExprsUsingSchemas(
                            conjuncts,
                            false,
                            leafConjuncts,
                            fromClause.getLeftChild().getPreparedSchema());
                    leftLeafConjuncts = leafConjuncts;
                }
                // Handle the two children
                joinComponentLeft = makeJoinPlan(lChild, leftLeafConjuncts);
                joinComponentRight = makeJoinPlan(rChild, rightLeafConjuncts);
                
                // Join the left and right components
                finalPlan = new NestedLoopsJoinNode(
                        joinComponentLeft.joinPlan,
                        joinComponentRight.joinPlan,
                        fromClause.getJoinType(),
                        fromClause.getPreparedJoinExpr());
                break;

            default:
                throw new UnsupportedOperationException(
                        "Incorrect leaf type passed (not a leaf)");

        }

        // We use a project node to avoid duplicate column names
        ArrayList<SelectValue> prepSelVal =
                fromClause.getPreparedSelectValues();
        if (prepSelVal != null) {
            finalPlan = new ProjectNode(finalPlan, prepSelVal);
        }
        
        finalPlan.prepare();
        return finalPlan;
    }


    /**
     * This helper method builds up a full join-plan using a dynamic 
     * programming approach. 
     * The implementation maintains a collection of optimal
     * intermediate plans that join <em>n</em> of the leaf nodes, each with its
     * own associated cost, and then uses that collection to generate a new
     * collection of optimal intermediate plans that join <em>n+1</em> of the
     * leaf nodes.  This process completes when all leaf plans are joined
     * together; there will be <em>one</em> plan, and it will be the optimal
     * join plan (as far as our limited estimates can determine, anyway).
     *
     * @param leafComponents the collection of leaf join-components, generated
     *        by the {@link #generateLeafJoinComponents} method.
     *
     * @param conjuncts the collection of all conjuncts found in the query
     *
     * @return a single {@link JoinComponent} object that joins all leaf
     *         components together in an optimal way.
     */
    private JoinComponent generateOptimalJoin(
        ArrayList<JoinComponent> leafComponents, Set<Expression> conjuncts) {

        // This object maps a collection of leaf-plans (represented as a
        // hash-set) to the optimal join-plan for that collection of leaf plans.
        //
        // This collection starts out only containing the leaf plans themselves,
        // and on each iteration of the loop below, join-plans are grown by one
        // leaf.  For example:
        //   * In the first iteration, all plans joining 2 leaves are created.
        //   * In the second iteration, all plans joining 3 leaves are created.
        //   * etc.
        // At the end, the collection will contain ONE entry, which is the
        // optimal way to join all N leaves.  Go Go Gadget Dynamic Programming!
        HashMap<HashSet<PlanNode>, JoinComponent> joinPlans =
            new HashMap<HashSet<PlanNode>, JoinComponent>();

        // Initially populate joinPlans with just the N leaf plans.
        for (JoinComponent leaf : leafComponents)
            joinPlans.put(leaf.leavesUsed, leaf);

        while (joinPlans.size() > 1) {
            logger.debug("Current set of join-plans has " + joinPlans.size() +
                " plans in it.");

            // This is the set of "next plans" we will generate.  Plans only
            // get stored if they are the first plan that joins together the
            // specified leaves, or if they are better than the current plan.
            HashMap<HashSet<PlanNode>, JoinComponent> nextJoinPlans =
                new HashMap<HashSet<PlanNode>, JoinComponent>();

            //        JOIN N + 1 LEAVES
            // For plan_n in JoinPlans_n
            for(Map.Entry<HashSet<PlanNode>, 
                    JoinComponent> entry : joinPlans.entrySet()) {
                // For leaf in LeafPlans
                for(JoinComponent leaf : leafComponents) {
                    // If leaf already appears in plan_n...
                    if(entry.getKey().contains(leaf)) {
                        continue;
                    }
                    else {
                        // Obtain the correct conjuncts using our left 
                        // and right plans
                        PlanNode leftNode = entry.getValue().joinPlan;
                        PlanNode rightNode = leaf.joinPlan;

                        Collection<Expression> tempConjunctsLeft = 
                                new HashSet<Expression>();
                        Collection<Expression> tempConjunctsRight = 
                                new HashSet<Expression>();

                        PredicateUtils.findExprsUsingSchemas(
                                conjuncts,
                                false,
                                tempConjunctsLeft,
                                leftNode.getSchema());

                        PredicateUtils.findExprsUsingSchemas(
                                conjuncts,
                                false,
                                tempConjunctsRight,
                                rightNode.getSchema());
                        // SubplanConjuncts = 
                        //        LeftChildConjuncts U RightChildConjuncts
                        tempConjunctsLeft.addAll(tempConjunctsRight);
                        Collection<Expression> unusedConjucts = 
                                new HashSet<Expression>();
                        unusedConjucts.addAll(conjuncts);

                        // Unused Conjuncts = AllConjuncts - SubplanConjuncts
                        unusedConjucts.removeAll(tempConjunctsLeft);
                        HashSet<Expression> usedConjucts = 
                                new HashSet<Expression>();
                        // Find which conjuncts from unused conjuncts are 
                        // applied to our theta join
                        PredicateUtils.findExprsUsingSchemas(
                                unusedConjucts,
                                false,
                                usedConjucts,
                                leftNode.getSchema(),
                                rightNode.getSchema()
                        );
                        // Get our correct predicates
                        Expression newExpressions = 
                                makePredicate(usedConjucts);

                        // Get the new plan by performing a theta join
                        PlanNode newPlanNode = new NestedLoopsJoinNode(
                                leftNode,
                                rightNode,
                                JoinType.INNER,
                                newExpressions);
                        newPlanNode.prepare();

                        // Add the leaf to plan_n
                        HashSet<PlanNode> newLeaves = new HashSet<PlanNode>();
                        newLeaves.addAll(entry.getKey());
                        newLeaves.add(rightNode);

                        // newCost = cost of the new plan
                        PlanCost newCost = newPlanNode.getCost();

                        // if JoinPlans_n+1 already contains a plan 
                        // with all leaves in plan_n+1...
                        if (nextJoinPlans.keySet().contains(newLeaves)) {
                            PlanCost oldCost = 
                                nextJoinPlans.get(newLeaves).joinPlan.getCost();
                            if (newCost.cpuCost < oldCost.cpuCost) {
                               // replace old plan with new plan
                                nextJoinPlans.remove(newLeaves);
                                JoinComponent newComponent = new JoinComponent(
                                        newPlanNode, 
                                        newLeaves, 
                                        usedConjucts);
                                nextJoinPlans.put(newLeaves, newComponent);
                            }
                        }
                        // else, add the new plan to the map
                        else {
                            JoinComponent newComponent = new JoinComponent(
                                    newPlanNode, 
                                    newLeaves, 
                                    usedConjucts);
                            nextJoinPlans.put(newLeaves, newComponent);
                        }

                    }
                }
            }


            // Now that we have generated all plans joining N leaves, time to
            // create all plans joining N + 1 leaves.
            joinPlans = nextJoinPlans;
        }

        // At this point, the set of join plans should only contain one plan,
        // and it should be the optimal plan.

        assert joinPlans.size() == 1 : "There can be only one optimal join plan!";
        return joinPlans.values().iterator().next();
    }


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
    private Expression makePredicate(Collection<Expression> conjuncts) {
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
