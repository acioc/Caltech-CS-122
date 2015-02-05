package edu.caltech.test.nanodb.sql;

import org.testng.annotations.Test;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;

/**
 * This class tests nanodb's ability to handle simple join cases.
 **/
@Test
public class TestSimpleJoins extends SqlTestCase {
    public TestSimpleJoins() {
        super ("setup_testSimpleJoins");
    }

    /**
     * This test checks that at least one value was successfully inserted into
     * the test table.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testTableNotEmpty() throws Throwable {
        testTableNotEmpty("simple_1");
    }

    /**
     * This tests nanodb's ability to handle inner, right outer, and
     * left outer joins under simple use cases.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testSimpleJoins() throws Throwable {
        CommandResult result;

        result = server.doCommand(
                "SELECT * FROM empty_table_1 INNER JOIN empty_table_2", true);
        System.err.println("HAVING RESULTS = " + result.getTuples());
        TupleLiteral[] expected1 = {
        };
        assert checkSizeResults(expected1, result);
        assert checkUnorderedResults(expected1, result);

        result = server.doCommand(
                "SELECT * FROM empty_table_1 INNER JOIN simple_1", true);
        TupleLiteral[] expected2 = {
        };
        assert checkSizeResults(expected2, result);
        assert checkUnorderedResults(expected2, result);

        result = server.doCommand(
                "SELECT * FROM simple_1 INNER JOIN empty_table_1", true);
        TupleLiteral[] expected3 = {
        };
        assert checkSizeResults(expected3, result);
        assert checkUnorderedResults(expected3, result);

        result = server.doCommand(
                "SELECT * FROM simple_1 INNER JOIN simple_2", true);
        TupleLiteral[] expected4 = {
                new TupleLiteral( 1 , 2 , 1 , 3 ),
                new TupleLiteral( 1 , 2 , 1 , 4 ),
                new TupleLiteral( 1 , 2 , 1 , 5 ),
                new TupleLiteral( 1 , 6 , 1 , 3 ),
                new TupleLiteral( 1 , 6 , 1 , 4 ),
                new TupleLiteral( 1 , 6 , 1 , 5 )
        };
        assert checkSizeResults(expected4, result);
        assert checkUnorderedResults(expected4, result);

        result = server.doCommand(
                "SELECT * FROM empty_table_1 LEFT OUTER JOIN empty_table_2", true);
        System.err.println("HAVING RESULTS = " + result.getTuples());
        TupleLiteral[] expected5 = {
        };
        assert checkSizeResults(expected5, result);
        assert checkUnorderedResults(expected5, result);

        result = server.doCommand(
                "SELECT * FROM empty_table_1 LEFT OUTER JOIN simple_1", true);
        TupleLiteral[] expected6 = {
        };
        assert checkSizeResults(expected6, result);
        assert checkUnorderedResults(expected6, result);

        result = server.doCommand(
                "SELECT * FROM simple_1 LEFT OUTER JOIN empty_table_1", true);
        TupleLiteral[] expected7 = {
        };
        assert checkSizeResults(expected7, result);
        assert checkUnorderedResults(expected7, result);

        result = server.doCommand(
                "SELECT * FROM simple_1 LEFT OUTER JOIN simple_2", true);
        TupleLiteral[] expected8 = {
                new TupleLiteral( 1 , 2 , 3 ),
                new TupleLiteral( 1 , 2 , 4 ),
                new TupleLiteral( 1 , 2 , 5 ),
                new TupleLiteral( 1 , 6 , 3 ),
                new TupleLiteral( 1 , 6 , 4 ),
                new TupleLiteral( 1 , 6 , 5 ),
                new TupleLiteral( 3 , 9 , null )
        };
        assert checkSizeResults(expected8, result);
        assert checkUnorderedResults(expected8, result);

        result = server.doCommand(
                "SELECT * FROM empty_table_1 RIGHT OUTER JOIN empty_table_2", true);
        System.err.println("HAVING RESULTS = " + result.getTuples());
        TupleLiteral[] expected9 = {
        };
        assert checkSizeResults(expected9, result);
        assert checkUnorderedResults(expected9, result);

        result = server.doCommand(
                "SELECT * FROM empty_table_1 RIGHT OUTER JOIN simple_1", true);
        TupleLiteral[] expected10 = {
        };
        assert checkSizeResults(expected10, result);
        assert checkUnorderedResults(expected10, result);

        result = server.doCommand(
                "SELECT * FROM simple_1 RIGHT OUTER JOIN empty_table_1", true);
        TupleLiteral[] expected11 = {
        };
        assert checkSizeResults(expected11, result);
        assert checkUnorderedResults(expected11, result);

        result = server.doCommand(
                "SELECT a, b, c FROM simple_1 INNER JOIN simple_2", true);
        TupleLiteral[] expected12 = {
                new TupleLiteral( 1 , 2 , 3 ),
                new TupleLiteral( 1 , 2 , 4 ),
                new TupleLiteral( 1 , 2 , 5 ),
                new TupleLiteral( 1 , 6 , 3 ),
                new TupleLiteral( 1 , 6 , 4 ),
                new TupleLiteral( 1 , 6 , 5 ),
                new TupleLiteral( 4 , null , 7 )
        };
        assert checkSizeResults(expected12, result);
        assert checkUnorderedResults(expected12, result);
    }
}
