package edu.caltech.nanodb.commands;


import java.io.IOException;
import java.io.PrintStream;

import edu.caltech.nanodb.plans.PlanNode;

import edu.caltech.nanodb.qeval.EvalStats;
import edu.caltech.nanodb.qeval.Planner;
import edu.caltech.nanodb.qeval.PlannerFactory;
import edu.caltech.nanodb.qeval.QueryEvaluator;
import edu.caltech.nanodb.qeval.TupleProcessor;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;
import edu.caltech.nanodb.relations.Tuple;

import edu.caltech.nanodb.storage.StorageManager;


/**
 * <p>
 * This command object represents a <tt>DUMP TABLE</tt> command issued against
 * the database.  <tt>DUMP TABLE</tt> commands are pretty simple, having a
 * single form:   <tt>DUMP TABLE ... [TO FILE ...] [FORMAT ...]</tt>.
 * </p>
 * <p>
 * This command is effectively identical to <tt>SELECT * FROM tbl;</tt> with
 * the results being output to console or a data file for analysis.  The
 * command is particularly helpful when the planner only implements
 * {@link Planner#makeSimpleSelect}; no planning is needed for this command
 * to work.
 * </p>
 */
public class DumpTableCommand extends Command {

    /**
     * An implementation of the tuple processor interface used by the
     * {@link DumpTableCommand} to dump each tuple.
     */
    private static class TupleExporter implements TupleProcessor {
        private PrintStream dumpOut;


        /**
         * Initialize the tuple-exporter object with the details it needs to
         * print out tuples from the specified table.
         */
        public TupleExporter(PrintStream dumpOut) {
            this.dumpOut = dumpOut;
        }


        /** The exporter can output the schema to the dump file. */
        public void setSchema(Schema schema) {
            dumpOut.print("{");

            boolean first = true;
            for (ColumnInfo colInfo : schema) {
                if (first)
                    first = false;
                else
                    dumpOut.print(",");

                String colName = colInfo.getName();
                String tblName = colInfo.getTableName();

                // TODO:  To only print out table-names when the column-name
                //        is ambiguous by itself, uncomment the first part and
                //        then comment out the next part.

                // Only print out the table name if there are multiple columns
                // with this column name.
                // if (schema.numColumnsWithName(colName) > 1 && tblName != null)
                //     out.print(tblName + '.');

                // If table name is specified, always print it out.
                if (tblName != null)
                    dumpOut.print(tblName + '.');

                dumpOut.print(colName);

                dumpOut.print(":");
                dumpOut.print(colInfo.getType());
            }
            dumpOut.println("}");
        }

        /** This implementation simply prints out each tuple it is handed. */
        public void process(Tuple tuple) throws IOException {
            dumpOut.print("[");
            boolean first = true;
            for (int i = 0; i < tuple.getColumnCount(); i++) {
                if (first)
                    first = false;
                else
                    dumpOut.print(", ");

                Object val = tuple.getColumnValue(i);
                if (val instanceof String)
                    dumpOut.printf("\"%s\"", val);
                else
                    dumpOut.print(val);
            }
            dumpOut.println("]");
        }

        public void finish() {
            // Not used
        }
    }


    /** The name of the table to dump. */
    private String tableName;


    /** The path and filename to dump the table data to, if desired. */
    private String fileName;


    /** The data format to use when dumping the table data. */
    private String format;


    //TableInfo tableInfo;


    /**
     * Constructs a new dump-table command.
     *
     * @param tableName the name of the table to dump
     *
     * @param fileName the path and file to dump the data to.  The console
     *        will be used if this is @code{null}.
     *
     * @param format the format to dump the data in
     *
     * @throws IllegalArgumentException if tableName is null.
     */
    public DumpTableCommand(String tableName, String fileName, String format) {
        super(Type.UTILITY);

        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        this.tableName = tableName;
        this.fileName = fileName;
        this.format = format;
    }


    @Override
    public void execute(StorageManager storageManager)
        throws ExecutionException {

        try {
            // Figure out where the dumped data should go.
            PrintStream dumpOut = out;
            if (fileName != null)
                dumpOut = new PrintStream(fileName);

            // Dump the table.
            PlanNode dumpPlan = prepareDumpPlan(storageManager);
            TupleExporter exporter = new TupleExporter(dumpOut);
            EvalStats stats = QueryEvaluator.executePlan(dumpPlan, exporter);

            if (fileName != null)
                dumpOut.close();

            // Print out the evaluation statistics.
            out.printf("Dumped %d rows of table %s in %f sec.%n",
                stats.getRowsProduced(), tableName, stats.getElapsedTimeSecs());
        }
        catch (ExecutionException e) {
            throw e;
        }
        catch (Exception e) {
            throw new ExecutionException(e);
        }
    }


    protected PlanNode prepareDumpPlan(StorageManager storageManager)
            throws IOException, SchemaNameException {

        // Create a simple plan for scanning the table.
        Planner planner = PlannerFactory.getPlanner(storageManager);
        PlanNode plan = planner.makeSimpleSelect(tableName, null, null);
        plan.prepare();

        return plan;
    }


    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("DumpTableCommand[table=");
        sb.append(tableName);

        if (fileName != null) {
            sb.append(", filename=\"");
            sb.append(fileName);
            sb.append("\"");
        }

        if (format != null) {
            sb.append(", format=");
            sb.append(format);
        }

        sb.append(']');

        return sb.toString();
    }
}
