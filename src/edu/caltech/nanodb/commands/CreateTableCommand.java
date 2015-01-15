package edu.caltech.nanodb.commands;


import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.TableConstraintType;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.relations.TableSchema;

import edu.caltech.nanodb.storage.TableManager;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This command handles the <tt>CREATE TABLE</tt> DDL operation.
 */
public class CreateTableCommand extends Command {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(CreateTableCommand.class);


    public static final String PROP_PAGESIZE = "pagesize";


    public static final String PROP_TYPE = "type";


    /** Name of the table to be created. */
    private String tableName;

    /** If this flag is {@code true} then the table is a temporary table. */
    private boolean temporary;


    /**
     * If this flag is {@code true} then the create-table operation should
     * only be performed if the specified table doesn't already exist.
     */
    private boolean ifNotExists;


    /** List of column-declarations for the new table. */
    private List<ColumnInfo> columnInfos = new ArrayList<ColumnInfo>();


    /** List of constraints for the new table. */
    private List<ConstraintDecl> constraints = new ArrayList<ConstraintDecl>();


    /** Any additional properties specified in the command. */
    private CommandProperties properties;


    /**
     * Create a new object representing a <tt>CREATE TABLE</tt> statement.
     *
     * @param tableName the name of the table to be created
     * @param temporary true if the table is a temporary table, false otherwise
     * @param ifNotExists If this flag is true, the table will only be created
     *        if it doesn't already exist.
     */
    public CreateTableCommand(String tableName,
                              boolean temporary, boolean ifNotExists) {
        super(Command.Type.DDL);

        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        this.tableName = tableName;
        this.temporary = temporary;
        this.ifNotExists = ifNotExists;
    }


    public void setProperties(CommandProperties properties) {
        this.properties = properties;
    }


    public CommandProperties getProperties() {
        return properties;
    }


    /**
     * Adds a column description to this create-table command.  This method is
     * primarily used by the SQL parser.
     *
     * @param colInfo the details of the column to add
     *
     * @throws NullPointerException if colDecl is null
     */
    public void addColumn(ColumnInfo colInfo) {
        if (colInfo == null)
            throw new IllegalArgumentException("colInfo cannot be null");

        if (!tableName.equals(colInfo.getTableName())) {
            colInfo = new ColumnInfo(colInfo.getName(), tableName,
                colInfo.getType());
        }

        columnInfos.add(colInfo);
    }


    /**
     * Adds a constraint to this create-table command.  This method is primarily
     * used by the SQL parser.
     *
     * @param con the details of the table constraint to add
     *
     * @throws NullPointerException if con is null
     */
    public void addConstraint(ConstraintDecl con) {
        if (con == null)
            throw new IllegalArgumentException("con cannot be null");

        constraints.add(con);
    }


    @Override
    public void execute(StorageManager storageManager)
        throws ExecutionException {

        TableManager tableManager = storageManager.getTableManager();

        // See if the table already exists.
        if (ifNotExists) {
            try {
                if (tableManager.tableExists(tableName)) {
                    out.printf("Table %s already exists; skipping create-table.%n",
                        tableName);
                    return;
                }
            }
            catch (IOException e) {
                // Some other unexpected exception occurred.  Report an error.
                throw new ExecutionException(
                    "Exception while trying to determine if table " +
                    tableName + " exists.", e);
            }
        }

        // Set up the table-file info based on the command details.

        logger.debug("Creating a TableSchema object for the new table " +
            tableName + ".");

        TableSchema schema = new TableSchema();
        for (ColumnInfo colInfo : columnInfos) {
            try {
                schema.addColumnInfo(colInfo);
            }
            catch (IllegalArgumentException iae) {
                throw new ExecutionException("Duplicate or invalid column \"" +
                    colInfo.getName() + "\".", iae);
            }
        }

        // Open all tables referenced by foreign-key constraints, so that we
        // can verify the constraints.
        HashMap<String, TableSchema> referencedTables =
            new HashMap<String, TableSchema>();
        for (ConstraintDecl cd: constraints) {
            if (cd.getType() == TableConstraintType.FOREIGN_KEY) {
                String refTableName = cd.getRefTable();
                try {
                    TableInfo refTblInfo = tableManager.openTable(refTableName);
                    TableSchema refSchema = refTblInfo.getSchema();
                    referencedTables.put(refTableName, refSchema);
                }
                catch (FileNotFoundException e) {
                    throw new ExecutionException(String.format(
                        "Referenced table %s doesn't exist.", refTableName), e);
                }
                catch (IOException e) {
                    throw new ExecutionException(String.format(
                        "Error while loading schema for referenced table %s.",
                        refTableName), e);
                }
            }
        }

        // Get the table manager and create the table.

        logger.debug("Creating the new table " + tableName + " on disk.");
        try {
            tableManager.createTable(tableName, schema, properties);
        }
        catch (IOException ioe) {
            throw new ExecutionException("Could not create table \"" +
                tableName + "\".  See nested exception for details.", ioe);
        }
        logger.debug("New table " + tableName + " was created.");

        out.println("Created table:  " + tableName);
    }


    @Override
    public String toString() {
        return "CreateTable[" + tableName + "]";
    }


    /**
     * Returns a verbose, multi-line string containing all of the details of
     * this table.
     *
     * @return a detailed description of the table described by this command
     */
    public String toVerboseString() {
        StringBuilder strBuf = new StringBuilder();

        strBuf.append(toString());
        strBuf.append('\n');

        for (ColumnInfo colInfo : columnInfos) {
            strBuf.append('\t');
            strBuf.append(colInfo.toString());
            strBuf.append('\n');
        }

        for (ConstraintDecl con : constraints) {
            strBuf.append('\t');
            strBuf.append(con.toString());
            strBuf.append('\n');
        }

        return strBuf.toString();
    }
}
