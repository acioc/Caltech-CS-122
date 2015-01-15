package edu.caltech.nanodb.commands;


import java.util.LinkedHashSet;

import edu.caltech.nanodb.storage.StorageManager;


/**
 * This Command class represents the <tt>VERIFY</tt> SQL command, which
 * verifies a table's representation (along with any indexes) to ensure that
 * all structural details are valid.  This is not a standard SQL command, but
 * it is very useful for verifying student implementations of file structures.
 */
public class VerifyCommand extends Command {

    /**
     * Table names are kept in a set so that we don't need to worry about a
     * particular table being specified multiple times.
     */
    private LinkedHashSet<String> tableNames;


    /**
     * Construct a new <tt>VERIFY</tt> command with an empty table list.
     * Tables can be added to the internal list using the {@link #addTable}
     * method.
     */
    public VerifyCommand() {
        super(Command.Type.UTILITY);
        tableNames = new LinkedHashSet<String>();
    }


    /**
     * Construct a new <tt>VERIFY</tt> command to verify the specified table.
     *
     * @param tableName the name of the table to verify.
     */
    public VerifyCommand(String tableName) {
        this();
        addTable(tableName);
    }


    /**
     * Add a table to the list of tables to verify.
     *
     * @param tableName the name of the table to verify.
     */
    public void addTable(String tableName) {
        if (tableName == null)
            throw new NullPointerException("tableName cannot be null");

        tableNames.add(tableName);
    }


    @Override
    public void execute(StorageManager storageManager)
        throws ExecutionException {

        throw new ExecutionException("Not yet implemented!");
    }


    /**
     * Prints a simple representation of the verify command, including the
     * names of the tables to be verified.
     *
     * @return a string representing this verify command
     */
    @Override
    public String toString() {
        return "Verify[" + tableNames + "]";
    }
}
