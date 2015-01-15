package edu.caltech.nanodb.commands;


import edu.caltech.nanodb.storage.StorageManager;

import org.apache.log4j.Logger;


/**
 * This command-class represents the <tt>DROP INDEX</tt> DDL command.
 */
public class DropIndexCommand extends Command {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(DropIndexCommand.class);


    /** The name of the index to drop. */
    private String indexName;


    /** The name of the table that the index is built against. */
    private String tableName;


    public DropIndexCommand(String indexName, String tableName) {
        super(Type.DDL);

        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        this.indexName = indexName;
        this.tableName = tableName;
    }


    @Override
    public void execute(StorageManager storageManager)
        throws ExecutionException {
        throw new ExecutionException("Not yet implemented!");
    }
}
