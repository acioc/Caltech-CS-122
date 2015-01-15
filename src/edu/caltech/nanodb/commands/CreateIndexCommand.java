package edu.caltech.nanodb.commands;


import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.storage.StorageManager;


/**
 * This command-class represents the <tt>CREATE INDEX</tt> DDL command.
 */
public class CreateIndexCommand extends Command {
    /** A logging object for reporting anything interesting that happens. **/
    private static Logger logger = Logger.getLogger(CreateIndexCommand.class);


    /** The name of the index being created. */
    private String indexName;


    /**
     * This flag specifies whether the index is a unique index or not.  If the
     * value is true then no key-value may appear multiple times; if the value
     * is false then a key-value may appear multiple times.
     */
    private boolean unique;


    /** The name of the table that the index is built against. */
    private String tableName;


    /**
     * The list of column-names that the index is built against.  The order of
     * these values is important; for ordered indexes, the index records must be
     * kept in the order specified by the sequence of column names.
     */
    private ArrayList<String> columnNames = new ArrayList<String>();


    /** Any additional properties specified in the command. */
    private CommandProperties properties;


    public CreateIndexCommand(String indexName, String tableName,
                              boolean unique) {
        super(Type.DDL);

        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        this.indexName = indexName;
        this.tableName = tableName;
        this.unique = unique;
    }


    public boolean isUnique() {
        return unique;
    }


    public void setProperties(CommandProperties properties) {
        this.properties = properties;
    }


    public CommandProperties getProperties() {
        return properties;
    }


    public void addColumn(String columnName) {
        this.columnNames.add(columnName);
    }

    public void addColumns(List<String> columnNames) {
        this.columnNames.addAll(columnNames);
    }


    @Override
    public void execute(StorageManager storageManager)
        throws ExecutionException {

        throw new ExecutionException("Not yet implemented!");
    }
}
