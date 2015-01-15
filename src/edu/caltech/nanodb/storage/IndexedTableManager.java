package edu.caltech.nanodb.storage;


import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.commands.CommandProperties;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.relations.TableSchema;


/**
 * This class provides an implementation of the {@link TableManager} interface
 * for tables that can have indexes and constraints on them.
 */
public class IndexedTableManager implements TableManager {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(IndexedTableManager.class);


    private StorageManager storageManager;

    private HashMap<String, TableInfo> openTables;


    public IndexedTableManager(StorageManager storageManager) {
        this.storageManager = storageManager;
        openTables = new HashMap<String, TableInfo>();
    }

    /**
     * This method takes a table name and returns a filename string that
     * specifies where the table's data is stored.
     *
     * @param tableName the name of the table to get the filename of
     *
     * @return the name of the file that holds the table's data
     */
    private String getTableFileName(String tableName) {
        return tableName + ".tbl";
    }


    // Inherit interface docs.
    @Override
    public boolean tableExists(String tableName) throws IOException {
        String tblFileName = getTableFileName(tableName);
        FileManager fileManager = storageManager.getFileManager();

        return fileManager.fileExists(tblFileName);
    }


    // Inherit interface docs.
    @Override
    public TableInfo createTable(String tableName, TableSchema schema,
        CommandProperties properties) throws IOException {

        int pageSize = StorageManager.getCurrentPageSize();
        String storageType = "heap";

        if (properties != null) {
            logger.info("Using command properties " + properties);

            pageSize = properties.getInt("pagesize", pageSize);
            storageType = properties.getString("storage", storageType);

            HashSet<String> names = new HashSet<String>(properties.getNames());
            names.remove("pagesize");
            names.remove("storage");
            if (!names.isEmpty()) {
                throw new IllegalArgumentException("Unrecognized property " +
                    "name(s) specified:  " + names);
            }
        }

        String tblFileName = getTableFileName(tableName);

        DBFileType type;
        if ("heap".equals(storageType)) {
            type = DBFileType.HEAP_TUPLE_FILE;
        }
        else if ("btree".equals(storageType)) {
            type = DBFileType.BTREE_TUPLE_FILE;
        }
        else {
            throw new IllegalArgumentException("Unrecognized table file " +
                "type:  " + storageType);
        }
        TupleFileManager tupleFileManager = storageManager.getTupleFileManager(type);

        // First, create a new DBFile that the tuple file will go into.
        FileManager fileManager = storageManager.getFileManager();
        DBFile dbFile = fileManager.createDBFile(tblFileName, type, pageSize);
        logger.debug("Created new DBFile for table " + tableName +
                     " at path " + dbFile.getDataFile());

        // Now, initialize it to be a tuple file with the specified type and
        // schema.
        TupleFile tupleFile = tupleFileManager.createTupleFile(dbFile, schema);

        // Cache this table since it's now considered "open".
        TableInfo tableInfo = new TableInfo(tableName, tupleFile);
        openTables.put(tableName, tableInfo);

        return tableInfo;
    }


    // Inherit interface docs.
    @Override
    public void saveTableInfo(TableInfo tableInfo) throws IOException {
        // TODO:  Implement!
        throw new UnsupportedOperationException("NYI");
    }


    // Inherit interface docs.
    @Override
    public TableInfo openTable(String tableName) throws IOException {
        TableInfo tableInfo;

        // If the table is already open, just return the cached information.
        tableInfo = openTables.get(tableName);
        if (tableInfo != null)
            return tableInfo;

        // Open the data file for the table; read out its type and page-size.

        String tblFileName = getTableFileName(tableName);
        TupleFile tupleFile = storageManager.openTupleFile(tblFileName);
        tableInfo = new TableInfo(tableName, tupleFile);

        // Cache this table since it's now considered "open".
        openTables.put(tableName, tableInfo);

        return tableInfo;
    }


    // Inherit interface docs.
    @Override
    public void analyzeTable(TableInfo tableInfo) throws IOException {
        // Analyze the table's tuple-file.
        tableInfo.getTupleFile().analyze();

        // TODO:  Probably want to analyze all the indexes associated with
        //        the table as well...
    }


    // Inherit interface docs.
    @Override
    public void closeTable(TableInfo tableInfo) throws IOException {
        // Remove this table from the cache since it's about to be closed.
        openTables.remove(tableInfo.getTableName());

        DBFile dbFile = tableInfo.getTupleFile().getDBFile();

        // Flush all open pages for the table.
        storageManager.getBufferManager().flushDBFile(dbFile);
        storageManager.getFileManager().closeDBFile(dbFile);
    }


    // Inherit interface docs.
    @Override
    public void dropTable(String tableName) throws IOException {
        TableInfo tableInfo = openTable(tableName);

        if (StorageManager.ENABLE_INDEXES)
            dropTableIndexes(tableInfo);

        // Close the table.  This will purge out all dirty pages for the table
        // as well.
        closeTable(tableInfo);

        String tblFileName = getTableFileName(tableName);
        storageManager.getFileManager().deleteDBFile(tblFileName);
    }


    private void dropTableIndexes(TableInfo tableInfo) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented!");
    }
}
