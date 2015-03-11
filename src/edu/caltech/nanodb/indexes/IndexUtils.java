package edu.caltech.nanodb.indexes;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.relations.ColumnRefs;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.ForeignKeyColumnRefs;
import edu.caltech.nanodb.relations.ForeignKeyValueChangeOption;
import edu.caltech.nanodb.relations.SQLDataType;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.HashedTupleFile;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.SequentialTupleFile;
import edu.caltech.nanodb.storage.TupleFile;


/**
 * This class provides a number of very useful utility operations that make it
 * easier to create and work with indexes.
 */
public class IndexUtils {


    public static TableSchema makeIndexSchema(TableSchema tableSchema,
                                              ColumnRefs indexDesc) {
        if (tableSchema == null)
            throw new IllegalArgumentException("tableSchema cannot be null");

        if (indexDesc == null)
            throw new IllegalArgumentException("indexDesc cannot be null");

        // Get the name of the table that the index will be built against.
        // (And, make sure there is only one table name in the schema...)
        Set<String> tableNames = tableSchema.getTableNames();
        if (tableSchema.getTableNames().size() != 1) {
            throw new IllegalArgumentException(
                "Schema must have exactly one table name");
        }

        String tableName = tableNames.iterator().next();

        // Add all the referenced columns from the table schema.
        TableSchema indexSchema = new TableSchema();
        for (int iCol : indexDesc.getCols())
            indexSchema.addColumnInfo(tableSchema.getColumnInfo(iCol));

        // Add a tuple-pointer field for the index as well.
        ColumnInfo filePtr = new ColumnInfo("#TUPLE_PTR", tableName,
            new ColumnType(SQLDataType.FILE_POINTER));
        indexSchema.addColumnInfo(filePtr);

        return indexSchema;
    }


    /**
     * This method constructs a <tt>ColumnIndexes</tt> object that includes
     * the columns named in the input list.  Note that this method <u>does
     * not</u> update the schema stored on disk, or create any other
     * supporting files.
     *
     * @param columnNames a list of column names that are in the index
     *
     * @return a new <tt>ColumnIndexes</tt> object with the indexes of the
     *         columns stored in the object
     *
     * @throws SchemaNameException if a column-name cannot be found, or if a
     *         column-name is ambiguous (unlikely), or if a column is
     *         specified multiple times in the input list.
     * /
    public static ColumnRefs makeIndex(Schema tableSchema,
                                       List<String> columnNames) {
        if (columnNames == null)
            throw new IllegalArgumentException("columnNames must be specified");

        if (columnNames.isEmpty()) {
            throw new IllegalArgumentException(
                "columnNames must specify at least one column");
        }

        return new ColumnRefs(tableSchema.getColumnIndexes(columnNames));
    }


    /**
     * This method constructs a {@code KeyColumnRefs} object that includes the
     * columns named in the input list.  Note that this method <u>does not</u>
     * update the schema stored on disk, or create any other supporting files.
     *
     * @param columnNames a list of column names that are in the key
     *
     * @return a new {@code KeyColumnRefs} object with the indexes of the
     *         columns stored in the object
     *
     * @throws SchemaNameException if a column-name cannot be found, or if a
     *         column-name is ambiguous (unlikely), or if a column is
     *         specified multiple times in the input list.
     * /
    public static KeyColumnRefs makeKey(Schema tableSchema,
                                        List<String> columnNames) {
        if (columnNames == null)
            throw new IllegalArgumentException("columnNames must be specified");

        if (columnNames.isEmpty()) {
            throw new IllegalArgumentException(
                "columnNames must specify at least one column");
        }

        int[] colIndexes = tableSchema.getColumnIndexes(columnNames);
        return new KeyColumnRefs(colIndexes);
    }
    */

    /**
     * This method constructs a {@link ForeignKeyColumnRefs} object that
     * includes the columns named in the input list, as well as the referenced
     * table and column names.  Note that this method <u>does not</u> update
     * the schema stored on disk, or create any other supporting files.
     *
     * @param columnNames a list of column names that are in the key
     *
     * @param refTableName the table referenced by this key
     *
     * @param refTableSchema the schema of the table referenced by this key
     *
     * @param refColumnNames the columns in the referenced table that this
     *        table's columns reference
     *
     * @param onDelete the {@link ForeignKeyValueChangeOption} for ON DELETE
     *
     * @param onUpdate the {@link ForeignKeyValueChangeOption} for ON UPDATE
     *
     * @return a new <tt>ForeignKeyColumns</tt> object with the indexes of the
     *         columns stored in the object
     *
     * @throws SchemaNameException if a column-name cannot be found, or if a
     *         column-name is ambiguous (unlikely), or if a column is specified
     *         multiple times in the input list.
     */
    public static ForeignKeyColumnRefs makeForeignKey(TableSchema tableSchema,
        List<String> columnNames, String refTableName,
        TableSchema refTableSchema, List<String> refColumnNames,
        ForeignKeyValueChangeOption onDelete,
        ForeignKeyValueChangeOption onUpdate) {

        if (tableSchema == null)
            throw new IllegalArgumentException("tableSchema must be specified");

        if (columnNames == null)
            throw new IllegalArgumentException("columnNames must be specified");

        if (refTableName == null)
            throw new IllegalArgumentException("refTableName must be specified");

        if (refTableSchema == null)
            throw new IllegalArgumentException("refTableSchema must be specified");

        if (refColumnNames == null)
            throw new IllegalArgumentException("refColumnNames must be specified");

        if (columnNames.isEmpty()) {
            throw new IllegalArgumentException(
                "columnNames must specify at least one column");
        }

        if (columnNames.size() != refColumnNames.size()) {
            throw new IllegalArgumentException("columnNames and " +
                "refColumnNames must specify the same number of columns");
        }

        int[] colIndexes = tableSchema.getColumnIndexes(columnNames);
        int[] refColIndexes = refTableSchema.getColumnIndexes(refColumnNames);

        if (!refTableSchema.hasKeyOnColumns(new ColumnRefs(refColIndexes))) {
            throw new SchemaNameException(String.format("Referenced columns " +
                    "%s in table %s are not a primary or candidate key",
                refColumnNames, refTableName));
        }

        ArrayList<ColumnInfo> myColInfos = tableSchema.getColumnInfos(colIndexes);
        ArrayList<ColumnInfo> refColInfos = refTableSchema.getColumnInfos(refColIndexes);

        // Check if the column relations are the same types
        for (int i = 0; i < myColInfos.size(); i++) {
            ColumnType myType = myColInfos.get(i).getType();
            ColumnType refType = refColInfos.get(i).getType();
            if (!myType.equals(refType)) {
                throw new IllegalArgumentException("columns in " +
                    "child and parent tables of the foreign key must be " +
                    "of the same type!");
            }
        }

        // The onDelete and onUpdate values could be null if they are
        // unspecified in the constructor.  They are set to
        // ForeignKeyValueChangeOption.RESTRICT as a default in this case in
        // the constructor for ForeignKeyColumnIndexes.
        return new ForeignKeyColumnRefs(colIndexes, refTableName,
            refColIndexes, onDelete, onUpdate);
    }


    /**
     * This helper function creates a {@link TupleLiteral} that holds the
     * key-values necessary for storing or deleting the specified table-tuple
     * in the index.  Specifically, this method stores the tuple's file-pointer
     * in the key as the last value.
     *
     * @param columnRefs the columns that the index is built on
     *
     * @param tuple the tuple from the original table, that the key will be
     *        created from.
     *
     * @return a tuple-literal that can be used for storing, looking up, or
     *         deleting the specific tuple {@code ptup}.
     */
    public static TupleLiteral makeSearchKeyValue(ColumnRefs columnRefs,
        Tuple tuple, boolean findExactTuple) {

        // Build up a new tuple-literal containing the search key.
        TupleLiteral searchKeyVal = new TupleLiteral();
        for (int i = 0; i < columnRefs.size(); i++)
            searchKeyVal.addValue(tuple.getColumnValue(columnRefs.getCol(i)));

        if (findExactTuple) {
            // Include the file-pointer as the last value in the tuple, so
            // that all key-values are unique in the index.
            searchKeyVal.addValue(((PageTuple) tuple).getExternalReference());
        }

        return searchKeyVal;
    }

/*
    public static void setSearchKeyStorageSize(IndexInfo indexInfo,
                                               TupleLiteral searchKeyVal) {

        List<ColumnInfo> colInfos = indexInfo.getIndexSchema();
        int storageSize = PageTuple.getTupleStorageSize(colInfos, searchKeyVal);
        searchKeyVal.setStorageSize(storageSize);
    }
*/


    public static PageTuple findTupleInIndex(Tuple key, TupleFile idxTupleFile)
        throws IOException {

        PageTuple idxPageTup;

        if (idxTupleFile instanceof SequentialTupleFile) {
            SequentialTupleFile seqTupleFile = (SequentialTupleFile) idxTupleFile;
            idxPageTup = (PageTuple) seqTupleFile.findFirstTupleEquals(key);
        }
        else if (idxTupleFile instanceof HashedTupleFile) {
            HashedTupleFile hashTupleFile = (HashedTupleFile) idxTupleFile;
            idxPageTup = (PageTuple) hashTupleFile.findFirstTupleEquals(key);
        }
        else {
            throw new IllegalStateException("Index files must " +
                "be sequential or hashing tuple files.");
        }

        return idxPageTup;
    }


    public static List<String> verifyIndex(TupleFile tableTupleFile,
        TupleFile indexTupleFile) throws IOException {

        ArrayList<String> errors = new ArrayList<String>();
        HashSet<FilePointer> tableTuples = new HashSet<FilePointer>();
        HashSet<FilePointer> indexTuples = new HashSet<FilePointer>();
        Tuple tup;

        // Scan through all tuples in the table file, and record the file
        // pointer to each one.
        tup = tableTupleFile.getFirstTuple();
        while (tup != null) {
            if (!tableTuples.add(tup.getExternalReference())) {
                // This should never happen.
                throw new IllegalStateException("The impossible has " +
                    "happened:  two tuples had the same external reference!");
            }
            tup = tableTupleFile.getNextTuple(tup);
        }

        // Scan through all entries in the index, and record the file pointer
        // stored in each index record.
        Schema indexSchema = indexTupleFile.getSchema();
        int iCol = indexSchema.getColumnIndex("#TUPLE_PTR");

        tup = indexTupleFile.getFirstTuple();
        while (tup != null) {
            FilePointer fptr = (FilePointer) tup.getColumnValue(iCol);

            if (indexTuples.contains(fptr)) {
                errors.add("Tuple at location " + fptr +
                    " appears multiple times in the index.");
            }

            if (!tableTuples.contains(fptr)) {
                errors.add("Tuple at location " + fptr +
                    " doesn't appear in the index.");
            }

            indexTuples.add(fptr);

            tup = indexTupleFile.getNextTuple(tup);
        }

        return errors;
    }
}
