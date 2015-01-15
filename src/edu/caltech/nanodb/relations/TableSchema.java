package edu.caltech.nanodb.relations;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * This class extends the {@link Schema} class with features specific to
 * tables, such as the ability to specify primary-key, foreign-key, and other
 * candidate-key constraints, indexes on columns, and so forth.
 */
public class TableSchema extends Schema {

    /**
     * This object specifies the primary key on this table.  This key is not
     * also included in the {@link #candidateKeys} collection.
     */
    private KeyColumnIndexes primaryKey;


    /** A set recording which columns have NOT NULL constraints on them */
    private HashSet<Integer> notNullCols = new HashSet<Integer>();


    /**
     * A collection of candidate-key objects specifying the sets of columns that
     * comprise candidate keys on this table.  This collection does not include
     * the primary key specified by the {@link #primaryKey} field.
     */
    private ArrayList<KeyColumnIndexes> candidateKeys =
        new ArrayList<KeyColumnIndexes>();


    /**
     * A collection of foreign-key objects specifying other tables that this
     * table references.
     */
    private ArrayList<ForeignKeyColumnIndexes> foreignKeys =
        new ArrayList<ForeignKeyColumnIndexes>();


    /**
     * This collection provides easy access to all the indexes defined on this
     * table, including those for candidate keys and the primary key.
     */
    private HashMap<String, ColumnIndexes> indexes =
        new HashMap<String, ColumnIndexes>();


    /**
     * Adds a column with given index to list of NOT NULL constrained columns.
     *
     * @param colIndex the integer index of the column to NOT NULL constrain.
     */
    public boolean addNotNull(int colIndex) {
        if (colIndex < 0 || colIndex >= numColumns()) {
            throw new IllegalArgumentException("Column index must be between" +
                " 0 and " + numColumns() + "; got " + colIndex + " instead.");
        }
        return notNullCols.add(colIndex);
    }


    /**
     * Removes a column with given index from the list of NOT NULL constrained
     * columns.
     *
     * @param colIndex the integer index of the column to remove the NOT NULL 
     *        constraint from.
     */
    public boolean removeNotNull(int colIndex) {
        if (colIndex < 0 || colIndex >= numColumns()) {
            throw new IllegalArgumentException("Column index must be between" +
                " 0 and " + numColumns() + "; got " + colIndex + " instead.");
        }
        return notNullCols.remove(colIndex);
    }
    
    
    /**
     * Returns a set of columns that have a NOT NULL constraint, specified by
     * the indexes of the columns in the table schema.
     *
     * @return set of integers - indexes of columns with NOT NULL constraint
     */
    public Set<Integer> getNotNull() {
        return Collections.unmodifiableSet(notNullCols);
    }
    
    
    /**
     * Sets the primary key on this table.
     *
     * @param pk the primary key to set on the table, or <tt>null</tt> if the
     *        table has no primary key.
     */
    public void setPrimaryKey(KeyColumnIndexes pk) {
        if (pk == null)
            throw new IllegalArgumentException("pk cannot be null");

        if (pk.getIndexName() == null)
            throw new IllegalArgumentException("pk must specify an index name");

        if (primaryKey != null)
            throw new IllegalStateException("Table already has a primary key");
        
        primaryKey = pk;
        indexes.put(pk.getIndexName(), pk);
    }


    /**
     * Returns the primary key on this table, or <tt>null</tt> if there is no
     * primary key.
     *
     * @return the primary key on this table, or <tt>null</tt> if there is no
     *         primary key.
     */
    public KeyColumnIndexes getPrimaryKey() {
        return primaryKey;
    }


    public void addCandidateKey(KeyColumnIndexes ck) {
        if (ck == null)
            throw new IllegalArgumentException("ck cannot be null");
        
        if (ck.getIndexName() == null)
            throw new IllegalArgumentException("ck must specify an index name");

        if (candidateKeys == null)
            candidateKeys = new ArrayList<KeyColumnIndexes>();

        candidateKeys.add(ck);
        indexes.put(ck.getIndexName(), ck);
    }


    public void addIndex(ColumnIndexes index) {
        if (index == null)
            throw new IllegalArgumentException("index cannot be null");

        if (index.getIndexName() == null)
            throw new IllegalArgumentException("index must specify an index name");

        indexes.put(index.getIndexName(), index);
    }

    public void addRefTable(String tblName, String idxName, 
        int[] referencedColumns) {
        // Find the primary or candidate key corresponding to referencedColumns
        KeyColumnIndexes referencedIdx = null;
        if (primaryKey != null && 
            primaryKey.hasSameColumns(new ColumnIndexes(referencedColumns))) {
            referencedIdx = primaryKey;
        }

        for (KeyColumnIndexes ck : candidateKeys) {
            if (ck.hasSameColumns(new ColumnIndexes(referencedColumns))) {
                referencedIdx = ck;
            }
        }

        // Add tblName and referencingColumns to the key
        if (referencedIdx == null) {
            throw new IllegalStateException("No primary or candidate key" +
            " found that corresponded to the columns specified for" +
            " adding referencing table.");
        }
        referencedIdx.addRef(tblName, idxName);
    }

    public void dropIndex(String idxName) {
        if (idxName == null)
            throw new IllegalArgumentException("drop index must specify an index name");

        if (!indexes.containsKey(idxName))
            throw new IllegalArgumentException("table does not have this index to drop");

        if (primaryKey != null && primaryKey.getIndexName().equals(idxName)) 
            primaryKey = null;
        Iterator<KeyColumnIndexes> it = candidateKeys.iterator();
        while (it.hasNext()) {
            if (it.next().getIndexName().equals(idxName))
                it.remove();
        }
        /* Foreign keys should never be able to be dropped this way */

        indexes.remove(idxName);
    }


    public int numCandidateKeys() {
        return candidateKeys.size();
    }


    public List<KeyColumnIndexes> getCandidateKeys() {
        return Collections.unmodifiableList(candidateKeys);
    }


    /**
     * This helper function returns <tt>true</tt> if this table has a primary or
     * candidate key on the set of columns specified in the argument <tt>k</tt>.
     * This method is used to determine if a foreign key references a candidate
     * key on this table.
     *
     * @param k the key to check against this table to see if it's a
     *        candidate key
     *
     * @return true if this table contains a primary or candidate key on the
     *         columns specified in <tt>k</tt>
     */
    public boolean hasKeyOnColumns(ColumnIndexes k) {
        if (primaryKey != null && primaryKey.hasSameColumns(k))
            return true;

        for (KeyColumnIndexes ck : candidateKeys) {
            if (ck.hasSameColumns(k))
                return true;
        }

        return false;
    }

    public String keyNameOnColumns(ColumnIndexes k) {
        if (primaryKey != null && primaryKey.hasSameColumns(k)) {
            return primaryKey.getIndexName();
        }
        
        for (KeyColumnIndexes ck : candidateKeys) {
            if (ck.hasSameColumns(k))
                return ck.getIndexName();
        }

        return null;
    }

    public void addForeignKey(ForeignKeyColumnIndexes fk) {
        if (foreignKeys == null)
            foreignKeys = new ArrayList<ForeignKeyColumnIndexes>();

        foreignKeys.add(fk);
    }


    public int numForeignKeys() {
        return foreignKeys.size();
    }


    public List<ForeignKeyColumnIndexes> getForeignKeys() {
        return Collections.unmodifiableList(foreignKeys);
    }


    public Map<String, ColumnIndexes> getIndexes() {
        return Collections.unmodifiableMap(indexes);
    }


    /**
     * Given a set of column names, this method returns the names of all
     * indexes built on these columns.
     *
     * @param columnNames the names of columns to test for
     * @return a list of index names built on the specified columns
     */
    public List<String> getIndexName(List<String> columnNames) {
        int[] colIndexes = getColumnIndexes(columnNames);
        ColumnIndexes index = new ColumnIndexes(colIndexes);

        ArrayList<String> indexNames = new ArrayList<String>();
        for (Map.Entry<String, ColumnIndexes> entry : indexes.entrySet()) {
            if (index.hasSameColumns(entry.getValue()))
                indexNames.add(entry.getKey());
        }

        return indexNames;
    }
}
