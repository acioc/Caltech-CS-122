package edu.caltech.nanodb.storage.heapfile;


import java.io.EOFException;
import java.io.IOException;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;

import edu.caltech.nanodb.qeval.ColumnStats;
import edu.caltech.nanodb.qeval.ColumnStatsCollector;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.SQLDataType;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.qeval.TableStats;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.relations.Tuple;

import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.TupleFile;
import edu.caltech.nanodb.storage.InvalidFilePointerException;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.StorageManager;

import javax.xml.crypto.Data;


/**
 * This class implements the TupleFile interface for heap files.
 */
public class HeapTupleFile implements TupleFile {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(HeapTupleFile.class);


    /**
     * The storage manager to use for reading and writing file pages, pinning
     * and unpinning pages, write-ahead logging, and so forth.
     */
    private StorageManager storageManager;


    /**
     * The manager for heap tuple files provides some higher-level operations
     * such as saving the metadata of a heap tuple file, so it's useful to
     * have a reference to it.
     */
    private HeapTupleFileManager heapFileManager;


    /** The schema of tuples in this tuple file. */
    private TableSchema schema;


    /** Statistics for this tuple file. */
    private TableStats stats;


    /** The file that stores the tuples. */
    private DBFile dbFile;


    public HeapTupleFile(StorageManager storageManager,
                         HeapTupleFileManager heapFileManager, DBFile dbFile,
                         TableSchema schema, TableStats stats) {
        if (storageManager == null)
            throw new IllegalArgumentException("storageManager cannot be null");

        if (heapFileManager == null)
            throw new IllegalArgumentException("heapFileManager cannot be null");

        if (dbFile == null)
            throw new IllegalArgumentException("dbFile cannot be null");

        if (schema == null)
            throw new IllegalArgumentException("schema cannot be null");

        if (stats == null)
            throw new IllegalArgumentException("stats cannot be null");

        this.storageManager = storageManager;
        this.heapFileManager = heapFileManager;
        this.dbFile = dbFile;
        this.schema = schema;
        this.stats = stats;
    }


    public TableSchema getSchema() {
        return schema;
    }

    @Override
    public TableStats getStats() {
        return stats;
    }


    public DBFile getDBFile() {
        return dbFile;
    }


    /**
     * Returns the first tuple in this table file, or <tt>null</tt> if
     * there are no tuples in the file.
     */
    @Override
    public Tuple getFirstTuple() throws IOException {
        try {
            // Scan through the data pages until we hit the end of the table
            // file.  It may be that the first run of data pages is empty,
            // so just keep looking until we hit the end of the file.

            // Header page is page 0, so first data page is page 1.

            for (int iPage = 1; /* nothing */ ; iPage++) {
                // Look for data on this page...

                DBPage dbPage = storageManager.loadDBPage(dbFile, iPage);
                int numSlots = DataPage.getNumSlots(dbPage);
                for (int iSlot = 0; iSlot < numSlots; iSlot++) {
                    // Get the offset of the tuple in the page.  If it's 0 then
                    // the slot is empty, and we skip to the next slot.
                    int offset = DataPage.getSlotValue(dbPage, iSlot);
                    if (offset == DataPage.EMPTY_SLOT)
                        continue;

                    //unpins the page once the tuple has been found
                    dbPage.unpin();

                    // This is the first tuple in the file.  Build up the
                    // HeapFilePageTuple object and return it.
                    return new HeapFilePageTuple(schema, dbPage, iSlot, offset);
                }

                // If we got here, the page has no tuples.  Unpin the page.
                dbPage.unpin();
            }
        }
        catch (EOFException e) {
            // We ran out of pages.  No tuples in the file!
            logger.debug("No tuples in table-file " + dbFile +
                         ".  Returning null.");
        }

        return null;
    }


    /**
     * Returns the tuple corresponding to the specified file pointer.  This
     * method is used by many other operations in the database, such as
     * indexes.
     *
     * @throws InvalidFilePointerException if the specified file-pointer
     *         doesn't actually point to a real tuple.
     */
    @Override
    public Tuple getTuple(FilePointer fptr)
        throws InvalidFilePointerException, IOException {

        DBPage dbPage;
        try {
            // This could throw EOFException if the page doesn't actually exist.
            dbPage = storageManager.loadDBPage(dbFile, fptr.getPageNo());
        }
        catch (EOFException eofe) {
            throw new InvalidFilePointerException("Specified page " +
                fptr.getPageNo() + " doesn't exist in file " +
                dbFile.getDataFile().getName(), eofe);
        }

        // The file-pointer points to the slot for the tuple, not the tuple itself.
        // So, we need to look up that slot's value to get to the tuple data.

        int slot;
        try {
            slot = DataPage.getSlotIndexFromOffset(dbPage, fptr.getOffset());
        }
        catch (IllegalArgumentException iae) {
            throw new InvalidFilePointerException(iae);
        }

        // Pull the tuple's offset from the specified slot, and make sure
        // there is actually a tuple there!

        int offset = DataPage.getSlotValue(dbPage, slot);
        if (offset == DataPage.EMPTY_SLOT) {
            throw new InvalidFilePointerException("Slot " + slot +
                " on page " + fptr.getPageNo() + " is empty.");
        }
        //unpins the page once the tuple location has been identified
        dbPage.unpin();

        return new HeapFilePageTuple(schema, dbPage, slot, offset);
    }


    /**
     * Returns the tuple that follows the specified tuple,
     * or <tt>null</tt> if there are no more tuples in the file.
     **/
    @Override
    public Tuple getNextTuple(Tuple tup) throws IOException {

        /* Procedure:
         *   1)  Get slot index of current tuple.
         *   2)  If there are more slots in the current page, find the next
         *       non-empty slot.
         *   3)  If we get to the end of this page, go to the next page
         *       and try again.
         *   4)  If we get to the end of the file, we return null.
         */

        if (!(tup instanceof HeapFilePageTuple)) {
            throw new IllegalArgumentException(
                "Tuple must be of type HeapFilePageTuple; got " + tup.getClass());
        }
        HeapFilePageTuple ptup = (HeapFilePageTuple) tup;

        DBPage dbPage = ptup.getDBPage();
        // pins the page the tuple is one (as the getDBPage does not pin it)
        dbPage.pin();
        DBFile dbFile = dbPage.getDBFile();

        int nextSlot = ptup.getSlot() + 1;
        while (true) {
            int numSlots = DataPage.getNumSlots(dbPage);

            while (nextSlot < numSlots) {
                int nextOffset = DataPage.getSlotValue(dbPage, nextSlot);
                if (nextOffset != DataPage.EMPTY_SLOT) {
                    //when the next tuple's location has been found, unpin the page
                    dbPage.unpin();
                    return new HeapFilePageTuple(schema, dbPage, nextSlot,
                                                 nextOffset);
                }

                nextSlot++;
            }

            // If we got here then we reached the end of this page with no
            // tuples.  Go on to the next data-page, and start with the first
            // tuple in that page.

            try {
                DBPage nextDBPage =
                    storageManager.loadDBPage(dbFile, dbPage.getPageNo() + 1);
                dbPage.unpin();
                dbPage = nextDBPage;

                nextSlot = 0;
            }
            catch (EOFException e) {
                // Hit the end of the file with no more tuples.  We are done
                // scanning.
                //unpins the current page if it exists.  If the loop went
                //past the end of the pages, does nothing
                if(dbPage!=null){
                    dbPage.unpin();
                }
                return null;
            }
        }
        // It's pretty gross to have no return statement here, but there's
        // no way to reach this point.
    }


    /**
     * Adds the specified tuple into the table file.  A new
     * <tt>HeapFilePageTuple</tt> object corresponding to the tuple is returned.
     *
     * @review (donnie) This could be made a little more space-efficient.
     *         Right now when computing the required space, we assume that we
     *         will <em>always</em> need a new slot entry, whereas the page may
     *         contain empty slots.  (Note that we don't always create a new
     *         slot when adding a tuple; we will reuse an empty slot.  This
     *         inefficiency is simply in estimating the size required for the
     *         new tuple.)
     */
    @Override
    public Tuple addTuple(Tuple tup) throws IOException {

        /*
         * Check to see whether any constraints are violated by
         * adding this tuple
         *
         * Find out how large the new tuple will be, so we can find a page to
         * store it.
         *
         * Find a page with space for the new tuple.
         *
         * Generate the data necessary for storing the tuple into the file.
         */

        int tupSize = PageTuple.getTupleStorageSize(schema, tup);
        logger.debug("Adding new tuple of size " + tupSize + " bytes.");

        // Sanity check:  Make sure that the tuple would actually fit in a page
        // in the first place!
        // The "+ 2" is for the case where we need a new slot entry as well.
        if (tupSize + 2 > dbFile.getPageSize()) {
            throw new IOException("Tuple size " + tupSize +
                " is larger than page size " + dbFile.getPageSize() + ".");
        }


        // Starting at the header page, we follow the linked lists of open blocks
        // Checking if each block has enough space for our tuple
        DBPage headerPage = storageManager.loadDBPage(dbFile, 0);
        short pageNo = DataPage.getNextPage(headerPage);

        DBPage dbPage = null;
        while (true) {
            // if the page number equals 0, we've looped through our entire linked list
            // without finding a block that can fit our tuple
            // if this is the case we break out of the loop and create a new block
            if(pageNo == 0) break;

            dbPage = storageManager.loadDBPage(dbFile, pageNo);
            int freeSpace = DataPage.getFreeSpaceInPage(dbPage);
            logger.trace(String.format("Page %d has %d bytes of free space.",
                         pageNo, freeSpace));

            // If this page has enough free space to add a new tuple, break
            // out of the loop.  (The "+ 2" is for the new slot entry we will
            // also need.)
            if (freeSpace >= tupSize + 2) {
                logger.debug("Found space for new tuple in page " + pageNo + ".");
                break;
            }

            // If we reached this point then the page doesn't have enough
            // space, so unpin and go on to the next data page.
            pageNo = DataPage.getNextPage(dbPage);
        }

        // we've looped around to the start of the list, so we need to create a new page
        if(pageNo == 0) {
            pageNo = (short) dbFile.getNumPages();
            logger.debug("Creating new page " + pageNo + " to store new tuple.");
            if(dbPage != null){
                dbPage.unpin();
            }
            dbPage = storageManager.loadDBPage(dbFile, pageNo, true);
            DataPage.initNewPage(dbPage);

            // puts the new page at the beginning of the linked list
            short headerNextIndex = DataPage.getNextPage(headerPage);
            DBPage oldNextPage = storageManager.loadDBPage(dbFile, headerNextIndex
            );
            DataPage.setNextPage(dbPage, headerNextIndex);
            DataPage.setLastPage(dbPage, (short) 0);
            DataPage.setNextPage(headerPage, pageNo);
            DataPage.setLastPage(oldNextPage, pageNo);
            // after references are fixed, unpin surrounding pages
            oldNextPage.unpin();
            headerPage.unpin();
        }

        int slot = DataPage.allocNewTuple(dbPage, tupSize);
        int tupOffset = DataPage.getSlotValue(dbPage, slot);

        logger.debug(String.format(
            "New tuple will reside on page %d, slot %d.", pageNo, slot));

        HeapFilePageTuple pageTup =
            HeapFilePageTuple.storeNewTuple(schema, dbPage, slot, tupOffset, tup);

        DataPage.sanityCheck(dbPage);

        // Check if adding the tuple causes the page to not have enough free space
        // If the page is too full, remove it from the linked list and update
        // the surrounding pointers accordingly
        if(DataPage.getFreeSpaceInPage(dbPage) < tupSize) {
            short nextIndex = DataPage.getNextPage(dbPage);
            short lastIndex = DataPage.getLastPage(dbPage);
            DBPage prevPage = storageManager.loadDBPage(dbFile, (int) lastIndex);
            DBPage nextPage = storageManager.loadDBPage(dbFile, (int) nextIndex);

            DataPage.setNextPage(prevPage, nextIndex);
            DataPage.setLastPage(nextPage, lastIndex);

            DataPage.setLastPage(dbPage, (short) -1);
            DataPage.setNextPage(dbPage, (short) -1);
            prevPage.unpin();
            nextPage.unpin();
        }
        pageTup.unpin();
        dbPage.unpin();
        return pageTup;
    }


    // Inherit interface-method documentation.
    /**
     * @review (donnie) This method will fail if a tuple is modified in a way
     *         that requires more space than is currently available in the data
     *         page.  One solution would be to move the tuple to a different
     *         page and then perform the update, but that would cause all kinds
     *         of additional issues.  So, if the page runs out of data, oh well.
     */
    @Override
    public void updateTuple(Tuple tup, Map<String, Object> newValues)
        throws IOException {

        int tupSize = PageTuple.getTupleStorageSize(schema, tup);
        if (!(tup instanceof HeapFilePageTuple)) {
            throw new IllegalArgumentException(
                "Tuple must be of type HeapFilePageTuple; got " + tup.getClass());
        }
        HeapFilePageTuple ptup = (HeapFilePageTuple) tup;

        for (Map.Entry<String, Object> entry : newValues.entrySet()) {
            String colName = entry.getKey();
            Object value = entry.getValue();

            int colIndex = schema.getColumnIndex(colName);
            ptup.setColumnValue(colIndex, value);
        }

        DBPage dbPage = ptup.getDBPage();
        DataPage.sanityCheck(dbPage);

        short currentNext = DataPage.getNextPage(dbPage);
        short currentLast = DataPage.getLastPage(dbPage);

        // Check if updating the tuple causes the page to not have enough free space
        // If the page is too full, remove it from the linked list and update
        // the surrounding pointers accordingly

        if(DataPage.getFreeSpaceInPage(dbPage) < tupSize
                && currentNext != -1 && currentLast != -1) {
            short nextIndex = DataPage.getNextPage(dbPage);
            short lastIndex = DataPage.getLastPage(dbPage);
            DBPage prevPage = storageManager.loadDBPage(dbFile, (int) lastIndex);
            DBPage nextPage = storageManager.loadDBPage(dbFile, (int) nextIndex);

            DataPage.setNextPage(prevPage, nextIndex);
            DataPage.setLastPage(nextPage, lastIndex);

            DataPage.setLastPage(dbPage, (short) -1);
            DataPage.setNextPage(dbPage, (short) -1);
            prevPage.unpin();
            nextPage.unpin();
        }

        if(DataPage.getFreeSpaceInPage(dbPage) > tupSize
            && currentNext == -1 && currentLast == -1) {
            // we add the page to the beginning of the list, updating the surrounding
            // pointers according
            DBPage headerPage = storageManager.loadDBPage(dbFile, 0);
            short headerNext = DataPage.getNextPage(headerPage);
            DBPage oldNextPage = storageManager.loadDBPage(dbFile, headerNext);
            DataPage.setNextPage(dbPage, headerNext);
            DataPage.setLastPage(dbPage, (short) 0);

            DataPage.setLastPage(oldNextPage, (short) dbPage.getPageNo());
            DataPage.setNextPage(headerPage, (short) dbPage.getPageNo());
            headerPage.unpin();
            oldNextPage.unpin();
        }
        dbPage.unpin();
    }


    // Inherit interface-method documentation.
    @Override
    public void deleteTuple(Tuple tup) throws IOException {

        if (!(tup instanceof HeapFilePageTuple)) {
            throw new IllegalArgumentException(
                "Tuple must be of type HeapFilePageTuple; got " + tup.getClass());
        }
        HeapFilePageTuple ptup = (HeapFilePageTuple) tup;

        DBPage dbPage = ptup.getDBPage();
        // calling ptup.getDBPage doesn't pin the page, so must be done now
        dbPage.pin();
        DataPage.deleteTuple(dbPage, ptup.getSlot());

        DataPage.sanityCheck(dbPage);

        // if the page we just deleted our tuple from was not in the list
        // it probably has enough space to be in the list again
        // so we put it back into the list
        // pages not in the list have pointers (-1, -1)
        // so that's what we check for
        short currentNext = DataPage.getNextPage(dbPage);
        short currentLast = DataPage.getLastPage(dbPage);

        if(currentNext == -1 && currentLast == -1) {
            // we add the page to the beginning of the list, updating the surrounding
            // pointers according
            DBPage headerPage = storageManager.loadDBPage(dbFile, 0);
            short headerNext = DataPage.getNextPage(headerPage);
            DBPage oldNextPage = storageManager.loadDBPage(dbFile, headerNext);
            DataPage.setNextPage(dbPage, headerNext);
            DataPage.setLastPage(dbPage, (short) 0);

            DataPage.setLastPage(oldNextPage, (short) dbPage.getPageNo());
            DataPage.setNextPage(headerPage, (short) dbPage.getPageNo());
            headerPage.unpin();
            oldNextPage.unpin();
        }
        dbPage.unpin();
    }

    @Override
    public void analyze() throws IOException {
        // TODO!
        HeaderPage hp = new HeaderPage();
        DataPage d = new DataPage();
        DBPage hPage = storageManager.loadDBPage(dbFile, 0);
        DBPage curPage;
        //int numCols = stats.getAllColumnStats().size();
        int st = hp.OFFSET_SCHEMA_START;
        int numCols = hPage.readUnsignedByte(st);
        ArrayList<ColumnType> types = new ArrayList<ColumnType>(numCols);
        ArrayList<ColumnStatsCollector> ar = new ArrayList<ColumnStatsCollector>(numCols);
        for(int i = 0; i < numCols; i++) {
            types.add(schema.getColumnInfo(i).getType());
            ar.add(new ColumnStatsCollector(types.get(i).getBaseType()));
        }
        int numTuples = 0;
        int totalSize = 0;
        float avgSize = 0;
        int numPages = dbFile.getNumPages() - 1;
        int pageNum = 1;
        int curPos = 0;
        int endPos = 1;
        while(pageNum < numPages) {
            curPage = storageManager.loadDBPage(dbFile, pageNum);
            curPos = d.getTupleDataStart(curPage);
            endPos = d.getTupleDataEnd(curPage);
            int curCol = 0;
            while(curPos < endPos) {
                int tupLen = d.getTupleLength(curPage, curPos);
                numTuples++;
                totalSize += tupLen;
                while(curCol < numCols) {
                    ar.get(curCol).addValue(curPage.readObject(curPos+curCol,
                            types.get(curCol)));
                    curCol++;
                }
                curPos = d.getNextPage(curPage);
                curCol = 0;
                //getNextPage gives index of next tuple
            }
            pageNum++;
        }
        avgSize = (float) totalSize / numTuples;
        ArrayList<ColumnStats> cStats = new ArrayList<ColumnStats>(numCols);
        for(ColumnStatsCollector c : ar)
            cStats.add(c.getColumnStats());
        TableStats tStats = new TableStats(numPages, numTuples, avgSize, cStats);
        stats = tStats;
        heapFileManager.saveMetadata(this);
    }

    @Override
    public List<String> verify() throws IOException {
        // TODO!
        throw new UnsupportedOperationException("Not yet implemented!");
    }

    @Override
    public void optimize() throws IOException {
        // TODO!
        throw new UnsupportedOperationException("Not yet implemented!");
    }
}
