package edu.caltech.test.nanodb.storage.writeahead;


import org.testng.annotations.*;

import edu.caltech.nanodb.storage.writeahead.LogSequenceNumber;
import edu.caltech.nanodb.storage.writeahead.WALManager;


/**
 * Make sure the {@link edu.caltech.nanodb.storage.writeahead.LogSequenceNumber}
 * class works properly.
 */
@Test
public class TestLogSequenceNumber {
    public void testBasicOperations() {
        LogSequenceNumber lsn = new LogSequenceNumber(123, 456);
        assert lsn.getLogFileNo() == 123;
        assert lsn.getFileOffset() == 456;

        lsn.setRecordSize(10);
        assert lsn.getRecordSize() == 10;
    }


    public void testInvalidCtorArgs() {
        try {
            new LogSequenceNumber(-1, 1234);
            assert false : "Invalid WAL file number didn't generate any exception.";
        }
        catch (IllegalArgumentException e) {
            // Success!
        }
        catch (Throwable t) {
            assert false : "Invalid WAL file number generated wrong exception";
        }

        try {
            new LogSequenceNumber(WALManager.MAX_WAL_FILE_NUMBER + 1, 1234);
            assert false : "Invalid WAL file number didn't generate any exception.";
        }
        catch (IllegalArgumentException e) {
            // Success!
        }
        catch (Throwable t) {
            assert false : "Invalid WAL file number generated wrong exception";
        }

        try {
            new LogSequenceNumber(1234, -1);
            assert false : "Invalid file offset didn't generate any exception.";
        }
        catch (IllegalArgumentException e) {
            // Success!
        }
        catch (Throwable t) {
            assert false : "Invalid file offset generated wrong exception";
        }
    }


    public void testCompareLSNs() {
        LogSequenceNumber lsn1 = new LogSequenceNumber(123, 456);
        LogSequenceNumber lsn2 = new LogSequenceNumber(123, 456);
        LogSequenceNumber lsn3 = new LogSequenceNumber(123, 789);
        LogSequenceNumber lsn4 = new LogSequenceNumber(125, 456);

        assert lsn1.compareTo(lsn2) == 0;
        assert lsn1.equals(lsn2);
        assert lsn1.hashCode() == lsn2.hashCode();

        assert lsn1.compareTo(lsn3) < 0;
        assert lsn3.compareTo(lsn1) > 0;
        assert !lsn1.equals(lsn3);
        assert !lsn3.equals(lsn1);

        assert lsn1.compareTo(lsn4) < 0;
        assert lsn4.compareTo(lsn1) > 0;
        assert !lsn1.equals(lsn4);
        assert !lsn4.equals(lsn1);

        assert lsn3.compareTo(lsn4) < 0;
        assert lsn4.compareTo(lsn3) > 0;
        assert !lsn3.equals(lsn4);
        assert !lsn4.equals(lsn3);
    }


    public void testCloneLSN() {
        LogSequenceNumber lsn1 = new LogSequenceNumber(123, 456);
        LogSequenceNumber lsn2 = lsn1.clone();

        assert lsn1.equals(lsn2);
        assert lsn1.hashCode() == lsn2.hashCode();
        assert lsn1.compareTo(lsn2) == 0;
    }
}
