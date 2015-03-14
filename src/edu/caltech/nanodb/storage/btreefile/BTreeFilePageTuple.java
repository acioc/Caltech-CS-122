package edu.caltech.nanodb.storage.btreefile;


import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.PageTuple;


/**
 * This class uses the <tt>PageTuple</tt> class functionality to access and
 * manipulate keys stored in a B<sup>+</sup> tree tuple file.  There is one
 * extension, which is to allow the tuple to remember its index within the
 * leaf page it is from; this makes it easy to move to the next tuple within
 * the page very easily.
 */
public class BTreeFilePageTuple extends PageTuple {

    private int tupleIndex;


    public BTreeFilePageTuple(Schema schema, DBPage dbPage, int pageOffset,
                              int tupleIndex) {
        super(dbPage, pageOffset, schema);

        if (tupleIndex < 0) {
            throw new IllegalArgumentException(
                "tupleIndex must be at least 0, got " + tupleIndex);
        }

        this.tupleIndex = tupleIndex;
    }


    public int getTupleIndex() {
        return tupleIndex;
    }


    @Override
    protected void insertTupleDataRange(int off, int len) {
        throw new UnsupportedOperationException(
            "B+ Tree index tuples don't support updating or resizing.");
    }


    @Override
    protected void deleteTupleDataRange(int off, int len) {
        throw new UnsupportedOperationException(
            "B+ Tree index tuples don't support updating or resizing.");
    }


    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();
        buf.append("BTPT[");

        boolean first = true;
        for (int i = 0; i < getColumnCount(); i++) {
            if (first)
                first = false;
            else
                buf.append(',');

            Object obj = getColumnValue(i);
            if (obj == null)
                buf.append("NULL");
            else
                buf.append(obj);
        }

        buf.append(']');

        return buf.toString();
    }
}
