package edu.caltech.nanodb.commands;


import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.transactions.TransactionException;


/**
 * This class represents a command that commits a transaction, such as
 * <tt>COMMIT</tt> or <tt>COMMIT WORK</tt>.
 */
public class CommitTransactionCommand extends Command {
    public CommitTransactionCommand() {
        super(Type.UTILITY);
    }


    @Override
    public void execute(StorageManager storageManager)
        throws ExecutionException {
        // Commit the transaction.
        try {
            storageManager.getTransactionManager().commitTransaction();
        }
        catch (TransactionException e) {
            throw new ExecutionException(e);
        }
    }
}
