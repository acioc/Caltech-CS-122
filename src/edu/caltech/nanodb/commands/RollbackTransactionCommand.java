package edu.caltech.nanodb.commands;


import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.transactions.TransactionException;


/**
 * This class represents a command that rolls back a transaction, such as
 * <tt>ROLLBACK</tt> or <tt>ROLLBACK WORK</tt>.
 */
public class RollbackTransactionCommand extends Command {
    public RollbackTransactionCommand() {
        super(Type.UTILITY);
    }


    @Override
    public void execute(StorageManager storageManager)
        throws ExecutionException {
        // Roll back the transaction.
        try {
            storageManager.getTransactionManager().rollbackTransaction();
        }
        catch (TransactionException e) {
            throw new ExecutionException(e);
        }
    }
}
