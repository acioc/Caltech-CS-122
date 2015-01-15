package edu.caltech.nanodb.commands;


import edu.caltech.nanodb.storage.StorageManager;


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
        throw new ExecutionException("Not yet implemented!");
    }
}
