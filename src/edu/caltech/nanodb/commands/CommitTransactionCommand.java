package edu.caltech.nanodb.commands;


import edu.caltech.nanodb.storage.StorageManager;


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

        throw new ExecutionException("Not yet implemented!");
    }
}
