package edu.caltech.nanodb.commands;


import edu.caltech.nanodb.storage.StorageManager;


/**
 * This class represents a command that starts a transaction, such as
 * <tt>BEGIN</tt>, <tt>BEGIN WORK</tt>, or <tt>START TRANSACTION</tt>.
 */
public class BeginTransactionCommand extends Command {
    public BeginTransactionCommand() {
        super(Type.UTILITY);
    }


    @Override
    public void execute(StorageManager storageManager)
        throws ExecutionException {

        throw new ExecutionException("Not yet implemented!");
    }
}
