package edu.caltech.nanodb.commands;


import edu.caltech.nanodb.storage.StorageManager;


/**
 * This Command class represents the <tt>EXIT</tt> or <tt>QUIT</tt> SQL
 * commands.  These commands aren't standard SQL of course, but are the
 * way that we tell the database to stop.
 */
public class ExitCommand extends Command {

    /** Construct an exit command. */
    public ExitCommand() {
        super(Command.Type.UTILITY);
    }

    /**
     * This method really doesn't do anything, and it isn't intended to be
     * called.
     */
    @Override
    public void execute(StorageManager storageManager)
        throws ExecutionException {
        // Do nothing.
    }
}
