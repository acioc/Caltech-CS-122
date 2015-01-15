package edu.caltech.nanodb.commands;


import edu.caltech.nanodb.storage.StorageManager;

/**
 * This command "crashes" the database by shutting it down immediately without
 * any proper cleanup or flushing of caches.
 */
public class CrashCommand extends Command {
    /**
     * Construct a new <tt>CRASH</tt> command.
     */
    public CrashCommand() {
        super(Command.Type.UTILITY);
    }


    @Override
    public void execute(StorageManager storageManager)
        throws ExecutionException {

        out.println("Goodbye, cruel world!  I'm taking your data with me!!!");

        // Using this API call avoids running shutdown hooks, finalizers, etc.
        // 22 is the exit status of the VM's process
        Runtime.getRuntime().halt(22);
    }


    /**
     * Prints a simple representation of the crash command.
     *
     * @return a string representing this crash command
     */
    @Override
    public String toString() {
        return "Crash";
    }
}
