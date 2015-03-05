package edu.caltech.nanodb.server;


import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import edu.caltech.nanodb.qeval.PlannerFactory;
import edu.caltech.nanodb.server.properties.PropertyRegistry;
import org.apache.log4j.Logger;

import antlr.RecognitionException;
import antlr.TokenStreamException;

import edu.caltech.nanodb.commands.Command;
import edu.caltech.nanodb.commands.SelectCommand;
import edu.caltech.nanodb.sqlparse.NanoSqlLexer;
import edu.caltech.nanodb.sqlparse.NanoSqlParser;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This class provides the entry-point operations for managing the database
 * server, and executing commands against it.  While it is certainly possible
 * to implement these operations outside of this class, these implementations
 * are strongly recommended since they include all necessary steps.
 */
public class NanoDBServer {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(NanoDBServer.class);


    public static final boolean FLUSH_DATA_AFTER_CMD = false;


    private StorageManager storageManager;


    /**
     * This static method encapsulates all of the operations necessary for
     * cleanly starting the NanoDB server.
     *
     * @throws IOException if a fatal error occurs during startup.
     */
    public void startup() throws IOException {
        // Start up the database by doing the appropriate startup processing.

        logger.info("Initializing storage manager.");
        storageManager = new StorageManager();
        storageManager.initialize(null);  // Use default base directory.

        // Register properties that are in parts of NanoDB that don't get
        // specifically initialized.

        PropertyRegistry propReg = PropertyRegistry.getInstance();

        propReg.registerProperties(
            new PlannerFactory.PlannerFactoryPropertyHandler(),
            PlannerFactory.PROP_PLANNER_CLASS);
    }


    public StorageManager getStorageManager() {
        return storageManager;
    }


    public Command parseCommand(String command)
        throws RecognitionException, TokenStreamException {

        StringReader strReader = new StringReader(command);
        NanoSqlLexer lexer = new NanoSqlLexer(strReader);
        NanoSqlParser parser = new NanoSqlParser(lexer);

        return parser.command();
    }


    public List<Command> parseCommands(String commands)
        throws RecognitionException, TokenStreamException {

        StringReader strReader = new StringReader(commands);
        NanoSqlLexer lexer = new NanoSqlLexer(strReader);
        NanoSqlParser parser = new NanoSqlParser(lexer);

        // Parse the string into however many commands there are.  If there is
        // a parsing error, no commands will run.
        return parser.commands();
    }


    public CommandResult doCommand(String command, boolean includeTuples)
        throws RecognitionException, TokenStreamException {

        Command parsedCommand = parseCommand(command);
        return doCommand(parsedCommand, includeTuples);
    }


    public List<CommandResult> doCommands(String commands,
        boolean includeTuples) throws RecognitionException, TokenStreamException {

        ArrayList<CommandResult> results = new ArrayList<CommandResult>();

        // Parse the string into however many commands there are.  If there is
        // a parsing error, no commands will run.
        List<Command> parsedCommands = parseCommands(commands);

        // Try to run each command in order.  Stop if a command fails.
        for (Command cmd : parsedCommands) {
            CommandResult result = doCommand(cmd, includeTuples);
            results.add(result);
            if (result.failed())
                break;
        }

        return results;
    }


    public CommandResult doCommand(Command command, boolean includeTuples) {
        CommandResult result = new CommandResult();

        if (includeTuples && command instanceof SelectCommand)
            result.collectSelectResults((SelectCommand) command);

        EventDispatcher eventDispatch = EventDispatcher.getInstance();
        result.startExecution();
        try {
            // Execute the command, but fire before- and after-command handlers
            // when we execute it.

            eventDispatch.fireBeforeCommandExecuted(command);
            command.execute(storageManager);
            eventDispatch.fireAfterCommandExecuted(command);
        }
        catch (Exception e) {
            logger.error("Command threw an exception!", e);
            result.recordFailure(e);
        }
        result.endExecution();

        // Post-command cleanup:
        storageManager.getBufferManager().unpinAllSessionPages();

        // TODO:  Remove this to improve performance.
        if (FLUSH_DATA_AFTER_CMD) {
            try {
                storageManager.flushAllData();
            } catch (IOException e) {
                logger.error("Post-command flush of all data threw an exception!",
                    e);
            }
        }

        return result;
    }


    /**
     * This method encapsulates all of the operations necessary for cleanly
     * shutting down the NanoDB server.
     *
     * @return {@code true} if the database server was shutdown cleanly, or
     *         {@code false} if an error occurred during shutdown.
     */
    public boolean shutdown() {
        boolean success = true;

        PropertyRegistry.getInstance().unregisterAllProperties();

        try {
            storageManager.shutdown();
        }
        catch (IOException e) {
            logger.error("Couldn't cleanly shut down the Storage Manager!", e);
            success = false;
        }

        return success;
    }
}
