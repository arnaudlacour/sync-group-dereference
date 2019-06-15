package com.pingidentity.sync.pipe;

import com.unboundid.directory.sdk.common.api.ServerThread;
import com.unboundid.directory.sdk.common.types.LogSeverity;
import com.unboundid.directory.sdk.sync.api.SyncPipePlugin;
import com.unboundid.directory.sdk.sync.config.SyncPipePluginConfig;
import com.unboundid.directory.sdk.sync.types.PreStepResult;
import com.unboundid.directory.sdk.sync.types.SyncOperation;
import com.unboundid.directory.sdk.sync.types.SyncServerContext;
import com.unboundid.ldap.sdk.*;
import com.unboundid.util.FixedRateBarrier;
import com.unboundid.util.args.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class provides a sync pipe plugin that may be used to parse the contents of a group entry
 * to trigger the synchronization on its members rather than just the group entry itself
 * <p>
 * A key use case is that most environments rely on group memeberships to trigger the provisioning of user
 * entries at a target system. When adding a member to a group, a change event is detected on the group that was
 * modified but the user entry is usually not altered. As a result, the user entry provisioning cannot be triggered
 * at that point in time with the current facilities provided in Sync.
 * <p>
 * This plugin will traverse the group, fetch the members entries and either enqueue them for processing in a
 * special kind of sync source or "touch" them by issuing a modification back to the member entry at the source
 */
public class GroupDereference extends SyncPipePlugin
{
    public static final String ATTACHMENT_ID = "connection";
    public static final String STRATEGY_TOUCH = "touch-member-entry";
    public static final String STRATEGY_ENQUEUE_ENTRY = "enqueue-member-entry";
    public static final String STRATEGY_ENQUEUE_DN = "enqueue-member-dn";
    public static final String ARG_NAME_STRATEGY = "strategy";
    public static final String ARG_NAME_DEREF_RATE = "max-rate-per-second";
    public static final String ARG_NAME_DEREF_MAX_GROUP_SIZE = "max-group-size";
    public static final String ARG_NAME_DEREF_ATTRIBUTE = "attribute";
    public static final String ARG_NAME_DEREF_THREADS = "number-of-threads";
    public static final String ARG_NAME_ABORT_SYNC = "skip-group-sync";
    public static final String ARG_NAME_DEREF_PARSE_MODE = "parse-mode";
    public static final String ARG_NAME_VERBOSE = "verbose";
    public static final String PARSE_MODE_WHOLE_GROUP = "parse-whole-group";
    public static final String PARSE_MODE_CHANGELOG = "parse-group-change";
    
    Queue<DereferenceOperation> queue = null;
    private SyncServerContext context;
    private Integer maxGroupSize;
    private List<DereferenceThread> threads = new ArrayList<>();
    private List<String> memberAttributes;
    private String strategy;
    private String parseMode;
    private boolean abortSync;

    AtomicLong maxQueueSize = new AtomicLong(0L);
    AtomicLong queueAddFailures = new AtomicLong(0L);
    AtomicLong queueAddAttempts = new AtomicLong(0L);
    
    
    /**
     * Performs the necessary processing to define the arguments that this extension needs
     *
     * @param parser the argument parser
     * @throws ArgumentException if any argument definition is incorrect
     */
    public void defineConfigArguments(ArgumentParser parser)
            throws ArgumentException
    {
        IntegerArgument rateArg = new IntegerArgument(
                null,
                ARG_NAME_DEREF_RATE,
                false,
                1,
                "{rate}",
                "Maximum rate per second to execute "
                        + "dereference operations. This will prevent surges of requests when processing large groups.");
        parser.addArgument(rateArg);
        
        IntegerArgument groupSizeArg = new IntegerArgument(
                null,
                ARG_NAME_DEREF_MAX_GROUP_SIZE,
                false,
                1,
                "{size}",
                "Maximum group size to process. This parameters allows to limit the impact on sync processing by " +
                        "filtering large groups out.");
        parser.addArgument(groupSizeArg);
        
        String[] defaultAttributes = new String[]{
                "member", "uniqueMember"};
        StringArgument attributeArg = new StringArgument(null, ARG_NAME_DEREF_ATTRIBUTE, false, 0,
                "{attribute}", "One or more attributes to dereference.",
                Arrays.asList(defaultAttributes));
        parser.addArgument(attributeArg);
        
        IntegerArgument threadsArg = new IntegerArgument(null, ARG_NAME_DEREF_THREADS, false, 1, "{threads}",
                "Number of threads to use to process dereference operations", 1);
        parser.addArgument(threadsArg);
        
        BooleanArgument abortArg = new BooleanArgument(
                null,
                ARG_NAME_ABORT_SYNC,
                "Whether to abort the synchronization for the group object being processed or continue processing. "
                        + "This might be useful when only users of a group need to be synchronized to an endpoint but" +
                        " not he group itself.");
        parser.addArgument(abortArg);
        
        Set<String> allowedParsing = new HashSet<>();
        allowedParsing.add(PARSE_MODE_WHOLE_GROUP);
        allowedParsing.add(PARSE_MODE_CHANGELOG);
        StringArgument parseModeArg = new StringArgument(null, ARG_NAME_DEREF_PARSE_MODE, false, 1,
                "{parseMode}", "Which group parsing mode to use.", allowedParsing,
                PARSE_MODE_CHANGELOG);
        parser.addArgument(parseModeArg);
        
        Set<String> allowedDereferencing = new HashSet<>();
        allowedDereferencing.add(STRATEGY_TOUCH);
        allowedDereferencing.add(STRATEGY_ENQUEUE_ENTRY);
        allowedDereferencing.add(STRATEGY_ENQUEUE_DN);
        StringArgument dereferenceModeArg = new StringArgument(
                null,
                ARG_NAME_STRATEGY,
                false,
                1,
                "{dereferenceMode}",
                "Which strategy to use in order to synchronize the membership change to the destination",
                allowedDereferencing, STRATEGY_ENQUEUE_DN);
        parser.addArgument(dereferenceModeArg);
        
        BooleanArgument verboseArg = new BooleanArgument(null, ARG_NAME_VERBOSE, "Verbose output");
        parser.addArgument(verboseArg);
    }
    
    @Override
    public ResultCode applyConfiguration(SyncPipePluginConfig config, ArgumentParser parser, List<String>
            adminActionsRequired, List<String> messages)
    {
        FixedRateBarrier rateBarrier = null;
        Integer value = parser.getIntegerArgument(ARG_NAME_DEREF_RATE).getValue();
        if (value != null)
        {
            rateBarrier = new FixedRateBarrier(1000L, value);
        } else {
            rateBarrier = new FixedRateBarrier(1000L, 1000);
        }
        
        abortSync = parser.getBooleanArgument(ARG_NAME_ABORT_SYNC).isPresent();
        strategy = parser.getStringArgument(ARG_NAME_STRATEGY).getValue();
        parseMode = parser.getStringArgument(ARG_NAME_DEREF_PARSE_MODE).getValue();
        maxGroupSize = parser.getIntegerArgument(ARG_NAME_DEREF_MAX_GROUP_SIZE).getValue();
        
        memberAttributes = parser.getStringArgument(ARG_NAME_DEREF_ATTRIBUTE).getValues();
        Integer numberOfThreads = parser.getIntegerArgument(ARG_NAME_DEREF_THREADS).getValue();
        if (threads == null || threads.size() != numberOfThreads)
        {
            List<DereferenceThread> newThreads = new ArrayList<>();
            for (int i = 0; i < parser.getIntegerArgument(ARG_NAME_DEREF_THREADS).getValue(); i++)
            {
                DereferenceThread thread = new DereferenceThread(queue, rateBarrier);
                newThreads.add(thread);
                Thread t = config.getServerContext().createThread((ServerThread) thread, "Deref thr-"
                        + i + " for " + config.getConfigObjectName());
                t.start();
            }
            
            if (threads != null ){
                for (DereferenceThread thread: threads)
                {
                    thread.halt();
                }
            }
            threads=newThreads;
        }
        return ResultCode.SUCCESS;
    }
    
    /**
     * Performs the necessary processing to initialize the instance of the extension
     *
     * @param serverContext the server context
     * @param config        the cofiguration object of the instance of the extension
     * @param parser        the argument parser
     * @throws LDAPException never
     */
    public void initializeSyncPipePlugin(SyncServerContext serverContext,
                                         SyncPipePluginConfig config, ArgumentParser parser) throws LDAPException
    {
        context = serverContext;
        queue = DereferenceOperationQueue.getInstance();
        List<String> adminActionsRequired = new ArrayList<>(3);
        List<String> messages = new ArrayList<>(3);
        applyConfiguration(config,parser,adminActionsRequired,messages);
    }
    
    /**
     * Performs the necessary processing to shut down the extension gracefully
     */
    public void finalizeSyncPipePlugin()
    {
        if ( threads != null )
        {
            for (DereferenceThread thread : threads)
            {
                thread.halt();
            }
        }
    }
    
    /**
     * Performs the necessary processing to compute the extension name
     *
     * @return the extension name
     */
    @Override
    public String getExtensionName()
    {
        return "Group Dereference";
    }
    
    /**
     * Performs the necessary processing to provide more detailed description about the extension
     *
     * @return an array of descriptive strings
     */
    @Override
    public String[] getExtensionDescription()
    {
        return new String[]{"A sync pipe plugin to inspect group objects for members change."
                + " This type of extension must be used in conjunction with the StashConnection LDAPS"};
    }
    
    @Override
    public void toString(StringBuilder buffer)
    {
    }
    
    /**
     * Performs the necessary processing to parse group entries and process its members
     *
     * @param sourceEntry                the source entry where a change was detected and being process by the sync pipe
     * @param equivalentDestinationEntry the entry equivalent at the destination
     * @param operation                  the current sync operation
     * @return the result of processing the group entry
     */
    public PreStepResult preMapping(com.unboundid.ldap.sdk.Entry sourceEntry,
                                    com.unboundid.ldap.sdk.Entry equivalentDestinationEntry,
                                    SyncOperation operation)
    {
        
        /**
         retrieve a connection back to the source where the change was detected
         The connection must have been stashed
         See {@link StashConnection} for how to do that
         */
        LDAPInterface connection = (LDAPInterface) operation
                .getAttachment(ATTACHMENT_ID);
        if (connection == null
                && STRATEGY_TOUCH.equalsIgnoreCase(strategy))
        {
            context
                    .logMessage(
                            LogSeverity.DEBUG,
                            "No connection for operation "
                                    + operation.getIdentifiableInfo()
                                    + " but connection is necessary to be able to \"touch\" the source entry. "
                                    + "You may correct this issue by creating a StashConnection LDAPSyncSource Plugin" +
                                    " and adding it to your LDAP Sync source.");
            return getResult();
        }
        
        ChangeLogEntry cle = operation.getChangeLogEntry();
        if (cle != null)
        {
            // uh oh, prolly should spit stuff out to stdout
            System.out.println(cle.toLDIFString());
        }
        
        if (cle == null
                && PARSE_MODE_CHANGELOG.equalsIgnoreCase(parseMode))
        {
            context
                    .logMessage(
                            LogSeverity.DEBUG,
                            "The changelog entry could not be retrieved but is required for the " +
                                    PARSE_MODE_CHANGELOG +
                                    "dereference mode.");
            return getResult();
        }
        
        if (PARSE_MODE_WHOLE_GROUP.equalsIgnoreCase(parseMode))
        {
            for (String attrName : memberAttributes)
            {
                Attribute attribute = sourceEntry.getAttribute(attrName);
                if (attribute != null)
                {
                    String[] values = attribute.getValues();
                    if (values != null)
                    {
                        if (maxGroupSize == null
                                || (maxGroupSize != null && values.length < maxGroupSize))
                        {
                            for (String referenceDN : attribute.getValues())
                            {
                                packageOperation(referenceDN, connection);
                            }
                        }
                    }
                }
            }
        } else
        {
            switch (operation.getChangeLogEntry().getChangeType())
            {
                case ADD:
                    // new group
                    for (Attribute attribute : operation.getChangeLogEntry().getAddAttributes())
                    {
                        if (memberAttributes.stream().anyMatch(attribute.getBaseName()::equalsIgnoreCase))
                        {
                            for (String referenceDN : attribute.getValues())
                            {
                                packageOperation(referenceDN, connection);
                            }
                        }
                    }
                    break;
                case DELETE:
                    // group was deleted
                    for (Attribute attribute : operation.getChangeLogEntry().getDeletedEntryAttributes())
                    {
                        if (memberAttributes.stream().anyMatch(attribute.getBaseName()::equalsIgnoreCase))
                        {
                            for (String referenceDN : attribute.getValues())
                            {
                                packageOperation(referenceDN, connection);
                            }
                        }
                    }
                    break;
                case MODIFY:
                    // check every modification in the changelog entry
                    for (Modification modification : operation.getChangeLogEntry().getModifications())
                    {
                        // if it's on an attribute we are interested in ..
                        if (memberAttributes.stream().anyMatch(modification.getAttributeName()::equalsIgnoreCase))
                        {
                            // grab all the values and package them for update
                            for (String referenceDN : modification.getValues())
                            {
                                packageOperation(referenceDN, connection);
                            }
                        }
                    }
                    break;
                case MODIFY_DN:
                    // the group was moved, we don't need to process the group
                    break;
            }
        }
        return getResult();
    }
    
    /**
     * Convenience method to compute the correct {@code PreStepResult} given the configuration arguments provided
     *
     * @return the correct PreStepResult
     */
    private PreStepResult getResult()
    {
        return abortSync ? PreStepResult.ABORT_OPERATION : PreStepResult.CONTINUE;
    }
    
    /**
     * This method will package the change in a {@code DereferenceOperation} based on configuration arguments
     *
     * @param referenceDN a reference DN (must not be null)
     * @param connection  a connection (may be null)
     */
    private void packageOperation(String referenceDN, LDAPInterface connection)
    {
        DereferenceOperation derefOp = null;
        // Other ways to dereference may be added later
        // This will simply issue a modify operation on the
        // referenced entry so that it can then be picked up
        // by another sync pipe or class
        switch (strategy)
        {
            case STRATEGY_TOUCH:
                if (connection != null)
                {
                    derefOp = new TouchDereferenceOperation(context, connection, referenceDN);
                }
                break;
            
            case STRATEGY_ENQUEUE_ENTRY:
                try
                {
                    derefOp = new WholeEntryDereferenceOperation(context,
                            connection, referenceDN);
                } catch (Exception e)
                {
                    context.logMessage(LogSeverity.MILD_ERROR, e.getMessage());
                }
                break;
            
            case STRATEGY_ENQUEUE_DN:
                derefOp = new DNDereferenceOperation(referenceDN);
                break;
        }
        
        if (derefOp != null)
        {
            enqeue(derefOp);
        }
    }
    
    private void enqeue(DereferenceOperation op)
    {
        queueAddAttempts.incrementAndGet();
        boolean addSuccessful = queue.add(op);
        if ( !addSuccessful ) {
            queueAddFailures.incrementAndGet();
        }
        updateQueueMax();
    }

    private synchronized void updateQueueMax()
    {
        Long size = new Long(queue.size());
        if ( size > maxQueueSize.get() )
        {
            maxQueueSize.set(size);
        }
    }
}