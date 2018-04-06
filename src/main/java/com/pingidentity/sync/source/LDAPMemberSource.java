package com.pingidentity.sync.source;

import com.unboundid.directory.sdk.sync.api.SyncSource;
import com.unboundid.directory.sdk.sync.config.SyncSourceConfig;
import com.unboundid.directory.sdk.sync.types.*;
import com.unboundid.ldap.sdk.Entry;
import com.unboundid.ldap.sdk.LDAPConnectionPool;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.util.args.ArgumentException;
import com.unboundid.util.args.ArgumentParser;
import com.unboundid.util.args.IntegerArgument;
import com.unboundid.util.args.StringArgument;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class provides a source that can be used to trigger a lookup in a server using an external server based
 * on some membership changes in a group entry. The queue is fed by the {@code GroupDereference} class
 *
 */
public class LDAPMemberSource extends SyncSource
{
    
    public static final String ARG_NAME_EXTERNAL_SERVER = "external-server";
    public static final String ARG_NAME_CONN_INIT = "pool-initial-connections";
    public static final String ARG_NAME_CONN_MAX = "pool-max-connections";
    public static final int ARG_CONN_INIT_DEFAULT = 1;
    public static final int ARG_CONN_MAX_DEFAULT = 20;
    private SyncServerContext serverContext;
    private LDAPConnectionPool ldapExternalServerConnectionPool = null;
    Queue<ChangeRecord> queue = MemberDNQueue.getInstance();
    
    
    /**
     * Performs the necessary processing to compute the extension name
     *
     * @return the extension name
     */
    @Override
    public String getExtensionName()
    {
        return "LDAP Member Source";
    }
    
    /**
     * Performs the necessary processing to compute a set of descriptive paragraphs about the extension
     *
     * @return the array of descriptive paragraphs
     */
    @Override
    public String[] getExtensionDescription()
    {
        return new String[]{"This source reaches back to an LDAP Extenal server"};
    }
    
    /**
     * Performs the necessary processing to compute the extension URL
     * @return the URL string
     */
    @Override
    public String getCurrentEndpointURL()
    {
        return "/ldap/member";
    }
    
    /**
     * Performs the necessary processing to set the startpoint
     * @param setStartpointOptions the startpoint options
     */
    @Override
    public void setStartpoint(SetStartpointOptions setStartpointOptions)
    {
    }
    
    /**
     * Performs the necessary processing to return the persisted start point
     * @return null
     */
    @Override
    public Serializable getStartpoint()
    {
        return null;
    }
    
    /**
     * Performs the necessary processing to define the arguments the extension instance requires to function
     * @param parser the argument parser
     * @throws ArgumentException if an argument definition was incorrect
     */
    @Override
    public void defineConfigArguments(ArgumentParser parser) throws ArgumentException
    {
        parser.addArgument(new StringArgument(null, ARG_NAME_EXTERNAL_SERVER,true,1,"{ext-server}","The name of the external server to use. This must match exactly the external server name in the configuration."));
        parser.addArgument(new IntegerArgument(null, ARG_NAME_CONN_INIT,false,1,"{num-conn}","The initial number of connections to keep in the pool", ARG_CONN_INIT_DEFAULT));
        parser.addArgument(new IntegerArgument(null, ARG_NAME_CONN_MAX,false,1,"{num-conn}","The maximum number of connections to keep in the pool", ARG_CONN_MAX_DEFAULT));
    }
    
    /**
     * Performs the necessary processing to initialize the instance of the extension
     * @param serverContext the server context
     * @param config the configuraton object for the instance of the extension
     * @param parser the argument parser
     */
    @Override
    public void initializeSyncSource(SyncServerContext serverContext, SyncSourceConfig config, ArgumentParser parser)
    {
        this.serverContext = serverContext;
        String externalServerName = parser.getStringArgument(ARG_NAME_EXTERNAL_SERVER).getValue();
        Integer connInit = parser.getIntegerArgument(ARG_NAME_CONN_INIT).getValue();
        Integer connMax = parser.getIntegerArgument(ARG_NAME_CONN_MAX).getValue();
        try
        {
             ldapExternalServerConnectionPool = serverContext.getLDAPExternalServerConnectionPool
                    (externalServerName, null, connInit, connMax, true);
        } catch (LDAPException e)
        {
            serverContext.debugCaught(e);
        }
    }
    
    /**
     * Performs the necessary processing to compute the next series of {@code ChangeRecord} for the engine to process
     * @param maxChanges the batch maximum size
     * @param numStillPending number of changes pending
     * @return the list of records to process
     */
    @Override
    public List<ChangeRecord> getNextBatchOfChanges(int maxChanges, AtomicLong numStillPending)
    {
        List<ChangeRecord> result = new ArrayList<>();
        int i = 0;
        while (i++ < maxChanges && !queue.isEmpty())
        {
            ChangeRecord record = queue.poll();
            if (record != null)
            {
                result.add(record);
            } else
            {
                break;
            }
        }
        return result;
    }
    
    /**
     * Performs the necessary processing to retrieve the entry from the external server
     * @param syncOperation the sync operation to use to identify the entry to fetch
     * @return the Entry retrieved from the source or null if the entry was not found
     * @throws EndpointException if an exception was ecountered in the process of fetching the entry
     */
    @Override
    public Entry fetchEntry(SyncOperation syncOperation) throws EndpointException
    {
        try
        {
            return ldapExternalServerConnectionPool.getEntry((String) syncOperation.getChangeRecord().getProperty("DN"));
        } catch (LDAPException e)
        {
            throw new EndpointException(e);
        }
    }
    
    /**
     * Performs the necessary processing to notify the external server that a change was processed
     * @param linkedList a list of operations to acknowledge with the source
     * @throws EndpointException if any exception was encountered in the process of acknowledging
     */
    @Override
    public void acknowledgeCompletedOps(LinkedList<SyncOperation> linkedList) throws EndpointException
    {
    }
}