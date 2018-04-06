package com.pingidentity.sync.pipe;

import com.pingidentity.sync.source.MemberEntryQueue;
import com.unboundid.directory.sdk.common.types.LogSeverity;
import com.unboundid.directory.sdk.sync.types.ChangeRecord;
import com.unboundid.directory.sdk.sync.types.SyncServerContext;
import com.unboundid.ldap.sdk.*;

import java.util.Queue;

/**
 * This class implements a {@code DereferenceOperation} that reaches back to
 * the source from which a change was detected to pull the full entry and
 * put it in the queue
 *
 * This type of dereference operation avoids having to connect the subsequent source to the initial source
 * from which the change was originally detected
 */
public class WholeEntryDereferenceOperation implements DereferenceOperation
{
    LDAPInterface connection;
    String dn;
    SyncServerContext context;
    Queue<ChangeRecord> queue = MemberEntryQueue.getInstance();
    
    /**
     * Performs the necessary processing to initialize the instance of the operation
     * @param ctx the server context
     * @param c the connection to the server from which the change was detected (cannot be null)
     * @param d the DN (cannot be null)
     * @throws Exception if there is a missing parameter
     */
    public WholeEntryDereferenceOperation(final SyncServerContext ctx,
                                          final LDAPInterface c, final String d) throws Exception
    {
        if (c == null)
            throw new Exception("Cannot enqueue DereferenceOperation with a null LDAP connection.");
        if (d == null)
            throw new Exception("Cannot enqueue DereferenceOperation with a null DN.");
        
        connection = c;
        dn = d;
        context = ctx;
    }
    
    
    /**
     * Performs the necessary processing to retrieve the entry and put it in the queue
     */
    @Override
    public void execute()
    {
        if (dn == null)
        {
            context.logMessage(LogSeverity.SEVERE_ERROR,
                    "DN not available. Aborting dereference operation");
            return;
        }
        if (connection == null)
        {
            context.logMessage(LogSeverity.INFO,
                    "Connection not available to execute this dereference operation for entry "
                            + dn);
            return;
        }
        if (queue == null)
        {
            context.logMessage(LogSeverity.INFO, "Queue not available to execute this dereference operation for entry" +
                    " " + dn);
            return;
        }
        
        try
        {
            SearchResultEntry sre = connection.getEntry(dn, "*", "+");
            ChangeRecord.Builder builder = new ChangeRecord.Builder(ChangeType.MODIFY, dn);
            builder.fullEntry(sre);
            builder.changeTime(System.currentTimeMillis());
            ChangeRecord changeRecord = builder.build();
            queue.add(changeRecord);
        } catch (LDAPException e)
        {
            context.logMessage(LogSeverity.FATAL_ERROR, e.getDiagnosticMessage());
        }
    }
}