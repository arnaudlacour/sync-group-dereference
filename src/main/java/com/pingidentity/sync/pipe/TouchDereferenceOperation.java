package com.pingidentity.sync.pipe;

import com.unboundid.directory.sdk.common.types.LogSeverity;
import com.unboundid.directory.sdk.sync.types.SyncServerContext;
import com.unboundid.ldap.sdk.*;

/**
 * This is a type of dereference operation that will "touch"
 * the dereferenced entry so that they may be handled by regular
 * sync pipes. This has many benefits, like leveraging proper
 * high availability mechanisms in stock sync sources.
 * The drawback is obviously that this may generate extraneous
 * write load on the source server
 */
public class TouchDereferenceOperation implements DereferenceOperation
{
    LDAPInterface connection;
    String dn;
    SyncServerContext context;
    
    public TouchDereferenceOperation(final SyncServerContext ctx,
                                     final LDAPInterface c, final String d)
    {
        dn = d;
        connection = c;
        context = ctx;
    }
    
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
        
        Modification mod0 = new Modification(ModificationType.DELETE,
                "objectClass", "top");
        Modification mod1 = new Modification(ModificationType.ADD, "objectClass",
                "top");
        ModifyRequest modifyRequest = new ModifyRequest(dn, mod0, mod1);
        try
        {
            connection.modify(modifyRequest);
        } catch (LDAPException e)
        {
            context.logMessage(LogSeverity.DEBUG, e.getDiagnosticMessage());
        }
    }
}