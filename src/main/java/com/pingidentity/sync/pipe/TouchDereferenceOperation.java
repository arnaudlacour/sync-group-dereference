package com.pingidentity.sync.pipe;

import com.unboundid.directory.sdk.common.types.LogSeverity;
import com.unboundid.directory.sdk.sync.types.SyncServerContext;
import com.unboundid.ldap.sdk.*;

import java.util.ArrayList;
import java.util.List;

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
    final static List<Modification> modifications = new ArrayList<Modification>(2){{
        add(new Modification(ModificationType.DELETE,
                "objectClass", "top"));
        add(new Modification(ModificationType.ADD, "objectClass",
                "top"));
    }};
    
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
        
        ModifyRequest modifyRequest = new ModifyRequest(dn, modifications);
        try
        {
            connection.modify(modifyRequest);
        } catch (LDAPException e)
        {
            context.logMessage(LogSeverity.MILD_ERROR, e.getMessage());
        }
    }
}