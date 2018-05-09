package com.pingidentity.sync.source;

import com.unboundid.directory.sdk.sync.api.LDAPSyncSourcePlugin;
import com.unboundid.directory.sdk.sync.types.PostStepResult;
import com.unboundid.directory.sdk.sync.types.SyncOperation;
import com.unboundid.ldap.sdk.Entry;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.LDAPInterface;
import com.unboundid.ldap.sdk.Modification;

import java.util.concurrent.atomic.AtomicReference;

public class MockEntryPlugin extends LDAPSyncSourcePlugin
{
    @Override
    public String getExtensionName()
    {
        return "Mock Entry LDAP Sync Source Plugin";
    }
    
    @Override
    public String[] getExtensionDescription()
    {
        return new String[]{"This extension generates an entry from the changelog record"};
    }
    
    @Override
    public void toString(StringBuilder stringBuilder)
    {
    }
    
    @Override
    public PostStepResult postFetch(LDAPInterface sourceConnection, AtomicReference<Entry> fetchedEntryRef, SyncOperation operation) throws LDAPException
    {
        Entry entry = new Entry(operation.getChangeLogEntry().getTargetDN());
        for(Modification mod: operation.getChangeLogEntry().getModifications())
        {
            entry.addAttribute(mod.getAttribute());
        }
        fetchedEntryRef.set(entry);
        return PostStepResult.CONTINUE;
    }
}