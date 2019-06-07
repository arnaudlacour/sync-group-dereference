package com.pingidentity.sync.source;

import com.unboundid.directory.sdk.sync.api.LDAPSyncSourcePlugin;
import com.unboundid.directory.sdk.sync.types.PostStepResult;
import com.unboundid.directory.sdk.sync.types.SyncOperation;
import com.unboundid.directory.sdk.sync.types.SyncOperationType;
import com.unboundid.ldap.sdk.*;

import java.util.concurrent.atomic.AtomicReference;

/**
 * This class provides a basic mechanism to build an entry
 */
public class MockEntryPlugin extends LDAPSyncSourcePlugin
{
    /**
     * Retrieves a human-readable name for this extension
     * @return the extension name
     */
    @Override
    public String getExtensionName()
    {
        return "Mock Entry LDAP Sync Source Plugin";
    }
    
    /**
     * Retrieves a human-readable description for this extension
     * @return a list of descriptive paragraphs
     */
    @Override
    public String[] getExtensionDescription()
    {
        return new String[]{"This extension generates an entry from the changelog record"};
    }
    
    /**
     * Appends a string representation of this LDAP sync source plugin to the provided buffer.
     * @param stringBuilder
     */
    @Override
    public void toString(StringBuilder stringBuilder)
    {
    }
    
    /**
     * This method is called after fetching a source entry and performs the necessary processing to build
     * an entry from the entry retrieved in the changelog
     * @param sourceConnection A connection to the source server.
     * @param fetchedEntryRef A reference to the entry that was fetched.
     * @param operation The synchronization operation for this change.
     * @return CONTINUE ... always
     */
    @Override
    public PostStepResult postFetch(LDAPInterface sourceConnection, AtomicReference<Entry> fetchedEntryRef, SyncOperation operation)
    {
        if ( SyncOperationType.RESYNC == operation.getType() )
        {
            return PostStepResult.CONTINUE;
        }
        
        Entry entry = new Entry(operation.getChangeLogEntry().getTargetDN());
        
        if ( SyncOperationType.CREATE  == operation.getType() )
        {
            for ( Attribute attribute: operation.getChangeLogEntry().getAddAttributes() )
            {
                entry.addAttribute(attribute);
            }
        }
        
        if ( SyncOperationType.MODIFY == operation.getType() )
        {
            for (Modification mod : operation.getChangeLogEntry().getModifications())
            {
                if ( ModificationType.DELETE != mod.getModificationType() )
                {
                    entry.addAttribute(mod.getAttribute());
                }
            }
            fetchedEntryRef.set(entry);
        }
        
        return PostStepResult.CONTINUE;
    }
}