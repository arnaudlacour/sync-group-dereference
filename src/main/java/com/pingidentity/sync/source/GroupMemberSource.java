package com.pingidentity.sync.source;

import com.unboundid.directory.sdk.sync.api.SyncSource;
import com.unboundid.directory.sdk.sync.types.ChangeRecord;
import com.unboundid.directory.sdk.sync.types.EndpointException;
import com.unboundid.directory.sdk.sync.types.SetStartpointOptions;
import com.unboundid.directory.sdk.sync.types.SyncOperation;
import com.unboundid.ldap.sdk.Entry;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class will feed off of a queue provisioned by a sync pipe plugin parsing groups
 */
public class GroupMemberSource extends SyncSource
{
    Queue<ChangeRecord> queue = MemberEntryQueue.getInstance();
    
    @Override
    public String getExtensionName()
    {
        return "GroupMemberSource";
    }
    
    @Override
    public String[] getExtensionDescription()
    {
        return new String[]{"This sync source is fed by the GroupDereference Sync Pipe Plugin when using the " +
                "EnqueueGroupMember dereferencing strategy."};
    }
    
    @Override
    public String getCurrentEndpointURL()
    {
        return "/groupMembers";
    }
    
    @Override
    public void setStartpoint(SetStartpointOptions options)
    {
    }
    
    @Override
    public Serializable getStartpoint()
    {
        return null;
    }
    
    @Override
    public List<ChangeRecord> getNextBatchOfChanges(int maxChanges,
                                                    AtomicLong numStillPending) throws EndpointException
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
    
    @Override
    public Entry fetchEntry(SyncOperation operation)
    {
        return operation.getSourceEntry();
    }
    
    @Override
    public void acknowledgeCompletedOps(LinkedList<SyncOperation> completedOps)
    {
    }
}