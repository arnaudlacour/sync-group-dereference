package com.pingidentity.sync.source;

import com.unboundid.directory.sdk.sync.types.ChangeRecord;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * This type of queue will carry member DNs only (not the full entry)
 * it can be used with an external source to get a fresh copy of the member user entry
 */
public class MemberEntryQueue
{
    private static Queue<ChangeRecord> instance = null;
    
    private MemberEntryQueue()
    {
    }
    
    public static Queue<ChangeRecord> getInstance()
    {
        if (instance == null)
        {
            instance = new ConcurrentLinkedQueue<>();
        }
        return instance;
    }
}