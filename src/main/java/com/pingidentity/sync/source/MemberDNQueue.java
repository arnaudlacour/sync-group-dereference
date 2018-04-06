package com.pingidentity.sync.source;

import com.unboundid.directory.sdk.sync.types.ChangeRecord;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MemberDNQueue
{
    private static Queue<ChangeRecord> instance = null;
    
    private MemberDNQueue()
    {
    }
    
    public static Queue<ChangeRecord> getInstance()
    {
        if (instance == null)
        {
            instance = new ConcurrentLinkedQueue<ChangeRecord>();
        }
        return instance;
    }
}