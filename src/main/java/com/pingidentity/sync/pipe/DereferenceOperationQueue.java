package com.pingidentity.sync.pipe;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DereferenceOperationQueue
{
    private static Queue<DereferenceOperation> instance = null;
    
    private DereferenceOperationQueue()
    {
    }
    
    public static Queue<DereferenceOperation> getInstance()
    {
        if (instance == null)
        {
            instance = new ConcurrentLinkedQueue<DereferenceOperation>();
        }
        return instance;
    }
}