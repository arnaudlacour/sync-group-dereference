package com.pingidentity.sync.pipe;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * Singleton pattern to manage a Queue that can convey changes from the one pipe to another
 */
public class DereferenceOperationQueue
{
    private static Queue<DereferenceOperation> instance = null;
    
    private DereferenceOperationQueue()
    {
    }
    
    public synchronized static Queue<DereferenceOperation> getInstance()
    {
        if (instance == null)
        {
            instance = new ConcurrentLinkedQueue<>();
        }
        return instance;
    }
}