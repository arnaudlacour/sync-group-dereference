package com.pingidentity.sync.pipe;

import com.unboundid.directory.sdk.common.api.ServerThread;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.util.FixedRateBarrier;

import java.util.Queue;
import java.util.concurrent.BlockingQueue;

/**
 * Server thread processing the dereference queue
 */
public class DereferenceThread implements ServerThread
{
    Queue<DereferenceOperation> queue;
    Boolean run = Boolean.TRUE;
    FixedRateBarrier barrier = null;
    
    /**
     * Performs the necessary processing to initialize the thread
     *
     * @param q the queue
     * @param b the throttling rate barrier (can be null = no throttling )
     */
    public DereferenceThread(final Queue<DereferenceOperation> q,
                             final FixedRateBarrier b)
    {
        queue = q;
        barrier = b;
    }
    
    /**
     * Performs the necessary processing to work on the queue
     * It dequeues {@code DereferenceOperation} and calls their {@code execute} method
     *
     * @throws LDAPException in cases where the execute method does LDAP processing
     */
    @Override
    public void runThread() throws LDAPException
    {
        while (run)
        {
            if (barrier != null)
            {
                barrier.await();
            }
            try
            {
                DereferenceOperation operation = null;
                if ( queue instanceof BlockingQueue )
                {
                    // this is likely more efficient in terms of resource utilization than polling like a madman
                     operation = (DereferenceOperation) ((BlockingQueue) queue).take();
                } else {
                    operation = queue.poll();
                }
                if (operation != null)
                {
                    operation.execute();
                }
            } catch (InterruptedException ie)
            {
                halt();
            }
        }
    }
    
    /**
     * Convenience method to stop the thread
     */
    public void halt()
    {
        run = Boolean.FALSE;
    }
}