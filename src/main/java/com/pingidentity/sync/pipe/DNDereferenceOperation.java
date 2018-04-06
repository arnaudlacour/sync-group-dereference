package com.pingidentity.sync.pipe;

import com.pingidentity.sync.source.MemberDNQueue;
import com.unboundid.directory.sdk.sync.types.ChangeRecord;
import com.unboundid.ldap.sdk.ChangeType;

/**
 * This class provides a mechanism to simply pass the DN of a member in the queue
 */
public class DNDereferenceOperation implements DereferenceOperation
{
    String dn;
    
    /**
     * Constructor with DN parameter
     *
     * @param dn
     */
    public DNDereferenceOperation(String dn)
    {
        this.dn = dn;
    }
    
    /**
     * Execution simply package the DN in a sync change record and enqueues it
     */
    @Override
    public void execute()
    {
        ChangeRecord.Builder builder = new ChangeRecord.Builder(ChangeType.MODIFY, dn);
        builder.addProperty("DN",dn);
        builder.changeTime(System.currentTimeMillis());
        ChangeRecord changeRecord = builder.build();
        MemberDNQueue.getInstance().add(changeRecord);
    }
}
