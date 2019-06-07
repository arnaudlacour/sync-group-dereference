package com.pingidentity.sync.pipe;

import com.unboundid.directory.sdk.common.api.MonitorProvider;
import com.unboundid.ldap.sdk.Attribute;
import org.w3c.dom.Attr;

import java.util.ArrayList;
import java.util.List;

public class GroupDereferenceMonitorProvider extends MonitorProvider {

    private GroupDereference groupDereference;

    /**
     * An empty constructor is *required*
     */
    public GroupDereferenceMonitorProvider() {
    }

    public GroupDereferenceMonitorProvider(GroupDereference groupDereference){
        this.groupDereference = groupDereference;
    }


    @Override
    public String getExtensionName() {
        return "GroupDereferenceOperationQueueMonitorProvider";
    }

    @Override
    public String[] getExtensionDescription() {
        return new String[]{"This monitor provider tracks metrics for the GroupDereferenceOperationQueue"};
    }

    @Override
    public String getMonitorInstanceName() {
        return null;
    }

    @Override
    public List<Attribute> getMonitorAttributes() {
        List<Attribute> result = new ArrayList<>(1);
        result.add(new Attribute("current-queue-size",Integer.toString(groupDereference.queue.size())));
        result.add(new Attribute("max-queue-size",Long.toString(groupDereference.maxQueueSize.get())));
        result.add(new Attribute("queue-add-attempts",Long.toString(groupDereference.queueAddAttempts.get())));
        result.add(new Attribute("queue-add-failures",Long.toString(groupDereference.queueAddFailures.get())));
        return result;
    }
}
