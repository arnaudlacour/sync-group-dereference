package com.pingidentity.sync.pipe;

import com.unboundid.directory.sdk.common.api.MonitorProvider;
import com.unboundid.directory.sdk.common.config.MonitorProviderConfig;
import com.unboundid.directory.sdk.common.types.ServerContext;
import com.unboundid.ldap.sdk.Attribute;
import com.unboundid.ldap.sdk.LDAPException;
import com.unboundid.ldap.sdk.ResultCode;
import com.unboundid.util.args.ArgumentParser;
import org.w3c.dom.Attr;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Queue;

public class GroupDereferenceMonitorProvider extends MonitorProvider {

    private GroupDereference groupDereference;
    DateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss");

    private volatile MonitorProviderConfig config;
    private ServerContext serverContext;

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
        return "GroupDereferenceMonitorProvider";
    }

    @Override
    public String[] getExtensionDescription() {
        return new String[]{"This monitor provider tracks metrics for GroupDereference"};
    }

    @Override
    public String getMonitorInstanceName() {
        return "Group Dereference Monitor Provider " + config.getConfigObjectName();
    }

    @Override()
    public void initializeMonitorProvider(final ServerContext serverContext,
                                          final MonitorProviderConfig config,
                                          final ArgumentParser parser)
            throws LDAPException {
        this.serverContext = serverContext;
        this.config = config;
    }

    @Override()
    public boolean isConfigurationAcceptable(final MonitorProviderConfig config,
                                             final ArgumentParser parser,
                                             final List<String> unacceptableReasons) {
        return true;
    }

    @Override()
    public long getUpdateIntervalMillis() {
        return 10000L;
    }

    @Override()
    public ResultCode applyConfiguration(final MonitorProviderConfig config,
                                         final ArgumentParser parser,
                                         final List<String> adminActionsRequired,
                                         final List<String> messages) {
        this.config = config;
        return ResultCode.SUCCESS;
    }

    @Override
    public List<Attribute> getMonitorAttributes() {
        List<Attribute> result = new ArrayList<>();

        result.add(new Attribute("monitor-last-updated",dateFormat.format(Calendar.getInstance().getTime())));

        Queue<DereferenceOperation> queue = DereferenceOperationQueue.getInstance();

        result.add(new Attribute("current-queue-size",Integer.toString(queue.size())));
        result.add(new Attribute("max-queue-size",Long.toString(GroupDereference.maxQueueSize.get())));
        result.add(new Attribute("queue-add-attempts",Long.toString(GroupDereference.queueAddAttempts.get())));
        result.add(new Attribute("queue-add-failures",Long.toString(GroupDereference.queueAddFailures.get())));

        return result;
    }
}
