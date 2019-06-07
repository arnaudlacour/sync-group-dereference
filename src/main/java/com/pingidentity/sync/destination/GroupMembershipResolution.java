package com.pingidentity.sync.destination;

import com.unboundid.directory.sdk.sync.api.LDAPSyncDestinationPlugin;
import com.unboundid.directory.sdk.sync.config.LDAPSyncDestinationPluginConfig;
import com.unboundid.directory.sdk.sync.types.PreStepResult;
import com.unboundid.directory.sdk.sync.types.SyncOperation;
import com.unboundid.directory.sdk.sync.types.SyncOperationType;
import com.unboundid.directory.sdk.sync.types.SyncServerContext;
import com.unboundid.ldap.sdk.*;
import com.unboundid.util.args.ArgumentException;
import com.unboundid.util.args.ArgumentParser;
import com.unboundid.util.args.AttributeNameArgumentValueValidator;
import com.unboundid.util.args.StringArgument;

import java.util.Arrays;
import java.util.List;

public class GroupMembershipResolution extends LDAPSyncDestinationPlugin
{
    public static final String ATTRIBUTE_ARG = "attribute";
    public static final List<String> ATTRIBUTE_DEFAULT = Arrays.asList("member", "uniqueMember");
    private List<String> attributeList;
    private SyncServerContext serverContext;
    
    @Override
    public String getExtensionName()
    {
        return "Group Membership Resolution";
    }
    
    @Override
    public String[] getExtensionDescription()
    {
        return new String[]{"This extension will turn the potentially costly lookup for a member in a group into a " +
                "search for the user with requesting its membership via the adequate virtual attribute like " +
                "isMemberOF"};
    }
    
    @Override
    public void toString(StringBuilder stringBuilder)
    {
    }
    
    @Override
    public void defineConfigArguments(ArgumentParser parser) throws ArgumentException
    {
        StringArgument attributeArg = new StringArgument(null, ATTRIBUTE_ARG, false, 0, "{attribute}", "Attribute(s) " +
                "for which the plugin should turn the request inside out. It is important that these attributes " +
                "contain valid DNs and is most likely going to be useful for member or uniqueMember",
                ATTRIBUTE_DEFAULT);
        attributeArg.addValueValidator(new AttributeNameArgumentValueValidator());
        parser.addArgument(attributeArg);
    }
    
    @Override
    public ResultCode applyConfiguration(LDAPSyncDestinationPluginConfig config, ArgumentParser parser, List<String>
            adminActionsRequired, List<String> messages)
    {
        attributeList = parser.getStringArgument(ATTRIBUTE_ARG).getValues();
        return ResultCode.SUCCESS;
    }
    
    @Override
    public void initializeLDAPSyncDestinationPlugin(SyncServerContext serverContext, LDAPSyncDestinationPluginConfig
            config, ArgumentParser parser) throws LDAPException
    {
        this.serverContext = serverContext;
        applyConfiguration(config, parser, null, null);
    }
    
    
    @Override
    public PreStepResult preFetch(LDAPInterface destinationConnection, SearchRequest searchRequest, List<Entry>
            fetchedEntries, SyncOperation operation) throws LDAPException
    {
        Entry destinationEntryAfterChange = operation.getDestinationEntryAfterChange();
        Boolean processingEffected = Boolean.FALSE;
        if (SyncOperationType.MODIFY == operation.getType())
        {
            String groupDN = destinationEntryAfterChange.getDN();
            for (Attribute attribute : operation.getDestinationEntryAfterChange().getAttributes())
            {
                if (attribute == null)
                {
                    continue;
                }
                if (attributeList.stream().anyMatch(attribute.getBaseName() :: equalsIgnoreCase))
                {
                    for (String memberDN : attribute.getValues())
                    {
                        if (memberDN == null)
                        {
                            continue;
                        }
                        if (!memberDN.isEmpty())
                        {
                            processingEffected = Boolean.TRUE;
                            SearchResultEntry memberEntry = destinationConnection.getEntry(memberDN, "isMemberOf");
                            if ( !  memberEntry.hasAttributeValue("isMemberOf", groupDN) )
                            {
                                destinationEntryAfterChange.removeAttributeValue(attribute.getName(),memberDN);
                            }
                        }
                    }
                }
            }
            if (processingEffected)
            {
                fetchedEntries.add(destinationEntryAfterChange);
                return PreStepResult.SKIP_CURRENT_STEP;
            }
        }
        return PreStepResult.CONTINUE;
    }
}
