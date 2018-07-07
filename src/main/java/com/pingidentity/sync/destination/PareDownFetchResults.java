package com.pingidentity.sync.destination;

import com.unboundid.directory.sdk.sync.api.LDAPSyncDestinationPlugin;
import com.unboundid.directory.sdk.sync.config.LDAPSyncDestinationPluginConfig;
import com.unboundid.directory.sdk.sync.types.PreStepResult;
import com.unboundid.directory.sdk.sync.types.SyncOperation;
import com.unboundid.directory.sdk.sync.types.SyncServerContext;
import com.unboundid.ldap.sdk.*;
import com.unboundid.ldap.sdk.controls.MatchedValuesFilter;
import com.unboundid.ldap.sdk.controls.MatchedValuesRequestControl;
import com.unboundid.util.args.ArgumentException;
import com.unboundid.util.args.ArgumentParser;
import com.unboundid.util.args.AttributeNameArgumentValueValidator;
import com.unboundid.util.args.StringArgument;

import java.util.ArrayList;
import java.util.List;

/**
 * This class aims at paring down results from a destination to:
 * - limit the cost at the destination
 * - limit the amount of processing in sync server to compute changes
 */
public class PareDownFetchResults extends LDAPSyncDestinationPlugin
{
    
    public static final String ATTRIBUTE_ARG = "attribute";
    private List<String> attributeList;
    
    /**
     * Preforms the necessary processing to generate the extension name
     *
     * @return the extension name
     */
    @Override
    public String getExtensionName()
    {
        return "Pare down fetch results";
    }
    
    /**
     * Performs the necessary processing to generate a description for the extension
     *
     * @return an array of descriptive paragraphs
     */
    @Override
    public String[] getExtensionDescription()
    {
        return new String[]{"This plugin pares down the results from the destination"};
    }
    
    /**
     * I honestly have no idea what the point of this one is
     *
     * @param stringBuilder
     */
    @Override
    public void toString(StringBuilder stringBuilder)
    {
    }
    
    @Override
    public void defineConfigArguments(ArgumentParser parser) throws ArgumentException
    {
        StringArgument attributeArg = new StringArgument(null, ATTRIBUTE_ARG, true, 0, "{attribute}", "Attribute(s) " +
                "for which the results should be pared down to the results from the source changelog entry");
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
        applyConfiguration(config, parser, null, null);
    }
    
    /**
     * Performs the necessary processing to pare down the results coming from the destination
     *
     * @param destinationConnection the connection to the destination
     * @param searchRequest the search request
     * @param fetchedEntries the list of entries fetched
     * @param operation the sync operation
     * @return the PreStepResult
     */
    @Override
    public PreStepResult preFetch(LDAPInterface destinationConnection, SearchRequest searchRequest, List<Entry>
            fetchedEntries, SyncOperation operation)
    {
        List<MatchedValuesFilter> filters = new ArrayList<>();
        for (Modification modification : operation.getChangeLogEntry().getModifications())
        {
            if (attributeList.stream().anyMatch(modification.getAttributeName() :: equalsIgnoreCase))
            {
                for (byte[] value : modification.getValueByteArrays())
                {
                    filters.add(MatchedValuesFilter.createEqualityFilter(modification.getAttributeName(), value));
                }
            }
        }
        if ( filters.size()>0)
        {
            searchRequest.addControl(new MatchedValuesRequestControl(filters));
        }
        return PreStepResult.CONTINUE;
    }
}
