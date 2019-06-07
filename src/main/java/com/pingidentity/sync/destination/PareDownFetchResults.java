package com.pingidentity.sync.destination;

import com.unboundid.directory.sdk.common.types.ServerContext;
import com.unboundid.directory.sdk.sync.api.LDAPSyncDestinationPlugin;
import com.unboundid.directory.sdk.sync.config.LDAPSyncDestinationPluginConfig;
import com.unboundid.directory.sdk.sync.types.PreStepResult;
import com.unboundid.directory.sdk.sync.types.SyncOperation;
import com.unboundid.directory.sdk.sync.types.SyncOperationType;
import com.unboundid.directory.sdk.sync.types.SyncServerContext;
import com.unboundid.ldap.sdk.*;
import com.unboundid.ldap.sdk.controls.MatchedValuesFilter;
import com.unboundid.ldap.sdk.controls.MatchedValuesRequestControl;
import com.unboundid.util.args.ArgumentException;
import com.unboundid.util.args.ArgumentParser;
import com.unboundid.util.args.AttributeNameArgumentValueValidator;
import com.unboundid.util.args.StringArgument;

import java.util.*;


/**
 * This class aims at paring down results from a destination to:
 * - limit the cost at the destination
 * - limit the amount of processing in sync server to compute changes
 */
public class PareDownFetchResults extends LDAPSyncDestinationPlugin
{
    
    public static final String DS_CFG_ATTRIBUTE_MAPS = "ds-cfg-attribute-map";
    public static final String DS_CFG_DN_MAP = "ds-cfg-dn-map";
    public static final String ATTRIBUTE_ARG = "attribute";
    private List<String> attributeList;
    private SyncServerContext serverContext;
    
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

    /**
     * Performs the necessary processing to declare the extension configuration arguments
     *
     * @param parser the argument parser
     * @throws ArgumentException in the event any issue was encountered when declaring arguments
     */
    @Override
    public void defineConfigArguments(ArgumentParser parser) throws ArgumentException
    {
        StringArgument attributeArg = new StringArgument(null, ATTRIBUTE_ARG, true, 0, "{attribute}", "Attribute(s) " +
                "for which the results should be pared down to the results from the source changelog entry");
        attributeArg.addValueValidator(new AttributeNameArgumentValueValidator());
        parser.addArgument(attributeArg);
    }

    /**
     * Performs the necessary processing to apply the provided configuration to the instance of the extension
     *
     * @param config the configuration object
     * @param parser the argument parser
     * @param adminActionsRequired a list of message explaining administrative actions required to take to apply configuration successfully
     * @param messages a list of messages explaining issues with applying the provided configuration successfully
     * @return ResultCode.SUCCESS if the configuration was successfully applied
     */
    @Override
    public ResultCode applyConfiguration(LDAPSyncDestinationPluginConfig config, ArgumentParser parser, List<String>
            adminActionsRequired, List<String> messages)
    {
        attributeList = parser.getStringArgument(ATTRIBUTE_ARG).getValues();
        return ResultCode.SUCCESS;
    }

    /**
     * Performs the necessary processing to initiliaze the instance of the extension
     *
     * @param serverContext the server context
     * @param config the configuration object
     * @param parser the argument parser
     * @throws LDAPException in the event any issue was encountered while initializing
     */
    @Override
    public void initializeLDAPSyncDestinationPlugin(SyncServerContext serverContext, LDAPSyncDestinationPluginConfig
            config, ArgumentParser parser) throws LDAPException
    {
        this.serverContext = serverContext;
        applyConfiguration(config, parser, null, null);
    }
    
    /**
     * Performs the necessary processing to pare down the results coming from the destination
     *
     * @param destinationConnection the connection to the destination
     * @param searchRequest         the search request
     * @param fetchedEntries        the list of entries fetched
     * @param operation             the sync operation
     * @return the PreStepResult
     */
    @Override
    public PreStepResult preFetch(LDAPInterface destinationConnection, SearchRequest searchRequest, List<Entry>
            fetchedEntries, SyncOperation operation)
    {
        /*
         * This only applies to modify operations
         *
         * in case the operation is an add, normal processing should occur
         *
         * in case the operation is a delete, the target entry should be delete if present
         *
         */
        if (SyncOperationType.MODIFY == operation.getType())
        {
            List<MatchedValuesFilter> filters = new ArrayList<>();
            
            for (Attribute attribute : operation.getDestinationEntryAfterChange().getAttributes())
            {
                if (attributeList.stream().anyMatch(attribute.getBaseName() :: equalsIgnoreCase))
                {
                    for (byte[] value : attribute.getValueByteArrays())
                    {
                        if (value != null && value.length > 0)
                        {
                            filters.add(MatchedValuesFilter.createEqualityFilter(attribute.getBaseName(), value));
                        }
                    }
                }
            }
            for (Modification modification : operation.getChangeLogEntry().getModifications())
            {
                if (modification.getModificationType() == ModificationType.DELETE
                        && attributeList.stream().anyMatch(modification.getAttribute().getBaseName() ::
                        equalsIgnoreCase))
                {
                    Entry mockSourceEntry = new Entry(operation.getSourceEntry().getDN(), Arrays.asList(modification.getAttribute()));
                    Entry mockMappedSourceEntry = applyMaps(mockSourceEntry,operation,serverContext);
                    for (byte[] value : mockMappedSourceEntry.getAttributeValueByteArrays(modification.getAttributeName()))
                    {
                        if (value != null && value.length > 0)
                        {
                            // this fails if the values have been mapped between the source and the destination
                            filters.add(MatchedValuesFilter.createEqualityFilter(modification.getAttributeName(),
                                    value));
                        }
                    }
                }
            }
            if (filters.size() > 0)
            {
                searchRequest.addControl(new MatchedValuesRequestControl(filters));
            }
        }
        return PreStepResult.CONTINUE;
    }

    /**
     * Performs the necessary processing to apply relevant DN and attribute maps
     * @param entry the entry on which maps will be applied
     * @param operation the SyncOperation
     * @param serverContext the server context
     * @return the resulting entry after all maps have been applied
     */
    private static Entry applyMaps(final Entry entry, final SyncOperation operation, final SyncServerContext serverContext)
    {
        Entry result = null;
        Entry configEntry = getSyncClassConfigEntry(serverContext, operation.getSyncPipeName(), operation
                .getSyncClassName());
        if (configEntry != null)
        {
            Set<String> attributeMaps = getMaps(configEntry, DS_CFG_ATTRIBUTE_MAPS);
            Set<String> dnMaps = getMaps(configEntry, DS_CFG_DN_MAP);
            Set<String> autoMappedSourceAttributes = getAttributes(configEntry,"ds-cfg-auto-mapped-source-attribute");
            Set<String> excludedAutoMappedSourceAttributes = getAttributes(configEntry, "ds-cfg-excluded-auto-mapped-source-attributes");
            Set<String> excludedAutoMappedSourceAttributesRegex = getAttributes(configEntry, "ds-cfg-excluded-auto-mapped-source-attribute-regex");
            
            try
            {
                result = serverContext.applyMaps(entry, dnMaps, attributeMaps, autoMappedSourceAttributes,
                        excludedAutoMappedSourceAttributes, excludedAutoMappedSourceAttributesRegex);
            } catch (LDAPException e)
            {
                e.printStackTrace();
            }
            return result;
        }
        return result;
    }

    /**
     * Performs the necessary processing to retrieve the attributes from the Sync Class configuration entry
     * @param configEntry the Sync Class configuration entry
     * @param attribute the attribute type in the configuration entry that stores the resulting attribute types
     * @return the Set of attribute types found
     */
    private static Set<String> getAttributes(final Entry configEntry, final String attribute)
    {
        Set<String> results = null;
        if (configEntry != null && attribute != null && !attribute.isEmpty() && configEntry.hasAttribute(attribute) )
        {
            String[] values = configEntry.getAttributeValues(attribute);
            if ( values != null && values.length > 0 )
            {
                results = new HashSet<>(values.length);
                for (String value : values )
                {
                    results.add(value);
                }
            }
        }
        return results;
    }

    /**
     * Performs the necessary processing to retrieve the maps from the Sync Class configuration entry
     * @param configEntry the Sync Class configuration entry
     * @param mapAttribute the source attribute type for which maps are sought
     * @return the Set of map DNs found to apply to the attribute type
     */
    private static Set<String> getMaps(final Entry configEntry, final String mapAttribute)
    {
        Set<String> results = null;
        String[] values = configEntry.getAttributeValues(mapAttribute);
        if (values != null && values.length > 0)
        {
            results = new HashSet<>(values.length);
            for (String value : values)
            {
                try
                {
                    DN dn = new DN(value);
                    results.add(dn.getRDN().getAttributeValues()[0]);
                } catch (LDAPException e)
                {
                }
            }
        }
        return results;
    }

    /**
     * Performs the necessary processing to retrieve the DN of the configuration Entry for the provided Pipe and Class
     * @param serverContext the server context
     * @param pipeName the Sync Pipe name
     * @param className the Sync Class name
     * @return the configuration entry found
     */
    private static Entry getSyncClassConfigEntry(final ServerContext serverContext, final String pipeName, final
    String className)
    {
        try
        {
            return serverContext.getInternalRootConnection().getEntry(getSyncClassDN(pipeName, className));
        } catch (LDAPException e)
        {
            // not gonna happen
        }
        return null;
    }

    /**
     * Performs the necessary processing to compute the Sync Class configuration entry DN
     * @param pipeName the Sync Pipe name
     * @param className the Sync Class name
     * @return
     */
    private static String getSyncClassDN(final String pipeName, final String className)
    {
        return "cn=" + className + ",cn=sync classes,cn=" + pipeName + "," +
                "cn=sync pipes,cn=config";
    }
}
