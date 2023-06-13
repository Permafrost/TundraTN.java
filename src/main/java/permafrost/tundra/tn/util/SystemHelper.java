package permafrost.tundra.tn.util;

import com.wm.app.b2b.server.ServiceException;
import com.wm.app.tn.delivery.DeliveryQueue;
import com.wm.app.tn.doc.BizDocAttribute;
import com.wm.app.tn.doc.BizDocType;
import com.wm.app.tn.profile.Profile;
import com.wm.app.tn.route.RoutingRule;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import permafrost.tundra.data.IDataCursorHelper;
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.data.IDataJSONParser;
import permafrost.tundra.data.IDataParser;
import permafrost.tundra.lang.ExceptionHelper;
import permafrost.tundra.lang.StringHelper;
import permafrost.tundra.tn.delivery.DeliveryQueueHelper;
import permafrost.tundra.tn.document.BizDocAttributeHelper;
import permafrost.tundra.tn.document.BizDocTypeHelper;
import permafrost.tundra.tn.profile.ProfileHelper;
import permafrost.tundra.tn.route.RoutingRuleHelper;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

public final class SystemHelper {
    /**
     * Disallow instantiation of this class.
     */
    private SystemHelper() {}

    /**
     * Exports all Trading Network components as a JSON string.
     *
     * @return All Trading Networks components as a JSON string.
     * @throws ServiceException If an error occurs.
     */
    public static String export() throws ServiceException {
        String export = null;

        IData document = IDataFactory.create();
        IDataCursor cursor = document.getCursor();
        IDataParser parser = new IDataJSONParser(true);

        try {
            cursor.insertAfter("Summary", summarise());
            cursor.insertAfter("BizDocAttribute", exportBizDocAttributes());
            cursor.insertAfter("BizDocType", exportBizDocTypes());
            cursor.insertAfter("Profile", exportProfiles());
            cursor.insertAfter("DeliveryQueue", exportDeliveryQueues());
            cursor.insertAfter("RoutingRule", exportRoutingRules());

            export = StringHelper.trim(parser.emit(document, null, String.class));
        } catch(IOException ex) {
            ExceptionHelper.raise(ex);
        } finally {
            cursor.destroy();
        }

        return export;
    }

    /**
     * Returns a summary of Trading Network components defined in this Integration Server instance.
     *
     * @return A summary of Trading Network components defined in this Integration Server instance.
     * @throws ServiceException If an error occurs.
     */
    private static IData summarise() throws ServiceException {
        IData document = IDataFactory.create();
        IDataCursor cursor = document.getCursor();

        try {
            BizDocAttribute[] attributes = BizDocAttributeHelper.list();
            String[] attributeNames = new String[attributes.length];
            for (int i = 0; i < attributes.length; i++) {
                attributeNames[i] = attributes[i].getName();
            }

            BizDocType[] docTypes = BizDocTypeHelper.list();
            String[] docTypeNames = new String[docTypes.length];
            for (int i = 0; i < docTypes.length; i++) {
                docTypeNames[i] = docTypes[i].getName();
            }

            Profile[] profiles = ProfileHelper.list();
            String[] profileNames = new String[profiles.length];
            for (int i = 0; i < profiles.length; i++) {
                profileNames[i] = profiles[i].getDisplayName();
            }

            DeliveryQueue[] queues = DeliveryQueueHelper.list();
            String[] queueNames = new String[queues.length];
            for (int i = 0; i < queues.length; i++) {
                queueNames[i] = queues[i].getQueueName();
            }

            List<RoutingRule> rules = RoutingRuleHelper.list();
            String[] ruleNames = new String[rules.size()];
            for (int i = 0; i < rules.size(); i++) {
                ruleNames[i] = rules.get(i).getName();
            }

            IDataHelper.put(cursor, "BizDocAttribute", attributeNames);
            IDataHelper.put(cursor, "BizDocType", docTypeNames);
            IDataHelper.put(cursor, "Profile", profileNames);
            IDataHelper.put(cursor, "DeliveryQueue", queueNames);
            IDataHelper.put(cursor, "RoutingRule", ruleNames);
        } catch(IOException ex) {
            ExceptionHelper.raise(ex);
        } catch(SQLException ex) {
            ExceptionHelper.raise(ex);
        } finally {
            cursor.destroy();
        }

        return document;
    }

    /**
     * The list of keys to be prepended to the exported component.
     */
    private static final String[] EXPORT_BIZDOCATTRIBUTE_PREPENDED_KEYS = new String[] { "AttributeID", "AttributeName", "AttributeDescription" };
    /**
     * The list of keys to be removed from the exported component.
     */
    private static final String[] EXPORT_BIZDOCATTRIBUTE_REMOVED_KEYS = new String[] { "LastModifiedTime", "VERSION_NO" };

    /**
     * Exports all Trading Network document attributes as an IData[].
     *
     * @return All Trading Network document attributes as an IData[].
     * @throws ServiceException If an error occurs.
     */
    private static IData[] exportBizDocAttributes() throws ServiceException {
        BizDocAttribute[] attributes = BizDocAttributeHelper.list();

        IData[] export = new IData[attributes.length];
        for (int i = 0; i < attributes.length; i++) {
            // sort properties by key name and remove null and incomparable values
            export[i] = IDataHelper.compact(sort(attributes[i], EXPORT_BIZDOCATTRIBUTE_PREPENDED_KEYS, EXPORT_BIZDOCATTRIBUTE_REMOVED_KEYS));
        }

        return export;
    }

    /**
     * The list of keys to be prepended to the exported component.
     */
    private static final String[] EXPORT_BIZDOCTYPE_PREPENDED_KEYS = new String[] { "TypeID", "TypeName", "TypeDescription" };
    /**
     * The list of keys to be removed from the exported component.
     */
    private static final String[] EXPORT_BIZDOCTYPE_REMOVED_KEYS = new String[] { "LastModifiedTime", "VERSION_NO" };

    /**
     * Exports all Trading Network document types as an IData[].
     *
     * @return All Trading Network document types as an IData[].
     * @throws ServiceException If an error occurs.
     */
    private static IData[] exportBizDocTypes() throws ServiceException {
        BizDocType[] docTypes = BizDocTypeHelper.list();

        IData[] export = new IData[docTypes.length];
        for (int i = 0; i < docTypes.length; i++) {
            // sort properties by key name and remove null and incomparable values
            export[i] = IDataHelper.compact(sort(docTypes[i], EXPORT_BIZDOCTYPE_PREPENDED_KEYS, EXPORT_BIZDOCTYPE_REMOVED_KEYS));
        }

        return export;
    }

    /**
     * The list of keys to be removed from the exported component.
     */
    private static final String[] EXPORT_PROFILE_REMOVED_KEYS = new String[] { "Contact/Address/Address_Type", "Corporate/PreferredProtocolType" };

    /**
     * Exports all Trading Network profiles as an IData[].
     *
     * @return All Trading Network profiles as an IData[].
     * @throws ServiceException If an error occurs.
     */
    private static IData[] exportProfiles() throws ServiceException {
        Profile[] profiles = ProfileHelper.list();

        IData[] export = new IData[profiles.length];
        for (int i = 0; i < profiles.length; i++) {
            // sort properties by key name and remove null and incomparable values
            export[i] = IDataHelper.compact(sort(profiles[i], null, EXPORT_PROFILE_REMOVED_KEYS));

            IDataCursor cursor = export[i].getCursor();
            try {
                cursor.insertBefore("PartnerName", profiles[i].getDisplayName());
                cursor.insertBefore("PartnerID", profiles[i].getCorporation().getPartnerID());
            } finally {
                cursor.destroy();
            }
        }

        return export;
    }

    /**
     * Exports all Trading Network delivery queues as an IData[].
     *
     * @return All Trading Network delivery queues as an IData[].
     * @throws ServiceException If an error occurs.
     */
    private static IData[] exportDeliveryQueues() throws ServiceException {
        IData[] export = null;

        try {
            DeliveryQueue[] queues = DeliveryQueueHelper.list();

            export = new IData[queues.length];
            for (int i = 0; i < queues.length; i++) {
                // sort properties by key name
                export[i] = IDataHelper.compact(sort(queues[i].getIData(), null, null));
            }
        } catch(IOException ex) {
            ExceptionHelper.raise(ex);
        } catch(SQLException ex) {
            ExceptionHelper.raise(ex);
        }

        return export;
    }

    /**
     * The list of keys to be prepended to the exported component.
     */
    private static final String[] EXPORT_ROUTINGRULE_PREPENDED_KEYS = new String[] { "RuleID", "RuleName", "RuleDescription" };
    /**
     * The list of keys to be removed from the exported component.
     */
    private static final String[] EXPORT_ROUTINGRULE_REMOVED_KEYS = new String[] { "DefaultRule?", "IntegrationDetails", "LastChangeID", "LastChangeSession", "LastChangeUser", "LastModified", "ReceiverDisplayName", "RuleIndex", "SenderDisplayName", "ToMwSString", "WmioCloudContainerDetails", "WmioIntegrationDetails" };

    /**
     * Exports all Trading Network routing rules as an IData[].
     *
     * @return All Trading Network routing rules as an IData[].
     * @throws ServiceException If an error occurs.
     */
    private static IData[] exportRoutingRules() throws ServiceException {
        List<RoutingRule> rules = RoutingRuleHelper.list();

        // sort rule list by name then index
        Collections.sort(rules, new RoutingRuleHelper.RoutingRuleNameComparator());

        IData[] export = new IData[rules.size()];
        for (int i = 0; i < export.length; i++) {
            // sort properties by key name and remove null and incomparable values
            export[i] = IDataHelper.compact(sort(rules.get(i), EXPORT_ROUTINGRULE_PREPENDED_KEYS, EXPORT_ROUTINGRULE_REMOVED_KEYS));
        }

        return export;
    }

    /**
     * Returns a new IData document prepended with the elements whose keys are in the given prepended keys, with the
     * elements whose keys are in the given removed keys removed, and with the remaining elements sorted by key.
     *
     * @param document      The document to sort.
     * @param prependedKeys The list of keys whose elements are to be prepended to the resulting document.
     * @param removedKeys   The list of keys whose elements are to be removed to the resulting document.
     * @return              A new document containing the elements from the given document sorted by key.
     */
    private static IData sort(IData document, String[] prependedKeys, String[] removedKeys) {
        if (document == null) return null;

        IData sortedDocument = IDataHelper.sort(document);
        IDataCursor sortedCursor = sortedDocument.getCursor();

        IData headDocument = IDataFactory.create();
        IDataCursor headCursor = headDocument.getCursor();

        try {
            if (prependedKeys != null) {
                for (String key : prependedKeys) {
                    if (key != null) {
                        Object value = IDataHelper.remove(sortedDocument, key);
                        if (value != null) {
                            headCursor.insertAfter(key, value);
                        }
                    }
                }
            }

            if (removedKeys != null) {
                for (String key : removedKeys) {
                    if (key != null) {
                        IDataHelper.remove(sortedDocument, key);
                    }
                }
            }

            IDataCursorHelper.prepend(headCursor, sortedCursor);
        } finally {
            headCursor.destroy();
            sortedCursor.destroy();
        }

        return sortedDocument;
    }
}
