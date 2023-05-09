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
     * Exports all Trading Network document attributes as an IData[].
     *
     * @return All Trading Network document attributes as an IData[].
     * @throws ServiceException If an error occurs.
     */
    private static IData[] exportBizDocAttributes() throws ServiceException {
        BizDocAttribute[] attributes = BizDocAttributeHelper.list();

        IData[] export = new IData[attributes.length];
        for (int i = 0; i < attributes.length; i++) {
            // sort properties by key name and remove null values
            export[i] = IDataHelper.compact(IDataHelper.sort(attributes[i]));
            // remove incomparable elements to reduce differences in comparisons across instances
            IDataHelper.remove(export[i], "LastModifiedTime");
            IDataHelper.remove(export[i], "VERSION_NO");
        }

        return export;
    }

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
            // sort properties by key name and remove null values
            export[i] = IDataHelper.compact(IDataHelper.sort(docTypes[i]));
            // remove incomparable elements to reduce differences in comparisons across instances
            IDataHelper.remove(export[i], "LastModified");
            IDataHelper.remove(export[i], "VERSION_NO");
        }

        return export;
    }

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
            // sort properties by key name and remove null values
            export[i] = IDataHelper.compact(IDataHelper.sort(profiles[i]));
            // remove incomparable elements to reduce differences in comparisons across instances
            IDataHelper.remove(export[i], "Contact/Address/Address_Type");
            IDataHelper.remove(export[i], "Corporate/PreferredProtocolType");
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
                export[i] = IDataHelper.sort(queues[i].getIData());
            }
        } catch(IOException ex) {
            ExceptionHelper.raise(ex);
        } catch(SQLException ex) {
            ExceptionHelper.raise(ex);
        }

        return export;
    }

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
            // sort properties by key name and remove null values
            export[i] = IDataHelper.compact(IDataHelper.sort(rules.get(i)));
            // remove incomparable elements to reduce differences in comparisons across instances
            IDataHelper.remove(export[i], "DefaultRule?");
            IDataHelper.remove(export[i], "IntegrationDetails");
            IDataHelper.remove(export[i], "LastChangeID");
            IDataHelper.remove(export[i], "LastChangeSession");
            IDataHelper.remove(export[i], "LastChangeUser");
            IDataHelper.remove(export[i], "LastModified");
            IDataHelper.remove(export[i], "ReceiverDisplayName");
            IDataHelper.remove(export[i], "RuleIndex");
            IDataHelper.remove(export[i], "SenderDisplayName");
            IDataHelper.remove(export[i], "ToMwSString");
            IDataHelper.remove(export[i], "WmioCloudContainerDetails");
            IDataHelper.remove(export[i], "WmioIntegrationDetails");
        }

        return export;
    }
}
