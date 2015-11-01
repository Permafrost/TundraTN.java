/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Lachlan Dowding
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package permafrost.tundra.tn.profile;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;
import com.wm.app.b2b.server.ServiceException;
import com.wm.app.tn.profile.Corporation;
import com.wm.app.tn.profile.Destination;
import com.wm.app.tn.profile.ExtendedProfileField;
import com.wm.app.tn.profile.ID;
import com.wm.app.tn.profile.LookupStore;
import com.wm.app.tn.profile.LookupStoreException;
import com.wm.app.tn.profile.Profile;
import com.wm.app.tn.profile.ProfileStore;
import com.wm.app.tn.profile.ProfileStoreException;
import com.wm.app.tn.profile.ProfileSummary;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import com.wm.data.IDataUtil;
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.lang.ExceptionHelper;
import permafrost.tundra.lang.IterableEnumeration;

public class ProfileHelper {
    /**
     * Disallow instantiation of this class.
     */
    private ProfileHelper() {}

    /**
     * Returns the partner profile associated with the given internal or external ID from the Trading Networks database.
     *
     * @param id An ID associated with the partner profile to be returned.
     * @return The partner profile summary associated with the given ID, or null if no profile for this ID exists.
     * @throws ServiceException If a database error occurs.
     */
    public static ProfileSummary getProfileSummary(ProfileID id) throws ServiceException {
        if (id == null) return null;

        ProfileSummary profile = null;

        id = id.toInternalID(); // normalize to internal ID

        if (id != null) {
            // if the id is null or doesn't exist, this call returns null
            profile = getProfileSummary(id.getValue());
        }

        return profile;
    }

    /**
     * Returns the partner profile summary associated with the given internal ID from the Trading Networks database.
     *
     * @param id An internal ID associated with the partner profile to be returned.
     * @return The partner profile summary associated with the given internal ID, or null if no profile for this ID exists.
     * @throws ServiceException If a database error occurs.
     */
    public static ProfileSummary getProfileSummary(String id) throws ServiceException {
        if (id == null) return null;

        ProfileSummary profile = null;

        try {
            // if the id is null or doesn't exist, this call returns null
            profile = ProfileStore.getProfileSummary(id);
        } catch(ProfileStoreException ex) {
            ExceptionHelper.raise(ex);
        }

        return profile;
    }


    /**
     * Returns the partner profile associated with the given ID from the Trading Networks database.
     *
     * @param id An ID associated with the partner profile to be returned.
     * @return The partner profile associated with the given ID, or null if no profile for this ID exists.
     * @throws ServiceException If a database error occurs.
     */
    public static Profile get(ProfileID id) throws ServiceException {
        if (id == null) return null;

        Profile profile = null;

        id = id.toInternalID(); // normalize to internal ID

        if (id != null) {
            // if the id is null or doesn't exist, this call returns null
            profile = get(id.getValue());
        }

        return profile;
    }

    /**
     * Returns the partner profile associated with the given internal ID from the Trading Networks database.
     *
     * @param id An internal ID associated with the partner profile to be returned.
     * @return The partner profile associated with the given internal ID, or null if no profile for this ID exists.
     * @throws ServiceException If a database error occurs.
     */
    public static Profile get(String id) throws ServiceException {
        if (id == null) return null;

        Profile profile = null;

        try {
            // if the id is null or doesn't exist, this call returns null
            ProfileSummary summary = ProfileStore.getProfileSummary(id);
            if (summary != null) profile = ProfileStore.getProfile(id);
        } catch(ProfileStoreException ex) {
            ExceptionHelper.raise(ex);
        }

        return profile;
    }

    /**
     * Returns the My Enterprise profile from the Trading Networks database.
     *
     * @return The My Enterprise profile from the Trading Networks database, or null if no My Enterprise profile exists.
     * @throws ServiceException
     */
    public static Profile self() throws ServiceException {
        return get(ProfileStore.getMyID());
    }

    /**
     * Returns a list of all partner cache from the Trading Networks database.
     *
     * @return A list of all partner cache from the Trading Networks database.
     * @throws ServiceException If a database error occurs.
     */
    public static Profile[] list() throws ServiceException {
        Vector summaries = ProfileStore.getProfileSummaryList(false, false);
        List<Profile> output = new ArrayList<Profile>(summaries.size());

        for (Object object : summaries) {
            if (object instanceof ProfileSummary) {
                ProfileSummary summary = (ProfileSummary)object;
                Profile profile = get(summary.getProfileID());
                if (profile != null) output.add(profile);
            }
        }

        return output.toArray(new Profile[output.size()]);
    }

    /**
     * Returns all external IDs associated with the given profile as an array of ProfileID objects.
     *
     * @param profile The profile whose external IDs are to be returned.
     * @return The external IDs associated with the given profile.
     * @throws ServiceException If a database error occurs.
     */
    public static ProfileID[] getExternalIDsAsArray(Profile profile) throws ServiceException {
        Collection<ProfileID> collection = getExternalIDs(profile);
        return collection.toArray(new ProfileID[collection.size()]);
    }

    /**
     * Returns all external IDs associated with the given profile as a collection of ProfileID objects.
     *
     * @param profile The profile whose external IDs are to be returned.
     * @return The external IDs associated with the given profile.
     * @throws ServiceException If a database error occurs.
     */
    public static Collection<ProfileID> getExternalIDs(Profile profile) throws ServiceException {
        Map<Integer, String> types = getExternalIDTypes();
        List<ProfileID> list = new ArrayList<ProfileID>(types.size());

        if (profile != null) {
            for (Object object : IterableEnumeration.of(profile.getExternalIDs())) {
                if (object instanceof ID) {
                    ID id = (ID)object;
                    list.add(new ProfileID(id.getExternalID(), types.get(id.getIDType())));
                }
            }
        }

        return list;
    }

    /**
     * Converts a Trading Networks partner profile to an IData representation which is a superset of
     * both the Profile and ProfileSummary objects.
     *
     * @param profile The partner profile to convert to an IData representation.
     * @return An IData representation of the given partner profile.
     * @throws ServiceException
     */
    public static IData toIData(Profile profile) throws ServiceException {
        if (profile == null) return null;

        IData output = IDataHelper.normalize(profile);

        Corporation corporation = profile.getCorporation();

        if (corporation != null) {
            String partnerID = corporation.getPartnerID();

            if (partnerID != null) {
                IDataCursor cursor = output.getCursor();
                try {
                    IDataUtil.put(cursor, "ExternalID", getExternalIDsAsIData(profile));
                    IDataUtil.put(cursor, "ExtendedFields", getExtendedFieldsAsIData(partnerID));
                    IDataUtil.put(cursor, "DeliveryMethods", getDeliveryAsIData(profile));
                } finally {
                    cursor.destroy();
                }

                output = IDataHelper.merge(IDataHelper.normalize(getProfileSummary(partnerID)), output);
            }
        }

        return output;
    }

    /**
     * Converts a list of Trading Networks partner profiles to an IData[] representation which is a superset of
     * both the Profile and ProfileSummary objects.
     *
     * @param profiles The list of partner profiles to convert to an IData[] representation.
     * @return An IData[] representation of the given partner profiles.
     * @throws ServiceException
     */
    public static IData[] toIDataArray(Profile[] profiles) throws ServiceException {
        if (profiles == null) return null;

        IData[] output = new IData[profiles.length];

        for (int i = 0; i < profiles.length; i++) {
            output[i] = toIData(profiles[i]);
        }

        return output;
    }

    /**
     * Returns all external IDs associated with the given profile as an IData.
     *
     * @param profile The profile whose external IDs are to be returned.
     * @return The external IDs associated with the given profile.
     * @throws ServiceException If a database error occurs.
     */
    private static IData getExternalIDsAsIData(Profile profile) throws ServiceException {
        if (profile == null) return null;

        IData output = IDataFactory.create();
        IDataCursor cursor = output.getCursor();

        try {
            for (ProfileID profileID : getExternalIDs(profile)) {
                IDataUtil.put(cursor, profileID.getType(), profileID.getValue());
            }
        } finally {
            cursor.destroy();
        }

        return output;
    }

    /**
     * Returns all delivery methods associated with the given profile as an IData.
     *
     * @param profile The profile whose delivery methods are to be returned.
     * @return  The delivery methods associated with the given profile with the delivery protocol as the key.
     */
    private static IData getDeliveryAsIData(Profile profile) {
        if (profile == null) return null;

        IData output = IDataFactory.create();
        IDataCursor cursor = output.getCursor();

        try {
            Destination preferred = profile.getPreferredDestination();
            if (preferred != null) IDataUtil.put(cursor, "Preferred Protocol", IDataHelper.normalize(preferred));

            for (Object object : IterableEnumeration.of(profile.getDestinations())) {
                if (object instanceof Destination) {
                    Destination destination = (Destination)object;
                    IDataUtil.put(cursor, destination.getProtocolDisplayName(), IDataHelper.normalize(destination));
                }
            }
        } finally {
            cursor.destroy();
        }

        return output;
    }

    /**
     * Returns the extended fields for a given partner profile internal ID.
     *
     * @param id The internal ID of the profile to return the associated extended fields for.
     * @return The extended fields associated with the given profile internal ID.
     * @throws ServiceException
     */
    private static IData getExtendedFieldsAsIData(String id) throws ServiceException {
        if (id == null) return null;

        IData output = IDataFactory.create();
        IDataCursor cursor = output.getCursor();

        try {
            Hashtable groups = LookupStore.getFieldGroups();
            if (groups != null) {
                Enumeration keys = groups.keys();

                while (keys.hasMoreElements()) {
                    String groupName = (String)keys.nextElement();
                    int groupID = (Short)groups.get(groupName);

                    Vector fields = ProfileStore.getExtendedFields(id, groupID);

                    if (fields != null) {
                        IData group = IDataFactory.create();
                        IDataCursor gc = group.getCursor();

                        for (Object object : fields) {
                            if (object instanceof ExtendedProfileField) {
                                ExtendedProfileField field = (ExtendedProfileField)object;
                                String name = field.getName();
                                Object value = field.getValue();
                                if (name != null && value != null) IDataUtil.put(gc, name, value);
                            }
                        }

                        gc.destroy();

                        if (fields.size() > 0) IDataUtil.put(cursor, groupName, group);
                    }
                }
            }
        } catch(ProfileStoreException ex) {
            ExceptionHelper.raise(ex);
        } finally {
            cursor.destroy();
        }

        return output;
    }

    /**
     * Returns all Trading Networks external ID types as a map, where ID is the key to the map.
     *
     * @return All registered external ID types in Trading Networks by ID.
     * @throws ServiceException If a database error occurs.
     */
    private static Map<Integer, String> getExternalIDTypes() throws ServiceException {
        Map<Integer, String> output = new TreeMap<Integer, String>();

        try {
            Hashtable types = LookupStore.getExternalIDTypes();

            Enumeration keys = types.keys();
            while (keys.hasMoreElements()) {
                String key = (String)keys.nextElement();
                Integer value = (Integer)types.get(key);
                output.put(value, key);
            }
        } catch(LookupStoreException ex) {
            ExceptionHelper.raise(ex);
        }

        return output;
    }
}
