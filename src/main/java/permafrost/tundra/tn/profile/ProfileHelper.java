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
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.lang.ExceptionHelper;
import permafrost.tundra.lang.IterableEnumeration;
import permafrost.tundra.tn.cache.ProfileCache;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.Vector;

public final class ProfileHelper {
    /**
     * Disallow instantiation of this class.
     */
    private ProfileHelper() {}

    /**
     * Returns the partner profile associated with the given internal or external ID from the Trading Networks database.
     *
     * @param id                An ID associated with the partner profile to be returned.
     * @return                  The partner profile summary associated with the given ID, or null if no profile for this
     *                          ID exists.
     * @throws ServiceException If a database error occurs.
     */
    public static ProfileSummary getProfileSummary(ProfileID id) throws ServiceException {
        if (id == null) return null;

        ProfileSummary profile = null;

        id = id.intern(); // normalize to internal ID

        if (id != null) {
            // if the id is null or doesn't exist, this call returns null
            profile = getProfileSummary(id.getIdentity());
        }

        return profile;
    }

    /**
     * Returns the partner profile summary associated with the given internal ID from the Trading Networks database.
     *
     * @param id                An internal ID associated with the partner profile to be returned.
     * @return                  The partner profile summary associated with the given internal ID, or null if no profile
     *                          for this ID exists.
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
     * @param id                An ID associated with the partner profile to be returned.
     * @return                  The partner profile associated with the given ID, or null if no profile for this ID
     *                          exists.
     * @throws ServiceException If a database error occurs.
     */
    public static Profile get(ProfileID id) throws ServiceException {
        if (id == null) return null;

        Profile profile = null;

        id = id.intern(); // normalize to internal ID

        if (id != null) {
            // if the id is null or doesn't exist, this call returns null
            profile = get(id.getIdentity());
        }

        return profile;
    }

    /**
     * Returns the partner profile associated with the given internal ID from the Trading Networks database.
     *
     * @param identity          An internal ID associated with the partner profile to be returned.
     * @return                  The partner profile associated with the given internal ID, or null if no profile for
     *                          this ID exists.
     * @throws ServiceException If a database error occurs.
     */
    public static Profile get(String identity) throws ServiceException {
        return get(identity, false);
    }

    /**
     * Returns the partner profile associated with the given internal ID from the Trading Networks database.
     *
     * @param identity          An internal ID associated with the partner profile to be returned.
     * @param refresh           If true refreshes the partner profile summaries from the database.
     * @return                  The partner profile associated with the given internal ID, or null if no profile for
     *                          this ID exists.
     * @throws ServiceException If a database error occurs.
     */
    public static Profile get(String identity, boolean refresh) throws ServiceException {
        if (identity == null) return null;

        Profile profile = null;

        try {
            // if the id is null or doesn't exist, this call returns null
            ProfileSummary summary = ProfileStore.getProfileSummary(identity, refresh);
            if (summary != null) profile = ProfileStore.getProfile(identity);
        } catch(ProfileStoreException ex) {
            ExceptionHelper.raise(ex);
        }

        return profile;
    }

    /**
     * Returns the profile identity from the given IData profile subset.
     *
     * @param profileSubset The IData profile subset, which might contain either "ProfileID" or "Corporate/PartnerID".
     * @return              The profile identity.
     */
    public static String getIdentity(IData profileSubset) {
        String profileIdentity = null;

        if (profileSubset != null) {
            IDataCursor cursor = profileSubset.getCursor();
            try {
                profileIdentity = IDataHelper.get(cursor, "ProfileID", String.class);
                if (profileIdentity == null) {
                    IData corporate = IDataHelper.get(cursor, "Corporate", IData.class);
                    if (corporate != null) {
                        IDataCursor corporateCursor = corporate.getCursor();
                        try {
                            profileIdentity = IDataHelper.get(corporateCursor, "PartnerID", String.class);
                        } finally {
                            corporateCursor.destroy();
                        }
                    }
                }
            } finally {
                cursor.destroy();
            }
        }

        return profileIdentity;
    }

    /**
     * Returns the My Enterprise profile from the Trading Networks database.
     *
     * @return                  The My Enterprise profile from the Trading Networks database, or null if no My
     *                          Enterprise profile exists.
     * @throws ServiceException If a database error occurs.
     */
    public static Profile self() throws ServiceException {
        return get(ProfileStore.getMyID());
    }

    /**
     * Returns a list of all partner profiles sorted by display name.
     *
     * @return                  A list of all partner profiles sorted by display name.
     * @throws ServiceException If a database error occurs.
     */
    public static Profile[] list() throws ServiceException {
        return list(false);
    }

    /**
     * Returns a list of all partner profiles sorted by display name.
     *
     * @param refresh           If true refreshes the cached list of profile summaries from the database first.
     * @return                  A list of all partner profiles sorted by display name.
     * @throws ServiceException If a database error occurs.
     */
    public static Profile[] list(boolean refresh) throws ServiceException {
        Vector summaries = ProfileStore.getProfileSummaryList(false, refresh);
        List<Profile> output = new ArrayList<Profile>(summaries.size());

        for (Object object : summaries) {
            if (object instanceof ProfileSummary) {
                ProfileSummary summary = (ProfileSummary)object;
                Profile profile = get(summary.getProfileID());
                if (profile != null) output.add(profile);
            }
        }

        // sort by profile display name
        Collections.sort(output, new ProfileDisplayNameComparator());

        return output.toArray(new Profile[0]);
    }

    /**
     * Profile Comparator which compares based on display name.
     */
    public static class ProfileDisplayNameComparator implements Comparator<Profile> {
        /**
         * Compares two profiles using the display name.
         *
         * @param profile       The first object to be compared.
         * @param otherProfile  The second object to be compared.
         * @return              The result of the comparison.
         */
        @Override
        public int compare(Profile profile, Profile otherProfile) {
            return profile.getDisplayName().compareTo(otherProfile.getDisplayName());
        }
    }

    /**
     * Returns all external IDs associated with the given profile as an array of ProfileID objects.
     *
     * @param profile           The profile whose external IDs are to be returned.
     * @return                  The external IDs associated with the given profile.
     * @throws ServiceException If a database error occurs.
     */
    public static ExternalID[] getExternalIDsAsArray(Profile profile) throws ServiceException {
        Collection<ExternalID> collection = getExternalIDs(profile);
        return collection.toArray(new ExternalID[0]);
    }

    /**
     * Returns all external IDs associated with the given profile as a collection of ProfileID objects.
     *
     * @param profile           The profile whose external IDs are to be returned.
     * @return                  The external IDs associated with the given profile.
     * @throws ServiceException If a database error occurs.
     */
    public static Collection<ExternalID> getExternalIDs(Profile profile) throws ServiceException {
        List<ExternalID> list = new ArrayList<ExternalID>();

        if (profile != null) {
            for (Object object : IterableEnumeration.of(profile.getExternalIDs())) {
                if (object instanceof ID) {
                    ID id = (ID)object;
                    list.add(new ExternalID(id.getExternalID(), id.getIDType(), Integer.valueOf(id.getSeqNo())));
                }
            }
        }

        return list;
    }

    /**
     * Converts a Trading Networks partner profile to an IData representation which is a superset of
     * both the Profile and ProfileSummary objects.
     *
     * @param profile           The partner profile to convert to an IData representation.
     * @return                  An IData representation of the given partner profile.
     * @throws ServiceException If a database error occurs.
     */
    @SuppressWarnings("unchecked")
    public static IData toIData(Profile profile) throws ServiceException {
        if (profile == null) return null;

        IData output = IDataHelper.normalize(profile);

        Corporation corporation = profile.getCorporation();

         if (corporation != null) {
            String partnerID = corporation.getPartnerID();

            if (partnerID != null) {
                IDataCursor cursor = output.getCursor();
                try {
                    IDataHelper.put(cursor, "DisplayName", profile.getDisplayName(), false);
                    IDataHelper.put(cursor, "DefaultID", getDefaultExternalID(profile), false);
                    IDataHelper.put(cursor, "ExternalID", getExternalIDsAsIData(profile), false);
                    IDataHelper.put(cursor, "ExtendedFields", getExtendedFieldsAsIData(partnerID), false);
                    IDataHelper.put(cursor, "Delivery", DestinationHelper.toIDataArray(IterableEnumeration.of(profile.getDestinations())), false);
                    IDataHelper.put(cursor, "DeliveryMethods", DestinationHelper.toIData(IterableEnumeration.of(profile.getDestinations()), profile.getPreferredDestination()), false);
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
     * @param profiles          The list of partner profiles to convert to an IData[] representation.
     * @return                  An IData[] representation of the given partner profiles.
     * @throws ServiceException If a database error occurs.
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
     * @param profile           The profile whose external IDs are to be returned.
     * @return                  The external IDs associated with the given profile.
     * @throws ServiceException If a database error occurs.
     */
    private static IData getExternalIDsAsIData(Profile profile) throws ServiceException {
        if (profile == null) return null;

        IData output = IDataFactory.create();
        IDataCursor cursor = output.getCursor();

        try {
            for (ExternalID externalID : getExternalIDs(profile)) {
                cursor.insertAfter(externalID.getTypeName(), externalID.getIdentity());
            }
        } finally {
            cursor.destroy();
        }

        return output;
    }

    /**
     * Returns the value of the default external ID for the given profile.
     *
     * @param profile           The profile whose default external ID should be returned.
     * @return                  The value of the default external ID for the given profile.
     * @throws ServiceException If a database error occurs.
     */
    private static String getDefaultExternalID(Profile profile) throws ServiceException {
        if (profile == null) return null;

        int defaultType = ProfileStore.getDefaultIDType();
        String defaultID = null;

        for (ExternalID externalID : getExternalIDs(profile)) {
            int type = externalID.getType();
            if (type == defaultType) defaultID = externalID.getIdentity();
        }

        return defaultID;
    }

    /**
     * Returns the extended fields for a given partner profile internal ID.
     *
     * @param id                The internal ID of the profile to return the associated extended fields for.
     * @return                  The extended fields associated with the given profile internal ID.
     * @throws ServiceException If a database error occurs.
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
                                if (name != null && value != null) {
                                    gc.insertAfter(name, value);
                                }
                            }
                        }

                        gc.destroy();

                        if (!fields.isEmpty()) {
                            cursor.insertAfter(groupName, group);
                        }
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
     * @return                  All registered external ID types in Trading Networks by ID.
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

    /**
     * Returns a summary of the given object, suitable for logging.
     *
     * @param profileID The ID of the object to summarize.
     * @return          The summary of the object.
     */
    public static IData summarize(String profileID) {
        IData summary = null;
        try {
            summary = summarize(ProfileStore.getProfileSummary(profileID));
        } catch(ProfileStoreException ex) {
            ExceptionHelper.raiseUnchecked(ex);
        }
        return summary;
    }

    /**
     * Returns a summary of the given object, suitable for logging.
     *
     * @param object    The object to summarize.
     * @return          The summary of the object.
     */
    public static IData summarize(Profile object) {
        IData summary = null;
        if (object != null) {
            summary = IDataFactory.create();
            IDataCursor cursor = summary.getCursor();
            try {
                Corporation corporation = object.getCorporation();
                if (corporation != null) {
                    IDataHelper.put(cursor, "PartnerID", corporation.getPartnerID());
                    IDataHelper.put(cursor, "CorporationName", corporation.getCorporationName(), false);
                    IDataHelper.put(cursor, "OrgUnitName", corporation.getOrgUnitName(), false);
                }
            } finally {
                cursor.destroy();
            }
        }
        return summary;
    }

    /**
     * Returns a summary of the given object, suitable for logging.
     *
     * @param object    The object to summarize.
     * @return          The summary of the object.
     */
    public static IData summarize(ProfileSummary object) {
        IData summary = null;
        if (object != null) {
            summary = IDataFactory.create();
            IDataCursor cursor = summary.getCursor();
            try {
                IDataHelper.put(cursor, "PartnerID", object.getProfileID());
                IDataHelper.put(cursor, "CorporationName", object.getCorporationName(), false);
                IDataHelper.put(cursor, "OrgUnitName", object.getOrgUnitName(), false);
            } finally {
                cursor.destroy();
            }
        }
        return summary;
    }

    /**
     * Returns the partnerID from the given profile.
     *
     * @param profile   The profile whose partnerID is to be returned.
     * @return          The partnerID from the given profile, if it exists.
     */
    private static String getPartnerID(IData profile) {
        String partnerID = null;
        if (profile != null) {
            IDataCursor cursor = profile.getCursor();
            try {
                partnerID = IDataHelper.get(cursor, "ProfileID", String.class);
                if (partnerID == null) {
                    IData corporate = IDataHelper.get(cursor, "Corporate", IData.class);
                    if (corporate != null) {
                        IDataCursor corporateCursor = corporate.getCursor();
                        try {
                            partnerID = IDataHelper.get(corporateCursor, "PartnerID", String.class);
                        } finally {
                            corporateCursor.destroy();
                        }
                    }
                }
            } finally {
                cursor.destroy();
            }
        }
        return partnerID;
    }

    /**
     * Normalizes the given IData document, if it is already a Profile it is
     * returned, otherwise it is queried for a PartnerID to the Profile related
     * to that PartnerID is returned.
     *
     * @param profileDocument   The Profile as an IData document.
     * @param raiseIfMissing    If true and no profile is found an exception will be thrown.
     * @return                  The normalized Profile.
     * @throws ServiceException If an error occurs.
     */
    public static Profile normalize(IData profileDocument, boolean raiseIfMissing) throws ServiceException {
        Profile profile = null;
        String partnerID = null;

        if (profileDocument instanceof Profile) {
            profile = (Profile)profileDocument;
        } else if (profileDocument != null) {
            partnerID = getPartnerID(profileDocument);
            if (partnerID != null) {
                profile = get(partnerID);
            }
        }

        if (profile == null && raiseIfMissing) {
            throw new ServiceException("No Trading Networks partner profile exists for specified ID: " + partnerID);
        }

        return profile;
    }

    private static final String PREFERRED_PROTOCOL = "Preferred Protocol";
    private static final String RECEIVERS_PREFERRED_PROTOCOL = "Receiver's Preferred Protocol";

    /**
     * Returns the Destination from the given profile with the given name.
     *
     * @param profile           The profile whose destination is to be returned.
     * @param destinationName   The name of the destination to be returned.
     * @return                  The Destination with the given name, or null.
     */
    public static IData getDestination(IData profile, String destinationName) throws ServiceException {
        IData destination = null;
        if (profile != null && destinationName != null) {
            destinationName = destinationName.trim();
            if (destinationName.equals(RECEIVERS_PREFERRED_PROTOCOL)) {
                destinationName = PREFERRED_PROTOCOL;
            }

            String partnerID = getPartnerID(profile);
            if (partnerID != null) {
                profile = ProfileCache.getInstance().get(partnerID);
                if (profile != null) {
                    IDataCursor cursor = profile.getCursor();
                    try {
                        IData deliveryMethods = IDataHelper.get(cursor, "DeliveryMethods", IData.class);
                        if (deliveryMethods != null) {
                            IDataCursor deliveryMethodsCursor = deliveryMethods.getCursor();
                            try {
                                destination = IDataHelper.get(deliveryMethodsCursor, destinationName, IData.class);
                            } finally {
                                deliveryMethodsCursor.destroy();
                            }
                        }
                    } finally {
                        cursor.destroy();
                    }
                }
            }
        }

        return destination;
    }

    /**
     * Returns the Destination from the given Profile with the given name.
     *
     * @param profile           The Profile whose destination is to be returned.
     * @param destinationName   The name of the destination to be returned.
     * @return                  The Destination with the given name, or null.
     */
    public static Destination getDestination(Profile profile, String destinationName) {
        Destination destination = null;
        if (profile != null && destinationName != null) {
            destinationName = destinationName.trim();
            if (destinationName.equals(PREFERRED_PROTOCOL) || destinationName.equals(RECEIVERS_PREFERRED_PROTOCOL)) {
                destination = profile.getPreferredDestination();
            } else {
                Enumeration enumeration = profile.getDestinations();
                if (enumeration != null) {
                    while (enumeration.hasMoreElements()) {
                        Destination profileDestination = (Destination)enumeration.nextElement();
                        if (destinationName.equals(DestinationHelper.getName(profileDestination).trim())) {
                            destination = profileDestination;
                            break;
                        }
                    }
                }
            }
        }
        return destination;
    }
}
