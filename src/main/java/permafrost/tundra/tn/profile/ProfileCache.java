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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import com.wm.app.b2b.server.ServiceException;
import com.wm.app.tn.profile.Profile;
import com.wm.app.tn.profile.ProfileStore;
import com.wm.app.tn.profile.ProfileSummary;
import com.wm.data.IData;
import permafrost.tundra.data.IDataHelper;

/**
 * A local in-memory cache of Trading Networks partner cache, to improve performance of Trading Networks
 * processes, and to avoid thread-synchronization bottlenecks in the Trading Networks partner profile
 * implementation.
 */
public class ProfileCache {
    /**
     * Initialization on demand holder idiom.
     */
    private static class Holder {
        /**
         * The singleton instance of the class.
         */
        private static final ProfileCache INSTANCE = new ProfileCache();
    }

    /**
     * Disallow instantiation of this class;
     */
    private ProfileCache() {}

    /**
     * Returns the singleton instance of this class.
     *
     * @return The singleton instance of this class.
     */
    public static ProfileCache getInstance() {
        return Holder.INSTANCE;
    }

    /**
     * The local in-memory cache of Trading Networks partner profiles.
     */
    protected ConcurrentMap<String, IData> cache = new ConcurrentHashMap<String, IData>();

    /**
     * Refreshes all Trading Networks partner profiles stored in the cache from the Trading Networks database.
     *
     * @throws ServiceException If a database error occurs.
     */
    public void refresh() throws ServiceException {
        for (String id : cache.keySet()) {
             get(id, true);
        }
    }

    /**
     * Removes all Trading Networks partner profiles from the cache.
     */
    public void clear() {
        cache.clear();
    }

    /**
     * Seeds the cache with all partner profiles from the Trading Networks database.
     *
     * @throws ServiceException
     */
    public void seed() throws ServiceException {
        List summaries = ProfileStore.getProfileSummaryList(false, false);

        if (summaries != null) {
            for (Object object : summaries) {
                if (object instanceof ProfileSummary) {
                    ProfileSummary profile = (ProfileSummary)object;
                    get(new ProfileID(profile.getProfileID()), true);
                }
            }
        }
    }

    /**
     * Returns a list of all the cached Trading Networks partner profiles.
     *
     * @return The list of all cached Trading Networks partner profiles.
     * @throws ServiceException If a database error occurs.
     */
    public IData[] list() throws ServiceException {
        return list(false);
    }

    /**
     * Returns a list of all the cached Trading Networks partner profiles, optionally seeded from the Trading Networks
     * database.
     *
     * @param seed If true, the local cache will first be seeded with all Trading Networks partner profiles from the
     *             Trading Networks database.
     * @return The list of all cached Trading Networks partner profiles.
     * @throws ServiceException If a database error occurs.
     */
    public IData[] list(boolean seed) throws ServiceException {
        if (seed) seed();

        List<IData> output = new ArrayList<IData>(cache.size());

        for (IData profile : cache.values()) {
            output.add(IDataHelper.duplicate(profile));
        }

        return output.toArray(new IData[output.size()]);
    }

    /**
     * Returns the Trading Networks partner profile associated with the given internal ID from the cache if cached, or
     * from the Trading Networks database if not cached (at which time it will be lazily cached).
     *
     * @param id The internal ID associated with the partner profile to be returned.
     * @return   The partner profile associated with the given internal ID.
     * @throws ServiceException If a database error occurs.
     */
    public IData get(String id) throws ServiceException {
        return get(id, false);
    }

    /**
     * Returns the Trading Networks partner profile associated with the given internal ID from the cache if cached, or
     * from the Trading Networks database if not cached or if a refresh is requested (at which time it will be lazily
     * cached).
     *
     * @param id      The internal ID associated with the partner profile to be returned.
     * @param refresh If true, the partner profile is first reloaded from the Trading Networks database before being
     *                returned.
     * @return        The partner profile associated with the given internal ID.
     * @throws ServiceException If a database error occurs.
     */
    public IData get(String id, boolean refresh) throws ServiceException {
        if (id == null) return null;

        IData output = null;

        if (!refresh) output = cache.get(id);

        if (refresh || output == null) {
            Profile profile = ProfileHelper.get(id);
            if (profile != null) {
                // make the cached profile a read only IData document
                output = ProfileHelper.toIData(profile);
                // cache the profile against the internal ID
                cache.put(id, output);
            }
        }

        return IDataHelper.duplicate(output);
    }

    /**
     * Returns the Trading Networks partner profile associated with the given external ID from the cache if cached, or
     * from the Trading Networks database if not cached (at which time it will be lazily cached).
     *
     * @param id   The external ID associated with the profile to be returned.
     * @param type The type of external ID specified.
     * @return     The profile associated with the given external ID, or null if no profile exists with the given
     *             external ID.
     * @throws ServiceException If a database error occurs.
     */
    public IData get(String id, String type) throws ServiceException {
        return get(id, type, false);
    }

    /**
     * Returns the Trading Networks partner profile associated with the given external ID from the cache if cached, or
     * from the Trading Networks database if not cached or if a refresh is requested (at which time it will be lazily
     * cached).
     *
     * @param id      The external ID associated with the profile to be returned.
     * @param type    The type of external ID specified.
     * @param refresh If true, the partner profile is first reloaded from the Trading Networks database before being
     *                returned.
     * @return        The profile associated with the given external ID, or null if no profile exists with the given
     *                external ID.
     * @throws ServiceException If a database error occurs.
     */
    public IData get(String id, String type, boolean refresh) throws ServiceException {
        return get(new ProfileID(id, type), refresh);
    }

    /**
     * Returns the Trading Networks partner profile associated with the given ID from the cache if cached, or
     * from the Trading Networks database if not cached (at which time it will be lazily cached).
     *
     * @param id The ID associated with the profile to be returned.
     * @return   The profile associated with the given ID, or null if no profile exists with the given ID.
     * @throws ServiceException If a database error occurs.
     */
    public IData get(ProfileID id) throws ServiceException {
        return get(id, false);
    }

    /**
     * Returns the Trading Networks partner profile associated with the given ID from the cache if cached, or
     * from the Trading Networks database if not cached or if a refresh is requested (at which time it will be lazily
     * cached).
     *
     * @param id      The ID associated with the profile to be returned.
     * @param refresh If true, the profile is first reloaded from the Trading Networks database before being returned.
     * @return        The profile associated with the given ID, or null if no profile exists with the given ID.
     * @throws ServiceException If a database error occurs.
     */
    public IData get(ProfileID id, boolean refresh) throws ServiceException {
        if (id == null) return null;

        IData output = null;

        id = id.toInternalID();
        if (id != null) {
            output = get(id.getValue(), refresh);
        }

        return output;
    }

    /**
     * Returns the Trading Networks My Enterprise profile from the cache if cached, or from the Trading Networks
     * database if not cached (at which time it will be lazily cached).
     *
     * @return The Trading Networks My Enterprise profile.
     * @throws ServiceException If a database error occurs.
     */
    public IData self() throws ServiceException {
        return self(false);
    }

    /**
     * Returns the Trading Networks My Enterprise profile from the cache if cached, or from the Trading Networks
     * database if not cached or if a refresh is requested (at which time it will be lazily cached).
     *
     * @param refresh   If true, the profile is first reloaded from the Trading Networks database before being returned.
     * @return          The Trading Networks My Enterprise profile.
     * @throws ServiceException If a database error occurs.
     */
    public IData self(boolean refresh) throws ServiceException {
        IData output = null;

        String id = ProfileStore.getMyID();
        if (id != null) {
            output = get(new ProfileID(id), refresh);
        }

        return output;
    }
}
