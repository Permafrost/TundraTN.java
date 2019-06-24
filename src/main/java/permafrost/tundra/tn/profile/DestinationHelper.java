/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 Lachlan Dowding
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

import com.wm.app.tn.profile.Destination;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import com.wm.data.IDataUtil;
import permafrost.tundra.lang.BooleanHelper;
import java.util.ArrayList;
import java.util.List;

/**
 * Collection of convenience methods for working with TN Profile Destination objects.
 */
public final class DestinationHelper {
    /**
     * Disallow instantiation of this class.
     */
    private DestinationHelper() {}

    /**
     * Returns an IData[] document list representation of the given list of destinations.
     *
     * @param destinations  The destinations to be converted to an IData[] document list representation.
     * @return              The IData[] document list representation of the given destinations.
     */
    public static IData[] toIDataArray(Iterable<Destination> destinations) {
        List<IData> output = new ArrayList<IData>();

        if (destinations != null) {
            for (Destination destination : destinations) {
                IData document = toIData(destination);
                if (document != null) output.add(document);
            }
        }

        return output.toArray(new IData[0]);
    }

    /**
     * Returns an IData document containing a child IData document for each given destination.
     *
     * @param destinations  The destinations to convert to an IData representation.
     * @return              The IData representation of the given destinations.
     */
    public static IData toIData(Iterable<Destination> destinations) {
        return toIData(destinations, null);
    }

    /**
     * Returns an IData document containing a child IData document for each given destination.
     *
     * @param destinations         The destinations to convert to an IData representation.
     * @param preferredDestination The preferred destination if specified will be associated with the Preferred Protocol
     *                             key in the resulting IData representation.
     * @return                     The IData representation of the given destinations.
     */
    public static IData toIData(Iterable<Destination> destinations, Destination preferredDestination) {
        IData output = IDataFactory.create();
        IDataCursor cursor = output.getCursor();

        if (preferredDestination != null) {
            IData document = toIData(preferredDestination);
            if (document != null) cursor.insertAfter("Preferred Protocol", document);
        }

        if (destinations != null) {
            for (Destination destination : destinations) {
                String name = getName(destination);
                IData document = toIData(destination);
                if (document != null) cursor.insertAfter(name, document);
            }
        }

        cursor.destroy();

        return output;
    }

    /**
     * Converts the given destination to an IData representation.
     *
     * @param destination   The destination to be converted.
     * @return              The IData representation of the given destination.
     */
    public static IData toIData(Destination destination) {
        if (destination == null ) return null;

        // take a copy of the destination so we can fix some field values for backwards-compatibility
        IData output = IDataFactory.create();
        IDataUtil.append(destination, output);

        IDataCursor cursor = output.getCursor();

        // fix protocol to be an actual protocol rather than the destination name for custom destinations
        String protocol = IDataUtil.getString(cursor, "B2BService");
        if (protocol != null) {
            IDataUtil.put(cursor, "Protocol", protocol);
        }

        // add name to the destination structure
        IDataUtil.put(cursor, "DestinationName", getName(destination));
        // add whether destination is the primary destination as a boolean string
        IDataUtil.put(cursor, "IsPrimary", BooleanHelper.emit(destination.isPrimary()));

        cursor.destroy();

        return output;
    }

    /**
     * Returns the destination name; supports custom destinations as well as built-in destinations.
     *
     * @param destination   The destination whose name is to be returned.
     * @return              The name of the given destination.
     */
    public static String getName(Destination destination) {
        if (destination == null) return null;

        String name = destination.getProtocolDisplayName();
        // protocol display name can be null, so use protocol when this happens
        if (name == null) name = destination.getProtocol();
        if (name == null) throw new NullPointerException("destination name must not be null");

        return name;
    }
}
