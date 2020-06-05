/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Lachlan Dowding
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

package permafrost.tundra.tn.document.attribute.transform.number;

import com.wm.app.b2b.server.ServiceException;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.data.IDataYAMLParser;
import permafrost.tundra.lang.ExceptionHelper;
import permafrost.tundra.time.DateTimeHelper;
import permafrost.tundra.time.DurationHelper;
import permafrost.tundra.tn.document.attribute.transform.Transformer;
import permafrost.tundra.tn.route.CallableRoute;
import javax.xml.datatype.Duration;
import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Returns a message priority based on how imminent the given extracted datetime/s are to current time.
 */
public class ImminentPrioritizer extends Transformer<Double> {
    /**
     * Regular expression pattern used to parse a duration range such as: -P1D..P1D
     */
    protected static final Pattern IMMINENT_RANGE_PATTERN = Pattern.compile("^([^\\.]+)\\.\\.([^\\.]+)$");
    /**
     * Default duration range start.
     */
    protected static final Duration DEFAULT_IMMINENT_RANGE_START = DurationHelper.parse("-PT1H");
    /**
     * Default duration range end.
     */
    protected static final Duration DEFAULT_IMMINENT_RANGE_END = DurationHelper.parse("P7D");
    /**
     * Transforms the given Trading Networks extracted document attribute values.
     *
     * @param values    The extracted document attribute values to transform.
     * @param isArray   Whether there are multiple values to be transformed.
     * @param arg       The modifying argument for the transformation, if applicable.
     * @return          The transformed document attribute values.
     */
    @Override
    public Double[] transform(String[] values, boolean isArray, String arg) {
        Double[] output = new Double[1];

        try {
            IDataYAMLParser parser = new IDataYAMLParser();
            IData args = parser.parse(arg == null ? "" : arg);
            IDataCursor cursor = args.getCursor();

            try {
                String pattern = IDataHelper.get(cursor, "pattern", String.class);
                String range = IDataHelper.get(cursor, "range", String.class);
                double priorityFloor = Math.floor(IDataHelper.getOrDefault(cursor, "priority", Double.class, CallableRoute.DEFAULT_MESSAGE_PRIORITY));

                double priorityRange = 1.0d;
                double priorityCeiling = priorityFloor + priorityRange;
                double priority = priorityFloor;

                Duration rangeStart = null, rangeEnd = null;
                if (range != null) {
                    Matcher matcher = IMMINENT_RANGE_PATTERN.matcher(range);
                    if (matcher.matches()) {
                        rangeStart = DurationHelper.parse(matcher.group(1));
                        rangeEnd = DurationHelper.parse(matcher.group(2));
                    }
                }
                if (rangeStart == null) rangeStart = DEFAULT_IMMINENT_RANGE_START;
                if (rangeEnd == null) rangeEnd = DEFAULT_IMMINENT_RANGE_END;

                Calendar now = DateTimeHelper.now();
                Calendar startTime = DateTimeHelper.add(now, rangeStart);
                Calendar endTime = DateTimeHelper.add(now, rangeEnd);

                boolean ascending = startTime.compareTo(endTime) <= 0;
                if (!ascending) {
                    // reorder start and end time to be ascending
                    Calendar tempTime = startTime;
                    startTime = endTime;
                    endTime = tempTime;
                }

                Duration rangeDuration = DateTimeHelper.duration(startTime, endTime);
                long rangeMilliseconds = rangeDuration.getTimeInMillis(now);

                if (rangeMilliseconds > 0.0d) {
                    double ulp = priorityRange / (rangeMilliseconds * 1.0d);
                    if (values != null) {
                        Set<String> valueSet = new TreeSet<String>(Arrays.asList(values));
                        for (String value : valueSet) {
                            Calendar datetime = DateTimeHelper.parse(value, pattern);
                            if (datetime != null) {
                                if (datetime.compareTo(startTime) >= 0 && datetime.compareTo(endTime) <= 0) {
                                    long imminence = DateTimeHelper.duration(startTime, datetime).getTimeInMillis(now);

                                    double newPriority;
                                    if (ascending) {
                                        newPriority = priorityCeiling - (imminence * ulp);
                                    } else {
                                        newPriority = priorityFloor + (imminence * ulp);
                                    }

                                    if (newPriority > priority) priority = newPriority;
                                }
                            }
                        }
                    }
                }
                output[0] = Math.min(Math.max(priority, priorityFloor), priorityCeiling);
            } finally {
                cursor.destroy();
            }
        } catch(IOException ex) {
            ExceptionHelper.raiseUnchecked(ex);
        } catch(ServiceException ex) {
            ExceptionHelper.raiseUnchecked(ex);
        }

        return output;
    }
}
