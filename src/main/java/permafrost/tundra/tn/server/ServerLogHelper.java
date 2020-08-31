package permafrost.tundra.tn.server;

import com.wm.app.b2b.server.ServiceException;
import com.wm.data.IData;
import com.wm.data.IDataCursor;
import com.wm.data.IDataFactory;
import org.apache.log4j.Level;
import permafrost.tundra.data.IDataHelper;
import permafrost.tundra.lang.ExceptionHelper;
import permafrost.tundra.server.ServiceHelper;

/**
 * Convenience methods for logging to the Integration Server server log.
 */
public class ServerLogHelper {
    /**
     * Disallow instantiation of this class.
     */
    private ServerLogHelper() {}

    /**
     * Logs the given message and context optionally prefixed by the current user and callstack.
     *
     * @param name      Logical name of the log target file to use.
     * @param level     The logging level to use.
     * @param message   The message to be logged.
     * @param context   The optional context to be logged.
     * @param addPrefix Whether to prefix log statement with logging metadata.
     */
    public static void log(String name, Level level, String message, IData context, boolean addPrefix) {
        IData pipeline = IDataFactory.create();
        IDataCursor cursor = pipeline.getCursor();

        try {
            if (level != null) IDataHelper.put(cursor, "$log.level", level.toString());
            if (message != null) IDataHelper.put(cursor, "$log.message", message);
            if (context != null) IDataHelper.put(cursor, "$log.context", context);
            IDataHelper.put(cursor, "$log.prefix?", addPrefix, String.class);
            if (name != null) IDataHelper.put(cursor, "$log.name", name);

            ServiceHelper.invoke("tundra:log", pipeline);
        } catch(ServiceException ex) {
            ExceptionHelper.raiseUnchecked(ex);
        } finally {
            cursor.destroy();
        }
    }
}