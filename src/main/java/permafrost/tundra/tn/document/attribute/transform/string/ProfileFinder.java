package permafrost.tundra.tn.document.attribute.transform.string;

import com.wm.app.tn.profile.ProfileStoreException;
import permafrost.tundra.lang.ExceptionHelper;
import permafrost.tundra.tn.document.attribute.transform.Transformer;
import permafrost.tundra.tn.profile.ProfileID;

/**
 * Transforms the given Trading Networks partner profile external identities to internal identities.
 */
public class ProfileFinder extends Transformer<String> {
    /**
     * Transforms the given Trading Networks extracted document attribute values.
     *
     * @param values    The extracted document attribute values to transform.
     * @param isArray   Whether there are multiple values to be transformed.
     * @param arg       The modifying argument for the transformation, if applicable.
     * @return          The transformed document attribute values.
     */
    @Override
    public String[] transform(String[] values, boolean isArray, String arg) {
        String[] output = new String[values.length];

        try {
            for (int i = 0; i < values.length; i++) {
                ProfileID externalProfileID = new ProfileID(values[i], arg);
                ProfileID internalProfileID = externalProfileID.toInternalID();
                if (internalProfileID != null) {
                    output[i] = internalProfileID.getValue();
                }
            }
        } catch(ProfileStoreException ex) {
            ExceptionHelper.raiseUnchecked(ex);
        }

        return output;
    }
}
