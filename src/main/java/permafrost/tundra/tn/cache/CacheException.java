/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2023 Lachlan Dowding
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

package permafrost.tundra.tn.cache;

import permafrost.tundra.lang.BaseRuntimeException;
import permafrost.tundra.lang.ExceptionHelper;
import java.util.Arrays;

/**
 * Base class for exceptions thrown by cache providers.
 */
public class CacheException extends BaseRuntimeException {
    /**
     * Constructs a new CacheException.
     */
    public CacheException() {
        this((String)null);
    }

    /**
     * Constructs a new CacheException with the given message.
     *
     * @param message A message describing why the CacheException was thrown.
     */
    public CacheException(String message) {
        super(message);
    }

    /**
     * Constructs a new CacheException with the given cause.
     *
     * @param cause The cause of this CacheException.
     */
    public CacheException(Throwable cause) {
        this(null, cause);
    }

    /**
     * Constructs a new CacheException with the given message and cause.
     *
     * @param message A message describing why the CacheException was thrown.
     * @param cause   Optional cause of this Exception.
     */
    public CacheException(String message, Throwable cause) {
        this(message, cause, (Iterable<? extends Throwable>)null);
    }

    /**
     * Constructs a new CacheException.
     *
     * @param message       A message describing why the CacheException was thrown.
     * @param cause         Optional cause of this Exception.
     * @param suppressed    Optional list of suppressed exceptions.
     */
    public CacheException(String message, Throwable cause, Throwable... suppressed) {
        this(message, cause, suppressed == null ? null : Arrays.asList(suppressed));
    }

    /**
     * Constructs a new CacheException.
     *
     * @param message       A message describing why the CacheException was thrown.
     * @param cause         Optional cause of this Exception.
     * @param suppressed    Optional list of suppressed exceptions.
     */
    public CacheException(String message, Throwable cause, Iterable<? extends Throwable> suppressed) {
        super(ExceptionHelper.normalizeMessage(message, cause, suppressed));
        if (cause != null) initCause(cause);
        suppress(suppressed);
    }
}
