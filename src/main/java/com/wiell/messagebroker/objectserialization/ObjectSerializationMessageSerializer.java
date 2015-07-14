package com.wiell.messagebroker.objectserialization;

import com.wiell.messagebroker.MessageSerializer;

import java.io.*;
import java.nio.charset.Charset;

public final class ObjectSerializationMessageSerializer implements MessageSerializer {
    public String serialize(Object message) throws SerializationFailed {
        ObjectOutputStream objectOutputStream = null;
        try {
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            objectOutputStream = new ObjectOutputStream(bo);
            objectOutputStream.writeObject(message);
            objectOutputStream.flush();
            return Base64.encodeBase64String(bo.toByteArray());
        } catch (IOException e) {
            throw new SerializationFailed("Failed to serialize " + message, e);
        } finally {
            close(objectOutputStream);
        }
    }

    public Object deserialize(String serializedMessage) throws DeserilizationFailed {
        ObjectInputStream objectInputStream = null;
        try {
            byte[] bytes = Base64.decodeBase64(serializedMessage);
            objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
            return objectInputStream.readObject();
        } catch (IOException e) {
            throw new SerializationFailed("Failed to deserialize " + serializedMessage, e);
        } catch (ClassNotFoundException e) {
            throw new SerializationFailed("Failed to deserialize " + serializedMessage, e);
        } finally {
            close(objectInputStream);
        }
    }

    private void close(Closeable closeable) {
        try {
            if (closeable != null)
                closeable.close();
        } catch (IOException ignore) {
            ignore.printStackTrace();
        }
    }

    /*
        Copy of Base64 related classes from commons-codec, included here to prevent that dependency.
        Java's Base64 only became available from Java 8, and this library should work with Java 6.
     */

    /**
     * Abstract superclass for Base-N encoders and decoders.
     * <p/>
     * <p>
     * This class is thread-safe.
     * </p>
     *
     * @version $Id: BaseNCodec.java 1634404 2014-10-26 23:06:10Z ggregory $
     */
    private abstract static class BaseNCodec {

        /**
         * Holds thread context so classes can be thread-safe.
         * <p/>
         * This class is not itself thread-safe; each thread must allocate its own copy.
         *
         * @since 1.7
         */
        static class Context {

            /**
             * Place holder for the bytes we're dealing with for our based logic.
             * Bitwise operations store and extract the encoding or decoding from this variable.
             */
            int ibitWorkArea;

            /**
             * Buffer for streaming.
             */
            byte[] buffer;

            /**
             * Position where next character should be written in the buffer.
             */
            int pos;

            /**
             * Position where next character should be read from the buffer.
             */
            int readPos;

            /**
             * Boolean flag to indicate the EOF has been reached. Once EOF has been reached, this object becomes useless,
             * and must be thrown away.
             */
            boolean eof;

            /**
             * Variable tracks how many characters have been written to the current line. Only used when encoding. We use
             * it to make sure each encoded line never goes beyond lineLength (if lineLength &gt; 0).
             */
            int currentLinePos;

            /**
             * Writes to the buffer only occur after every 3/5 reads when encoding, and every 4/8 reads when decoding. This
             * variable helps track that.
             */
            int modulus;
        }

        /**
         * EOF
         *
         * @since 1.7
         */
        static final int EOF = -1;

        /**
         * MIME chunk size per RFC 2045 section 6.8.
         * <p/>
         * <p>
         * The {@value} character limit does not count the trailing CRLF, but counts all other characters, including any
         * equal signs.
         * </p>
         *
         * @see <a href="http://www.ietf.org/rfc/rfc2045.txt">RFC 2045 section 6.8</a>
         */
        static final int MIME_CHUNK_SIZE = 76;

        static final int DEFAULT_BUFFER_RESIZE_FACTOR = 2;

        /**
         * Defines the default buffer size - currently {@value}
         * - must be large enough for at least one encoded block+separator
         */
        static final int DEFAULT_BUFFER_SIZE = 8192;

        /**
         * Mask used to extract 8 bits, used in decoding bytes
         */
        protected static final int MASK_8BITS = 0xff;

        /**
         * Byte used to pad output.
         */
        protected static final byte PAD_DEFAULT = '='; // Allow static access to default

        protected final byte pad; // instance variable just in case it needs to vary later

        /**
         * Number of bytes in each full block of unencoded data, e.g. 4 for Base64 and 5 for Base32
         */
        final int unencodedBlockSize;

        /**
         * Number of bytes in each full block of encoded data, e.g. 3 for Base64 and 8 for Base32
         */
        final int encodedBlockSize;

        /**
         * Chunksize for encoding. Not used when decoding.
         * A value of zero or less implies no chunking of the encoded data.
         * Rounded down to nearest multiple of encodedBlockSize.
         */
        protected final int lineLength;

        /**
         * Size of chunk separator. Not used unless {@link #lineLength} &gt; 0.
         */
        final int chunkSeparatorLength;

        /**
         * Note <code>lineLength</code> is rounded down to the nearest multiple of {@link #encodedBlockSize}
         * If <code>chunkSeparatorLength</code> is zero, then chunking is disabled.
         *
         * @param unencodedBlockSize   the size of an unencoded block (e.g. Base64 = 3)
         * @param encodedBlockSize     the size of an encoded block (e.g. Base64 = 4)
         * @param lineLength           if &gt; 0, use chunking with a length <code>lineLength</code>
         * @param chunkSeparatorLength the chunk separator length, if relevant
         */
        protected BaseNCodec(final int unencodedBlockSize, final int encodedBlockSize,
                             final int lineLength, final int chunkSeparatorLength) {
            this(unencodedBlockSize, encodedBlockSize, lineLength, chunkSeparatorLength, PAD_DEFAULT);
        }

        /**
         * Note <code>lineLength</code> is rounded down to the nearest multiple of {@link #encodedBlockSize}
         * If <code>chunkSeparatorLength</code> is zero, then chunking is disabled.
         *
         * @param unencodedBlockSize   the size of an unencoded block (e.g. Base64 = 3)
         * @param encodedBlockSize     the size of an encoded block (e.g. Base64 = 4)
         * @param lineLength           if &gt; 0, use chunking with a length <code>lineLength</code>
         * @param chunkSeparatorLength the chunk separator length, if relevant
         * @param pad                  byte used as padding byte.
         */
        protected BaseNCodec(final int unencodedBlockSize, final int encodedBlockSize,
                             final int lineLength, final int chunkSeparatorLength, final byte pad) {
            this.unencodedBlockSize = unencodedBlockSize;
            this.encodedBlockSize = encodedBlockSize;
            final boolean useChunking = lineLength > 0 && chunkSeparatorLength > 0;
            this.lineLength = useChunking ? (lineLength / encodedBlockSize) * encodedBlockSize : 0;
            this.chunkSeparatorLength = chunkSeparatorLength;

            this.pad = pad;
        }

        /**
         * Returns the amount of buffered data available for reading.
         *
         * @param context the context to be used
         * @return The amount of buffered data available for reading.
         */
        int available(final Context context) {  // package protected for access from I/O streams
            return context.buffer != null ? context.pos - context.readPos : 0;
        }

        /**
         * Get the default buffer size. Can be overridden.
         *
         * @return {@link #DEFAULT_BUFFER_SIZE}
         */
        protected int getDefaultBufferSize() {
            return DEFAULT_BUFFER_SIZE;
        }

        /**
         * Increases our buffer by the {@link #DEFAULT_BUFFER_RESIZE_FACTOR}.
         *
         * @param context the context to be used
         */
        byte[] resizeBuffer(final Context context) {
            if (context.buffer == null) {
                context.buffer = new byte[getDefaultBufferSize()];
                context.pos = 0;
                context.readPos = 0;
            } else {
                final byte[] b = new byte[context.buffer.length * DEFAULT_BUFFER_RESIZE_FACTOR];
                System.arraycopy(context.buffer, 0, b, 0, context.buffer.length);
                context.buffer = b;
            }
            return context.buffer;
        }

        /**
         * Ensure that the buffer has room for <code>size</code> bytes
         *
         * @param size    minimum spare space required
         * @param context the context to be used
         * @return the buffer
         */
        protected byte[] ensureBufferSize(final int size, final Context context) {
            if ((context.buffer == null) || (context.buffer.length < context.pos + size)) {
                return resizeBuffer(context);
            }
            return context.buffer;
        }

        /**
         * Extracts buffered data into the provided byte[] array, starting at position bPos, up to a maximum of bAvail
         * bytes. Returns how many bytes were actually extracted.
         * <p/>
         * Package protected for access from I/O streams.
         *
         * @param b       byte[] array to extract the buffered data into.
         * @param bPos    position in byte[] array to start extraction at.
         * @param bAvail  amount of bytes we're allowed to extract. We may extract fewer (if fewer are available).
         * @param context the context to be used
         * @return The number of bytes successfully extracted into the provided byte[] array.
         */
        int readResults(final byte[] b, final int bPos, final int bAvail, final Context context) {
            if (context.buffer != null) {
                final int len = Math.min(available(context), bAvail);
                System.arraycopy(context.buffer, context.readPos, b, bPos, len);
                context.readPos += len;
                if (context.readPos >= context.pos) {
                    context.buffer = null; // so hasData() will return false, and this method can return -1
                }
                return len;
            }
            return context.eof ? EOF : 0;
        }


        /**
         * Decodes a String containing characters in the Base-N alphabet.
         *
         * @param pArray A String containing Base-N character data
         * @return a byte array containing binary data
         */
        byte[] decode(final String pArray) {
            return decode(StringUtils.getBytesUtf8(pArray));
        }

        /**
         * Decodes a byte[] containing characters in the Base-N alphabet.
         *
         * @param pArray A byte array containing Base-N character data
         * @return a byte array containing binary data
         */
        byte[] decode(final byte[] pArray) {
            if (pArray == null || pArray.length == 0) {
                return pArray;
            }
            final Context context = new Context();
            decode(pArray, 0, pArray.length, context);
            decode(pArray, 0, EOF, context); // Notify decoder of EOF.
            final byte[] result = new byte[context.pos];
            readResults(result, 0, result.length, context);
            return result;
        }

        /**
         * Encodes a byte[] containing binary data, into a byte[] containing characters in the alphabet.
         *
         * @param pArray a byte array containing binary data
         * @return A byte array containing only the basen alphabetic character data
         */
        byte[] encode(final byte[] pArray) {
            if (pArray == null || pArray.length == 0) {
                return pArray;
            }
            final Context context = new Context();
            encode(pArray, 0, pArray.length, context);
            encode(pArray, 0, EOF, context); // Notify encoder of EOF.
            final byte[] buf = new byte[context.pos - context.readPos];
            readResults(buf, 0, buf.length, context);
            return buf;
        }

        // package protected for access from I/O streams
        abstract void encode(byte[] pArray, int i, int length, Context context);

        // package protected for access from I/O streams
        abstract void decode(byte[] pArray, int i, int length, Context context);

        /**
         * Returns whether or not the <code>octet</code> is in the current alphabet.
         * Does not allow whitespace or pad.
         *
         * @param value The value to test
         * @return <code>true</code> if the value is defined in the current alphabet, <code>false</code> otherwise.
         */
        protected abstract boolean isInAlphabet(byte value);


        /**
         * Tests a given byte array to see if it contains any characters within the alphabet or PAD.
         * <p/>
         * Intended for use in checking line-ending arrays
         *
         * @param arrayOctet byte array to test
         * @return <code>true</code> if any byte is a valid character in the alphabet or PAD; <code>false</code> otherwise
         */
        protected boolean containsAlphabetOrPad(final byte[] arrayOctet) {
            if (arrayOctet == null) {
                return false;
            }
            for (final byte element : arrayOctet) {
                if (pad == element || isInAlphabet(element)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Calculates the amount of space needed to encode the supplied array.
         *
         * @param pArray byte[] array which will later be encoded
         * @return amount of space needed to encoded the supplied array.
         * Returns a long since a max-len array will require &gt; Integer.MAX_VALUE
         */
        long getEncodedLength(final byte[] pArray) {
            // Calculate non-chunked size - rounded up to allow for padding
            // cast to long is needed to avoid possibility of overflow
            long len = ((pArray.length + unencodedBlockSize - 1) / unencodedBlockSize) * (long) encodedBlockSize;
            if (lineLength > 0) { // We're using chunking
                // Round up to nearest multiple
                len += ((len + lineLength - 1) / lineLength) * chunkSeparatorLength;
            }
            return len;
        }
    }

    /**
     * Provides Base64 encoding and decoding as defined by <a href="http://www.ietf.org/rfc/rfc2045.txt">RFC 2045</a>.
     * <p/>
     * <p>
     * This class implements section <cite>6.8. Base64 Content-Transfer-Encoding</cite> from RFC 2045 <cite>Multipurpose
     * Internet Mail Extensions (MIME) Part One: Format of Internet Message Bodies</cite> by Freed and Borenstein.
     * </p>
     * <p>
     * The class can be parameterized in the following manner with various constructors:
     * </p>
     * <ul>
     * <li>URL-safe mode: Default off.</li>
     * <li>Line length: Default 76. Line length that aren't multiples of 4 will still essentially end up being multiples of
     * 4 in the encoded data.
     * <li>Line separator: Default is CRLF ("\r\n")</li>
     * </ul>
     * <p>
     * The URL-safe parameter is only applied to encode operations. Decoding seamlessly handles both modes.
     * </p>
     * <p>
     * Since this class operates directly on byte streams, and not character streams, it is hard-coded to only
     * encode/decode character encodings which are compatible with the lower 127 ASCII chart (ISO-8859-1, Windows-1252,
     * UTF-8, etc).
     * </p>
     * <p>
     * This class is thread-safe.
     * </p>
     *
     * @version $Id: Base64.java 1635986 2014-11-01 16:27:52Z tn $
     * @see <a href="http://www.ietf.org/rfc/rfc2045.txt">RFC 2045</a>
     * @since 1.0
     */
    private static class Base64 extends BaseNCodec {

        /**
         * BASE32 characters are 6 bits in length.
         * They are formed by taking a block of 3 octets to form a 24-bit string,
         * which is converted into 4 BASE64 characters.
         */
        static final int BITS_PER_ENCODED_BYTE = 6;
        static final int BYTES_PER_UNENCODED_BLOCK = 3;
        static final int BYTES_PER_ENCODED_BLOCK = 4;

        /**
         * Chunk separator per RFC 2045 section 2.1.
         * <p/>
         * <p>
         * N.B. The next major release may break compatibility and make this field private.
         * </p>
         *
         * @see <a href="http://www.ietf.org/rfc/rfc2045.txt">RFC 2045 section 2.1</a>
         */
        static final byte[] CHUNK_SEPARATOR = {'\r', '\n'};

        /**
         * This array is a lookup table that translates 6-bit positive integer index values into their "Base64 Alphabet"
         * equivalents as specified in Table 1 of RFC 2045.
         * <p/>
         * Thanks to "commons" project in ws.apache.org for this code.
         * http://svn.apache.org/repos/asf/webservices/commons/trunk/modules/util/
         */
        static final byte[] STANDARD_ENCODE_TABLE = {
                'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
                'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
                'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'
        };

        /**
         * This is a copy of the STANDARD_ENCODE_TABLE above, but with + and /
         * changed to - and _ to make the encoded Base64 results more URL-SAFE.
         * This table is only used when the Base64's mode is set to URL-SAFE.
         */
        static final byte[] URL_SAFE_ENCODE_TABLE = {
                'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
                'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
                'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '-', '_'
        };

        /**
         * This array is a lookup table that translates Unicode characters drawn from the "Base64 Alphabet" (as specified
         * in Table 1 of RFC 2045) into their 6-bit positive integer equivalents. Characters that are not in the Base64
         * alphabet but fall within the bounds of the array are translated to -1.
         * <p/>
         * Note: '+' and '-' both decode to 62. '/' and '_' both decode to 63. This means decoder seamlessly handles both
         * URL_SAFE and STANDARD base64. (The encoder, on the other hand, needs to know ahead of time what to emit).
         * <p/>
         * Thanks to "commons" project in ws.apache.org for this code.
         * http://svn.apache.org/repos/asf/webservices/commons/trunk/modules/util/
         */
        static final byte[] DECODE_TABLE = {
                -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
                -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, 62, -1, 63, 52, 53, 54,
                55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3, 4,
                5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
                24, 25, -1, -1, -1, -1, 63, -1, 26, 27, 28, 29, 30, 31, 32, 33, 34,
                35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51
        };

        /**
         * Base64 uses 6-bit fields.
         */
        /**
         * Mask used to extract 6 bits, used when encoding
         */
        static final int MASK_6BITS = 0x3f;

        // The static final fields above are used for the original static byte[] methods on Base64.
        // The member fields below are used with the new streaming approach, which requires
        // some state be preserved between calls of encode() and decode().

        /**
         * Encode table to use: either STANDARD or URL_SAFE. Note: the DECODE_TABLE above remains static because it is able
         * to decode both STANDARD and URL_SAFE streams, but the encodeTable must be a member variable so we can switch
         * between the two modes.
         */
        final byte[] encodeTable;

        // Only one decode table currently; keep for consistency with Base32 code
        final byte[] decodeTable = DECODE_TABLE;

        /**
         * Line separator for encoding. Not used when decoding. Only used if lineLength &gt; 0.
         */
        final byte[] lineSeparator;

        /**
         * Convenience variable to help us determine when our buffer is going to run out of room and needs resizing.
         * <code>decodeSize = 3 + lineSeparator.length;</code>
         */
        final int decodeSize;

        /**
         * Convenience variable to help us determine when our buffer is going to run out of room and needs resizing.
         * <code>encodeSize = 4 + lineSeparator.length;</code>
         */
        final int encodeSize;

        /**
         * Creates a Base64 codec used for decoding (all modes) and encoding in URL-unsafe mode.
         * <p>
         * When encoding the line length is 0 (no chunking), and the encoding table is STANDARD_ENCODE_TABLE.
         * </p>
         * <p/>
         * <p>
         * When decoding all variants are supported.
         * </p>
         */
        Base64() {
            this(0);
        }

        /**
         * Creates a Base64 codec used for decoding (all modes) and encoding in the given URL-safe mode.
         * <p>
         * When encoding the line length is 76, the line separator is CRLF, and the encoding table is STANDARD_ENCODE_TABLE.
         * </p>
         * <p/>
         * <p>
         * When decoding all variants are supported.
         * </p>
         *
         * @param urlSafe if <code>true</code>, URL-safe encoding is used. In most cases this should be set to
         *                <code>false</code>.
         * @since 1.4
         */
        Base64(final boolean urlSafe) {
            this(MIME_CHUNK_SIZE, CHUNK_SEPARATOR, urlSafe);
        }

        /**
         * Creates a Base64 codec used for decoding (all modes) and encoding in URL-unsafe mode.
         * <p>
         * When encoding the line length is given in the constructor, the line separator is CRLF, and the encoding table is
         * STANDARD_ENCODE_TABLE.
         * </p>
         * <p>
         * Line lengths that aren't multiples of 4 will still essentially end up being multiples of 4 in the encoded data.
         * </p>
         * <p>
         * When decoding all variants are supported.
         * </p>
         *
         * @param lineLength Each line of encoded data will be at most of the given length (rounded down to nearest multiple of
         *                   4). If lineLength &lt;= 0, then the output will not be divided into lines (chunks). Ignored when
         *                   decoding.
         * @since 1.4
         */
        Base64(final int lineLength) {
            this(lineLength, CHUNK_SEPARATOR);
        }

        /**
         * Creates a Base64 codec used for decoding (all modes) and encoding in URL-unsafe mode.
         * <p>
         * When encoding the line length and line separator are given in the constructor, and the encoding table is
         * STANDARD_ENCODE_TABLE.
         * </p>
         * <p>
         * Line lengths that aren't multiples of 4 will still essentially end up being multiples of 4 in the encoded data.
         * </p>
         * <p>
         * When decoding all variants are supported.
         * </p>
         *
         * @param lineLength    Each line of encoded data will be at most of the given length (rounded down to nearest multiple of
         *                      4). If lineLength &lt;= 0, then the output will not be divided into lines (chunks). Ignored when
         *                      decoding.
         * @param lineSeparator Each line of encoded data will end with this sequence of bytes.
         * @throws IllegalArgumentException Thrown when the provided lineSeparator included some base64 characters.
         * @since 1.4
         */
        Base64(final int lineLength, final byte[] lineSeparator) {
            this(lineLength, lineSeparator, false);
        }

        /**
         * Creates a Base64 codec used for decoding (all modes) and encoding in URL-unsafe mode.
         * <p>
         * When encoding the line length and line separator are given in the constructor, and the encoding table is
         * STANDARD_ENCODE_TABLE.
         * </p>
         * <p>
         * Line lengths that aren't multiples of 4 will still essentially end up being multiples of 4 in the encoded data.
         * </p>
         * <p>
         * When decoding all variants are supported.
         * </p>
         *
         * @param lineLength    Each line of encoded data will be at most of the given length (rounded down to nearest multiple of
         *                      4). If lineLength &lt;= 0, then the output will not be divided into lines (chunks). Ignored when
         *                      decoding.
         * @param lineSeparator Each line of encoded data will end with this sequence of bytes.
         * @param urlSafe       Instead of emitting '+' and '/' we emit '-' and '_' respectively. urlSafe is only applied to encode
         *                      operations. Decoding seamlessly handles both modes.
         *                      <b>Note: no padding is added when using the URL-safe alphabet.</b>
         * @throws IllegalArgumentException The provided lineSeparator included some base64 characters. That's not going to work!
         * @since 1.4
         */
        Base64(final int lineLength, final byte[] lineSeparator, final boolean urlSafe) {
            super(BYTES_PER_UNENCODED_BLOCK, BYTES_PER_ENCODED_BLOCK,
                    lineLength,
                    lineSeparator == null ? 0 : lineSeparator.length);
            // TODO could be simplified if there is no requirement to reject invalid line sep when length <=0
            // @see test case Base64Test.testConstructors()
            if (lineSeparator != null) {
                if (containsAlphabetOrPad(lineSeparator)) {
                    final String sep = StringUtils.newStringUtf8(lineSeparator);
                    throw new IllegalArgumentException("lineSeparator must not contain base64 characters: [" + sep + "]");
                }
                if (lineLength > 0) { // null line-sep forces no chunking rather than throwing IAE
                    this.encodeSize = BYTES_PER_ENCODED_BLOCK + lineSeparator.length;
                    this.lineSeparator = new byte[lineSeparator.length];
                    System.arraycopy(lineSeparator, 0, this.lineSeparator, 0, lineSeparator.length);
                } else {
                    this.encodeSize = BYTES_PER_ENCODED_BLOCK;
                    this.lineSeparator = null;
                }
            } else {
                this.encodeSize = BYTES_PER_ENCODED_BLOCK;
                this.lineSeparator = null;
            }
            this.decodeSize = this.encodeSize - 1;
            this.encodeTable = urlSafe ? URL_SAFE_ENCODE_TABLE : STANDARD_ENCODE_TABLE;
        }


        /**
         * <p>
         * Encodes all of the provided data, starting at inPos, for inAvail bytes. Must be called at least twice: once with
         * the data to encode, and once with inAvail set to "-1" to alert encoder that EOF has been reached, to flush last
         * remaining bytes (if not multiple of 3).
         * </p>
         * <p><b>Note: no padding is added when encoding using the URL-safe alphabet.</b></p>
         * <p>
         * Thanks to "commons" project in ws.apache.org for the bitwise operations, and general approach.
         * http://svn.apache.org/repos/asf/webservices/commons/trunk/modules/util/
         * </p>
         *
         * @param in      byte[] array of binary data to base64 encode.
         * @param inPos   Position to start reading data from.
         * @param inAvail Amount of bytes available from input for encoding.
         * @param context the context to be used
         */
        @Override
        void encode(final byte[] in, int inPos, final int inAvail, final Context context) {
            if (context.eof) {
                return;
            }
            // inAvail < 0 is how we're informed of EOF in the underlying data we're
            // encoding.
            if (inAvail < 0) {
                context.eof = true;
                if (0 == context.modulus && lineLength == 0) {
                    return; // no leftovers to process and not using chunking
                }
                final byte[] buffer = ensureBufferSize(encodeSize, context);
                final int savedPos = context.pos;
                switch (context.modulus) { // 0-2
                    case 0: // nothing to do here
                        break;
                    case 1: // 8 bits = 6 + 2
                        // top 6 bits:
                        buffer[context.pos++] = encodeTable[(context.ibitWorkArea >> 2) & MASK_6BITS];
                        // remaining 2:
                        buffer[context.pos++] = encodeTable[(context.ibitWorkArea << 4) & MASK_6BITS];
                        // URL-SAFE skips the padding to further reduce size.
                        if (encodeTable == STANDARD_ENCODE_TABLE) {
                            buffer[context.pos++] = pad;
                            buffer[context.pos++] = pad;
                        }
                        break;

                    case 2: // 16 bits = 6 + 6 + 4
                        buffer[context.pos++] = encodeTable[(context.ibitWorkArea >> 10) & MASK_6BITS];
                        buffer[context.pos++] = encodeTable[(context.ibitWorkArea >> 4) & MASK_6BITS];
                        buffer[context.pos++] = encodeTable[(context.ibitWorkArea << 2) & MASK_6BITS];
                        // URL-SAFE skips the padding to further reduce size.
                        if (encodeTable == STANDARD_ENCODE_TABLE) {
                            buffer[context.pos++] = pad;
                        }
                        break;
                    default:
                        throw new IllegalStateException("Impossible modulus " + context.modulus);
                }
                context.currentLinePos += context.pos - savedPos; // keep track of current line position
                // if currentPos == 0 we are at the start of a line, so don't add CRLF
                if (lineLength > 0 && context.currentLinePos > 0) {
                    System.arraycopy(lineSeparator, 0, buffer, context.pos, lineSeparator.length);
                    context.pos += lineSeparator.length;
                }
            } else {
                for (int i = 0; i < inAvail; i++) {
                    final byte[] buffer = ensureBufferSize(encodeSize, context);
                    context.modulus = (context.modulus + 1) % BYTES_PER_UNENCODED_BLOCK;
                    int b = in[inPos++];
                    if (b < 0) {
                        b += 256;
                    }
                    context.ibitWorkArea = (context.ibitWorkArea << 8) + b; //  BITS_PER_BYTE
                    if (0 == context.modulus) { // 3 bytes = 24 bits = 4 * 6 bits to extract
                        buffer[context.pos++] = encodeTable[(context.ibitWorkArea >> 18) & MASK_6BITS];
                        buffer[context.pos++] = encodeTable[(context.ibitWorkArea >> 12) & MASK_6BITS];
                        buffer[context.pos++] = encodeTable[(context.ibitWorkArea >> 6) & MASK_6BITS];
                        buffer[context.pos++] = encodeTable[context.ibitWorkArea & MASK_6BITS];
                        context.currentLinePos += BYTES_PER_ENCODED_BLOCK;
                        if (lineLength > 0 && lineLength <= context.currentLinePos) {
                            System.arraycopy(lineSeparator, 0, buffer, context.pos, lineSeparator.length);
                            context.pos += lineSeparator.length;
                            context.currentLinePos = 0;
                        }
                    }
                }
            }
        }

        /**
         * <p>
         * Decodes all of the provided data, starting at inPos, for inAvail bytes. Should be called at least twice: once
         * with the data to decode, and once with inAvail set to "-1" to alert decoder that EOF has been reached. The "-1"
         * call is not necessary when decoding, but it doesn't hurt, either.
         * </p>
         * <p>
         * Ignores all non-base64 characters. This is how chunked (e.g. 76 character) data is handled, since CR and LF are
         * silently ignored, but has implications for other bytes, too. This method subscribes to the garbage-in,
         * garbage-out philosophy: it will not check the provided data for validity.
         * </p>
         * <p>
         * Thanks to "commons" project in ws.apache.org for the bitwise operations, and general approach.
         * http://svn.apache.org/repos/asf/webservices/commons/trunk/modules/util/
         * </p>
         *
         * @param in      byte[] array of ascii data to base64 decode.
         * @param inPos   Position to start reading data from.
         * @param inAvail Amount of bytes available from input for encoding.
         * @param context the context to be used
         */
        @Override
        void decode(final byte[] in, int inPos, final int inAvail, final Context context) {
            if (context.eof) {
                return;
            }
            if (inAvail < 0) {
                context.eof = true;
            }
            for (int i = 0; i < inAvail; i++) {
                final byte[] buffer = ensureBufferSize(decodeSize, context);
                final byte b = in[inPos++];
                if (b == pad) {
                    // We're done.
                    context.eof = true;
                    break;
                } else {
                    if (b >= 0 && b < DECODE_TABLE.length) {
                        final int result = DECODE_TABLE[b];
                        if (result >= 0) {
                            context.modulus = (context.modulus + 1) % BYTES_PER_ENCODED_BLOCK;
                            context.ibitWorkArea = (context.ibitWorkArea << BITS_PER_ENCODED_BYTE) + result;
                            if (context.modulus == 0) {
                                buffer[context.pos++] = (byte) ((context.ibitWorkArea >> 16) & MASK_8BITS);
                                buffer[context.pos++] = (byte) ((context.ibitWorkArea >> 8) & MASK_8BITS);
                                buffer[context.pos++] = (byte) (context.ibitWorkArea & MASK_8BITS);
                            }
                        }
                    }
                }
            }

            // Two forms of EOF as far as base64 decoder is concerned: actual
            // EOF (-1) and first time '=' character is encountered in stream.
            // This approach makes the '=' padding characters completely optional.
            if (context.eof && context.modulus != 0) {
                final byte[] buffer = ensureBufferSize(decodeSize, context);

                // We have some spare bits remaining
                // Output all whole multiples of 8 bits and ignore the rest
                switch (context.modulus) {
                    //              case 0 : // impossible, as excluded above
                    case 1: // 6 bits - ignore entirely
                        // TODO not currently tested; perhaps it is impossible?
                        break;
                    case 2: // 12 bits = 8 + 4
                        context.ibitWorkArea = context.ibitWorkArea >> 4; // dump the extra 4 bits
                        buffer[context.pos++] = (byte) ((context.ibitWorkArea) & MASK_8BITS);
                        break;
                    case 3: // 18 bits = 8 + 8 + 2
                        context.ibitWorkArea = context.ibitWorkArea >> 2; // dump 2 bits
                        buffer[context.pos++] = (byte) ((context.ibitWorkArea >> 8) & MASK_8BITS);
                        buffer[context.pos++] = (byte) ((context.ibitWorkArea) & MASK_8BITS);
                        break;
                    default:
                        throw new IllegalStateException("Impossible modulus " + context.modulus);
                }
            }
        }


        /**
         * Encodes binary data using the base64 algorithm but does not chunk the output.
         * <p/>
         * NOTE:  We changed the behaviour of this method from multi-line chunking (commons-codec-1.4) to
         * single-line non-chunking (commons-codec-1.5).
         *
         * @param binaryData binary data to encode
         * @return String containing Base64 characters.
         * @since 1.4 (NOTE:  1.4 chunked the output, whereas 1.5 does not).
         */
        static String encodeBase64String(final byte[] binaryData) {
            return StringUtils.newStringUtf8(encodeBase64(binaryData, false));
        }


        /**
         * Encodes binary data using the base64 algorithm, optionally chunking the output into 76 character blocks.
         *
         * @param binaryData Array containing binary data to encode.
         * @param isChunked  if <code>true</code> this encoder will chunk the base64 output into 76 character blocks
         * @return Base64-encoded data.
         * @throws IllegalArgumentException Thrown when the input array needs an output array bigger than {@link Integer#MAX_VALUE}
         */
        static byte[] encodeBase64(final byte[] binaryData, final boolean isChunked) {
            return encodeBase64(binaryData, isChunked, false);
        }

        /**
         * Encodes binary data using the base64 algorithm, optionally chunking the output into 76 character blocks.
         *
         * @param binaryData Array containing binary data to encode.
         * @param isChunked  if <code>true</code> this encoder will chunk the base64 output into 76 character blocks
         * @param urlSafe    if <code>true</code> this encoder will emit - and _ instead of the usual + and / characters.
         *                   <b>Note: no padding is added when encoding using the URL-safe alphabet.</b>
         * @return Base64-encoded data.
         * @throws IllegalArgumentException Thrown when the input array needs an output array bigger than {@link Integer#MAX_VALUE}
         * @since 1.4
         */
        static byte[] encodeBase64(final byte[] binaryData, final boolean isChunked, final boolean urlSafe) {
            return encodeBase64(binaryData, isChunked, urlSafe, Integer.MAX_VALUE);
        }

        /**
         * Encodes binary data using the base64 algorithm, optionally chunking the output into 76 character blocks.
         *
         * @param binaryData    Array containing binary data to encode.
         * @param isChunked     if <code>true</code> this encoder will chunk the base64 output into 76 character blocks
         * @param urlSafe       if <code>true</code> this encoder will emit - and _ instead of the usual + and / characters.
         *                      <b>Note: no padding is added when encoding using the URL-safe alphabet.</b>
         * @param maxResultSize The maximum result size to accept.
         * @return Base64-encoded data.
         * @throws IllegalArgumentException Thrown when the input array needs an output array bigger than maxResultSize
         * @since 1.4
         */
        static byte[] encodeBase64(final byte[] binaryData, final boolean isChunked,
                                   final boolean urlSafe, final int maxResultSize) {
            if (binaryData == null || binaryData.length == 0) {
                return binaryData;
            }

            // Create this so can use the super-class method
            // Also ensures that the same roundings are performed by the ctor and the code
            final Base64 b64 = isChunked ? new Base64(urlSafe) : new Base64(0, CHUNK_SEPARATOR, urlSafe);
            final long len = b64.getEncodedLength(binaryData);
            if (len > maxResultSize) {
                throw new IllegalArgumentException("Input array too big, the output array would be bigger (" +
                        len +
                        ") than the specified maximum size of " +
                        maxResultSize);
            }

            return b64.encode(binaryData);
        }

        /**
         * Decodes a Base64 String into octets.
         * <p>
         * <b>Note:</b> this method seamlessly handles data encoded in URL-safe or normal mode.
         * </p>
         *
         * @param base64String String containing Base64 data
         * @return Array containing decoded data.
         * @since 1.4
         */
        static byte[] decodeBase64(final String base64String) {
            return new Base64().decode(base64String);
        }


        /**
         * Returns whether or not the <code>octet</code> is in the Base64 alphabet.
         *
         * @param octet The value to test
         * @return <code>true</code> if the value is defined in the the Base64 alphabet <code>false</code> otherwise.
         */
        @Override
        protected boolean isInAlphabet(final byte octet) {
            return octet >= 0 && octet < decodeTable.length && decodeTable[octet] != -1;
        }

    }

    /**
     * Converts String to and from bytes using the encodings required by the Java specification. These encodings are
     * specified in <a href="http://download.oracle.com/javase/6/docs/api/java/nio/charset/Charset.html">
     * Standard charsets</a>.
     * <p/>
     * <p>This class is immutable and thread-safe.</p>
     *
     * @version $Id: StringUtils.java 1634456 2014-10-27 05:26:56Z ggregory $
     * @see <a href="http://download.oracle.com/javase/6/docs/api/java/nio/charset/Charset.html">Standard charsets</a>
     * @since 1.4
     */
    private static class StringUtils {


        /**
         * Calls {@link String#getBytes(Charset)}
         *
         * @param string  The string to encode (if null, return null).
         * @param charset The {@link Charset} to encode the <code>String</code>
         * @return the encoded bytes
         */
        static byte[] getBytes(final String string, final Charset charset) {
            if (string == null) {
                return null;
            }
            return string.getBytes(charset);
        }

        /**
         * Encodes the given string into a sequence of bytes using the UTF-8 charset, storing the result into a new byte
         * array.
         *
         * @param string the String to encode, may be <code>null</code>
         * @return encoded bytes, or <code>null</code> if the input string was <code>null</code>
         * @throws NullPointerException Thrown if UTF_8 is not initialized, which should never happen since it is
         *                              required by the Java platform specification.
         * @see <a href="http://download.oracle.com/javase/6/docs/api/java/nio/charset/Charset.html">Standard charsets</a>
         * @since As of 1.7, throws {@link NullPointerException} instead of UnsupportedEncodingException
         */
        static byte[] getBytesUtf8(final String string) {
            return getBytes(string, Charset.forName("UTF-8"));
        }

        /**
         * Constructs a new <code>String</code> by decoding the specified array of bytes using the given charset.
         *
         * @param bytes   The bytes to be decoded into characters
         * @param charset The {@link Charset} to encode the <code>String</code>
         * @return A new <code>String</code> decoded from the specified array of bytes using the given charset,
         * or <code>null</code> if the input byte array was <code>null</code>.
         * @throws NullPointerException Thrown if UTF_8 is not initialized, which should never happen since it is
         *                              required by the Java platform specification.
         */
        static String newString(final byte[] bytes, final Charset charset) {
            return bytes == null ? null : new String(bytes, charset);
        }

        /**
         * Constructs a new <code>String</code> by decoding the specified array of bytes using the UTF-8 charset.
         *
         * @param bytes The bytes to be decoded into characters
         * @return A new <code>String</code> decoded from the specified array of bytes using the UTF-8 charset,
         * or <code>null</code> if the input byte array was <code>null</code>.
         * @throws NullPointerException Thrown if UTF_8 is not initialized, which should never happen since it is
         *                              required by the Java platform specification.
         * @since As of 1.7, throws {@link NullPointerException} instead of UnsupportedEncodingException
         */
        static String newStringUtf8(final byte[] bytes) {
            return newString(bytes, Charset.forName("UTF-8"));
        }

    }
}
