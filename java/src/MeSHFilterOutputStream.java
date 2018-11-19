import org.jetbrains.annotations.NotNull;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class MeSHFilterOutputStream extends FilterOutputStream {

    private OutputStream out;
    private final Charset charset = StandardCharsets.UTF_8;

    /**
     * Creates an output stream filter built on top of the specified
     * underlying output stream.
     *
     * @param out the underlying output stream to be assigned to
     *            the field <tt>this.out</tt> for later use, or
     *            <code>null</code> if this instance is to be
     *            created without an underlying stream.
     */
    public MeSHFilterOutputStream(@NotNull OutputStream out) {
        super(out);
        this.out = out;
    }

    /**
     * This method transforms the input String as follows:
     * The input should be "Descriptor(as MeSH-URI)|Name" where MeSH URI's are
     * of the form "http://id.nlm.nih.gov/mesh/X123456". These URI's are
     * truncated to the ID (--> X123456). Names are followed by a language-tag,
     * e.g. @en, @de, etc., and only inputs with @en tag are kept (but the tag
     * is removed). Furthermore, some unnecessary quotes in the names are
     * removed.
     * The output is a String in CSV-Format "ID|Name" with delimiter '|'.
     *
     * @param input The input data to be filtered
     * @return The filtered output
     */
    private String filterOutput(@NotNull String input) {
        String output = "";
        if (input.contains("@en")) {
            // Truncate the URI, replace quotes, replace language-tag
            final String prefix = "http://id.nlm.nih.gov/mesh/";
            output = input.replace(prefix, "")
                    .replaceAll("\"", "")
                    .replaceAll("'", "")
                    .replaceAll("@en", "");
        }
        return output;
    }


    public void writeString(String s) throws IOException {
        byte[] b = this.filterOutput(s).getBytes(this.charset);
        out.write(b);
    }


//-------------------------OVERWRITTEN METHODS--------------------------

    @Override
    public void write(int b) throws IOException {
        super.write(b);
    }

    /**
     * This method should be used to filter the output before it is given to the underlying
     * OutputStream. For the filtering details see {@link #filterOutput(String)}. The input
     * byte array is decoded by {@link java.nio.charset.StandardCharsets#UTF_8}.
     *
     * @param b The input byte[]
     * @throws IOException
     */
    @Override
    public void write(@NotNull byte[] b) throws IOException {
        String s = new String(b, charset);
        String output = this.filterOutput(s);
        this.out.write(output.getBytes(charset));
    }

    @Override
    public void write(@NotNull byte[] b, int off, int len) throws IOException {
        System.out.println("b,off,len: " + (new String(b, StandardCharsets.UTF_8)) + "," + off + "," + len);
        // Copy the byte array data and write with this.write(byte[])
        byte[] bslice = new byte[len];
        for (int i = 0; i < len; i++)
            bslice[i] = b[off + i];
        this.write(b);
    }

    @Override
    public void close() throws IOException {
        this.out.close();
    }

    @Override
    public void flush() throws IOException {
        this.out.flush();
    }


}
