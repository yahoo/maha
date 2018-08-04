package com.yahoo.maha.data;


import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

/**
 * Created by hiral on 8/5/14.
 */
public class TestCompressors {

    @Test
    public void testLz4EncoderDecoder() throws Exception {
        Compressor.Codec codec = Compressor.Codec.LZ4;
        testStringEncoderWithCodec(codec);
        testStringBatchEncoderWithCodec(codec);
        testCompress(codec);
        testCompressToBB(codec);
        testCompressBB(codec);
    }

    @Test
    public void testLz4HCEncoderDecoder() throws Exception {
        Compressor.Codec codec = Compressor.Codec.LZ4HC;
        testStringEncoderWithCodec(codec);
        testStringBatchEncoderWithCodec(codec);
        testCompress(codec);
        testCompressToBB(codec);
        testCompressBB(codec);
    }

    @Test
    public void testGZIPEncoderDecoder() throws Exception {
        Compressor.Codec codec = Compressor.Codec.GZIP;
        testStringEncoderWithCodec(codec);
        testStringBatchEncoderWithCodec(codec);
        testCompress(codec);
        testCompressToBB(codec);
        testCompressBB(codec);
    }

    @Test
    public void testNoneEncoderDecoder() throws Exception {
        Compressor.Codec codec = Compressor.Codec.NONE;
        testStringEncoderWithCodec(codec);
        testStringBatchEncoderWithCodec(codec);
        testCompress(codec);
        testCompressToBB(codec);
        testCompressBB(codec);
    }

    private void testStringEncoderWithCodec(Compressor.Codec codec) throws Exception {
        String input = "a;sdkfja;fjojapoijfiosajf0w4r0-jf[oIj[oiJWPRUE90FVJ09AJZ-RU39-3RU-3URQ309RU09-RQUR4RUQ-RU-QR" +
                "A;FJSL;FASJ;JSD;JS;JFAW3RJ0V VURU -0EUR4-UR0-9RU09IR[Rwu023 3eiq29ei30-    ure2[U90RU90 RAU90[2093RU2";
        StringEncoder encoder = new StringEncoder(codec);
        byte[] encodedBytes = encoder.encode(input);
        System.out.println(String.format("codec=%s encodedBytes.length=%s", codec, encodedBytes.length));
        StringDecoder decoder = new StringDecoder(codec);
        String output = decoder.decode(encodedBytes);
        Assert.assertEquals(input, output);
    }

    private void testStringBatchEncoderWithCodec(Compressor.Codec codec) throws Exception {
        String input1 = "a;sdkfja;fjojapoijfiosajf0w4r0-jf[oIj[oiJWPRUE90FVJ09AJZ-RU39-3RU-3URQ309RU09-RQUR4RUQ-RU-QR" +
                "A;FJSL;FASJ;JSD;JS;JFAW3RJ0V VURU -0EUR4-UR0-9RU09IR[Rwu023 3eiq29ei30-    ure2[U90RU90 RAU90[2093RU2";
        String input2 = "dffdjfds;fd;f;jf;fdjdafskfdas;kfsak;lafdjfddfjsalkfs;ksadfk;adfskdfs;";
        String input3 = "343423342342423423423534534534534534";
        StringEventBatch.Builder builder = new StringEventBatch.Builder(3);
        builder.add(input1);
        builder.add(input2);
        builder.add(input3);
        StringEventBatch input = (StringEventBatch) builder.build();
        StringEventBatchEncoder encoder = new StringEventBatchEncoder(codec);
        byte[] encodedBytes = encoder.encode(input);
        System.out.println(String.format("codec=%s encodedBytes.length=%s", codec, encodedBytes.length));
        StringEventBatchDecoder decoder = new StringEventBatchDecoder(codec);
        StringEventBatch output = decoder.decode(encodedBytes);
        Assert.assertEquals(input.getEvents().size(), output.getEvents().size());
        Assert.assertEquals(input.getEvents().get(0), output.getEvents().get(0));
        Assert.assertEquals(input.getEvents().get(1), output.getEvents().get(1));
        Assert.assertEquals(input.getEvents().get(2), output.getEvents().get(2));
    }

    private void testCompress(Compressor.Codec codec) throws Exception {
        Compressor compressor = CompressorFactory.getCompressor(codec);
        String input = "a;sdkfja;fjojapoijfiosajf0w4r0-jf[oIj[oiJWPRUE90FVJ09AJZ-RU39-3RU-3URQ309RU09-RQUR4RUQ-RU-QR" +
                "A;FJSL;FASJ;JSD;JS;JFAW3RJ0V VURU -0EUR4-UR0-9RU09IR[Rwu023 3eiq29ei30-    ure2[U90RU90 RAU90[2093RU2";

        byte[] inputBytes = input.getBytes();
        byte[] buffer = new byte[inputBytes.length + 256];
        int compressSize = compressor.compress(inputBytes, buffer);

        byte[] compressedBytes = new byte[compressSize];
        System.arraycopy(buffer, 0, compressedBytes, 0, compressSize);

        int decompressSize = compressor.decompress(compressedBytes, buffer);
        byte[] outputBytes = new byte[decompressSize];
        System.arraycopy(buffer, 0, outputBytes, 0, decompressSize);

        String output = new String(outputBytes);

        Assert.assertEquals(input, output);
    }

    private void testCompressToBB(Compressor.Codec codec) throws Exception {
        Compressor compressor = CompressorFactory.getCompressor(codec);
        String input = "a;sdkfja;fjojapoijfiosajf0w4r0-jf[oIj[oiJWPRUE90FVJ09AJZ-RU39-3RU-3URQ309RU09-RQUR4RUQ-RU-QR" +
                "A;FJSL;FASJ;JSD;JS;JFAW3RJ0V VURU -0EUR4-UR0-9RU09IR[Rwu023 3eiq29ei30-    ure2[U90RU90 RAU90[2093RU2";

        byte[] inputBytes = input.getBytes();
        ByteBuffer buffer = ByteBuffer.allocate(inputBytes.length + 256);
        compressor.compressToBB(inputBytes, buffer);

        buffer.flip();
        int compressSize = buffer.limit();
        byte[] compressedBytes = new byte[compressSize];
        buffer.get(compressedBytes);

        buffer.clear();
        compressor.decompressToBB(compressedBytes, buffer);
        buffer.flip();
        int decompressSize = buffer.limit();
        byte[] outputBytes = new byte[decompressSize];
        buffer.get(outputBytes);

        String output = new String(outputBytes);

        Assert.assertEquals(input, output);
    }

    private void testCompressBB(Compressor.Codec codec) throws Exception {
        Compressor compressor = CompressorFactory.getCompressor(codec);
        String input = "a;sdkfja;fjojapoijfiosajf0w4r0-jf[oIj[oiJWPRUE90FVJ09AJZ-RU39-3RU-3URQ309RU09-RQUR4RUQ-RU-QR" +
                "A;FJSL;FASJ;JSD;JS;JFAW3RJ0V VURU -0EUR4-UR0-9RU09IR[Rwu023 3eiq29ei30-    ure2[U90RU90 RAU90[2093RU2";

        int inputSize = input.getBytes().length;
        ByteBuffer inputBuffer = ByteBuffer.allocate(inputSize + 256);
        inputBuffer.put(input.getBytes());
        inputBuffer.flip();
        ByteBuffer buffer = ByteBuffer.allocate(inputSize + 512);
        compressor.compressBB(inputBuffer, buffer);

        buffer.flip();
        int compressSize = buffer.limit();
        byte[] compressedBytes = new byte[compressSize];
        buffer.get(compressedBytes);

        buffer.clear();
        compressor.decompressBB(ByteBuffer.wrap(compressedBytes), buffer);
        buffer.flip();
        int decompressSize = buffer.limit();
        byte[] outputBytes = new byte[decompressSize];
        buffer.get(outputBytes);

        String output = new String(outputBytes);

        Assert.assertEquals(input, output);
    }
}
