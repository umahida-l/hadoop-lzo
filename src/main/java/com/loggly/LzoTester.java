package com.loggly;

import com.hadoop.compression.lzo.LzoCompressor;
import com.hadoop.compression.lzo.LzoIndex;
import com.hadoop.compression.lzo.LzopCodec;
import com.hadoop.compression.lzo.LzopOutputStream;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

public class LzoTester {

  public static void compressLZO(String inputFile) throws Exception {
    //Compression
    /*long startTime = System.currentTimeMillis();
    File f = new File("lzo_compression.txt.lzo");
    byte[] d = Files.readAllBytes(Paths.get("lzo_compression.txt"));
    OutputStream out = new FileOutputStream(f);
    LzoAlgorithm algorithm = LzoAlgorithm.LZO1X;
    //LzoAlgorithm algorithm = null;
    LzoCompressor compressor = LzoLibrary.getInstance().newCompressor(algorithm, LzoConstraint.COMPRESSION);
    LzoOutputStream stream = new LzopOutputStream(out, compressor, 256);
    stream.write(d);
    stream.close();
    //stream.flush();
    //out.flush();
    //out.close();
    long endTime = System.currentTimeMillis();
    System.out.println("******Compressed File*******:took " + (endTime - startTime));
    */
    //read
    /*
    InputStream in = new FileInputStream(new File("lzo_compression.txt.lzo"));
    StringWriter writer = new StringWriter();
    algorithm = LzoAlgorithm.LZO1X;
    byte[] r = new byte[d.length];
    LzoDecompressor decompressor = LzoLibrary.getInstance().newDecompressor(algorithm, null);
    InputStream ipStream = new LzoInputStream(in, decompressor);
    ipStream.read(r);
    String s = new String(r);
    System.out.println(s);
    */

    // Set up the text file reader.
    // Assumes the flat file is at filename, and the compressed version is filename.lzo
    //String IP_FILENAME = "lzo_compression.txt";
    String IP_FILENAME = "lzo_compressionab";
    IP_FILENAME = inputFile;
    File lzoOutFile = new File("output_" + IP_FILENAME + new LzopCodec().getDefaultExtension());
    File lzoIndexFile = new File(lzoOutFile.getAbsolutePath() + LzoIndex.LZO_INDEX_SUFFIX);
    if (lzoOutFile.exists()) {
      lzoOutFile.delete();
    }
    if (lzoIndexFile.exists()) {
      lzoIndexFile.delete();
    }


    //
    // First, read in the text file, and write each line to an lzop output stream.
    //

    // Set up the text file reader.
    //BufferedReader textBr = new BufferedReader(new InputStreamReader(new FileInputStream(textFile.getAbsolutePath())));
    // Set up the LZO writer..
    // TODO: Configurable buffer ?
    int lzoBufferSize = 256 * 1024;
    LzoCompressor.CompressionStrategy strategy = LzoCompressor.CompressionStrategy.LZO1X_1;
    LzoCompressor lzoCompressor = new LzoCompressor(strategy, lzoBufferSize);
    LzopOutputStream lzoOut = new LzopOutputStream(new FileOutputStream(lzoOutFile),
        new DataOutputStream(new FileOutputStream(lzoIndexFile)),
        lzoCompressor, lzoBufferSize, strategy);

    /*// Now read line by line and stream out..
    String textLine;
    while ((textLine = textBr.readLine()) != null) {
      textLine += "\n";
      byte[] bytes = textLine.getBytes();
      lzoOut.write(bytes, 0, bytes.length);
    }*/

    // Real deal
    byte[] encoded = Files.readAllBytes(Paths.get(IP_FILENAME));
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < 100; i++) {
      lzoOut.write(encoded);
      lzoOut.write(encoded);
    }
    //textBr.close();
    lzoOut.close();
    long endTime = System.currentTimeMillis();
    System.out.println("******Compression and Index File*******:took millizz" + (endTime - startTime));


    //Generating Indexes
    /*
    System.out.println("******Indexing File*******");
    Configuration c = new Configuration();
    c.set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec," +
        "org.apache.hadoop.io.compress.DefaultCodec," +
        "org.apache.hadoop.io.compress.BZip2Codec," +
        "com.hadoop.compression.lzo.LzoCodec," +
        "com.hadoop.compression.lzo.LzopCodec");
    LzoIndexer indexer = new LzoIndexer(c);
    startTime = System.currentTimeMillis();
    indexer.index(new Path("lzo_compression.txt.lzo"));
    endTime = System.currentTimeMillis();
    System.out.println("******Indexed File*******:took " + (endTime - startTime));
    /*LzoIndexer indexer = new LzoIndexer();
    indexer.createIndex(
        new FileInputStream("lzo_compression.txt.lzo"),
        new FileOutputStream("lzo_compression.txt.lzo.index"));
    */
  }

  public static void main(String[] args) {
    String fileName = null;
    if (args != null && args.length > 0) {
      fileName = args[0];
    }
    if (fileName == null) {
      fileName = "COPYING";
    }
    try {
      compressLZO(fileName);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
