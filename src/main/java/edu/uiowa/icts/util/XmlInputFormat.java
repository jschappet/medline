package edu.uiowa.icts.util;

import java.io.DataInputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;


/**
* Reads records that are delimited by a specifc begin/end tag.
*/
public class XmlInputFormat extends  TextInputFormat {

  public static final String START_TAG_KEY = "xmlinput.start";
  public static final String END_TAG_KEY = "xmlinput.end";
  private static final Logger LOG = Logger.getLogger(XmlInputFormat.class);

    @Override
    public RecordReader<LongWritable,Text> createRecordReader(InputSplit is, TaskAttemptContext tac)  {
        
        
    
        return new XmlRecordReader();

    

        
    }
  public static class XmlRecordReader extends RecordReader<LongWritable,Text> {
    private  byte[] startTag;
    private  byte[] endTag;
    private  long start;
    private  long end;
    private long pos;

    private  DataInputStream fsin;
    private  DataOutputBuffer buffer = new DataOutputBuffer();
    private LongWritable key = new LongWritable();
    private Text value = new Text();
    private long recordStartPos;

    

        @Override
        public void initialize(InputSplit is, TaskAttemptContext tac) throws IOException, InterruptedException {
           //LOG.info("Start Init");
        	FileSplit fileSplit= (FileSplit) is;
            startTag = tac.getConfiguration().get(START_TAG_KEY).getBytes("utf-8");
            endTag = tac.getConfiguration().get(END_TAG_KEY).getBytes("utf-8");

            
                start = fileSplit.getStart();
                end = start + fileSplit.getLength();
                Path file = fileSplit.getPath();

//                FileSystem fs = file.getFileSystem(tac.getConfiguration());
//                fsin = fs.open(fileSplit.getPath());
//                fsin.seek(start);

                
                
                
                CompressionCodecFactory compressionCodecs = new CompressionCodecFactory(tac.getConfiguration());
                CompressionCodec codec = compressionCodecs.getCodec(file);

                FileSystem fs = file.getFileSystem(tac.getConfiguration());

                if (codec != null) {
                  LOG.info("Reading compressed file...");
                  fsin = new DataInputStream(codec.createInputStream(fs.open(file)));

                  end = Long.MAX_VALUE;
                } else {
                  LOG.info("Reading uncompressed file...");
                  FSDataInputStream fileIn = fs.open(file);

                  fileIn.seek(start);
                  fsin = fileIn;

                  end = start + fileSplit.getLength();
                }
              


            
        }
        
        
        /**
         * Read the next key, value pair.
         *
         * @return {@code true} if a key/value pair was read
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
          if (pos < end) {
            if (readUntilMatch(startTag, false)) {
              recordStartPos = pos - startTag.length;

              try {
                buffer.write(startTag);
                if (readUntilMatch(endTag, true)) {
                  key.set(recordStartPos);
                  value.set(buffer.getData(), 0, buffer.getLength());
                  return true;
                }
              } finally {
                // Because input streams of gzipped files are not seekable, we need to keep track of
                // bytes consumed ourselves.

                // This is a sanity check to make sure our internal computation of bytes consumed is
                // accurate. This should be removed later for efficiency once we confirm that this code
                // works correctly.

                if (fsin instanceof Seekable) {
                  if (pos != ((Seekable) fsin).getPos()) {
                    throw new RuntimeException("bytes consumed error!");
                  }
                }

                buffer.reset();
              }
            }
          }
          return false;
        }
        
        
//
//        @Override
//        public boolean nextKeyValue() throws IOException, InterruptedException {
//             if (fsin.getPos() < end) {
//        if (readUntilMatch(startTag, false)) {
//          try {
//            buffer.write(startTag);
//            if (readUntilMatch(endTag, true)) {
//            
//            value.set(buffer.getData(), 0, buffer.getLength());
//            key.set(fsin.getPos());
//                   return true;
//            }
//          } finally {
//            buffer.reset();
//          }
//        }
//      }
//      return false;
//        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
        }

        @Override
        public Text getCurrentValue() throws IOException, InterruptedException {
                   return value;
            
            

        }

//        @Override
//        public float getProgress() throws IOException, InterruptedException {
//            return (fsin.getPos() - start) / (float) (end - start);
//        }
        
        /**
         * The current progress of the record reader through its data.
         *
         * @return a number between 0.0 and 1.0 that is the fraction of the data read
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public float getProgress() throws IOException {
          return ((float) (pos - start)) / ((float) (end - start));
        }

        

        @Override
        public void close() throws IOException {
            fsin.close();
        }
//        private boolean readUntilMatch(byte[] match, boolean withinBlock) throws IOException {
//      int i = 0;
//      while (true) {
//        int b = fsin.read();
//        // end of file:
//        if (b == -1) return false;
//        // save to buffer:
//        if (withinBlock) buffer.write(b);
//
//        // check if we're matching:
//        if (b == match[i]) {
//          i++;
//          if (i >= match.length) return true;
//        } else i = 0;
//        // see if we've passed the stop point:
//        if (!withinBlock && i == 0 && fsin.getPos() >= end) return false;
//      }
//    }
        
        
        private boolean readUntilMatch(byte[] match, boolean withinBlock)
                throws IOException {
              int i = 0;
              while (true) {
                int b = fsin.read();
                // increment position (bytes consumed)
                pos++;

                // end of file:
                if (b == -1)
                  return false;
                // save to buffer:
                if (withinBlock)
                  buffer.write(b);

                // check if we're matching:
                if (b == match[i]) {
                  i++;
                  if (i >= match.length)
                    return true;
                } else
                  i = 0;
                // see if we've passed the stop point:
                if (!withinBlock && i == 0 && pos >= end)
                  return false;
              }
            }



  }
  
    
}