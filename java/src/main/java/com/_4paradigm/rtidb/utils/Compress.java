package com._4paradigm.rtidb.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.Snappy;

public class Compress {
    private final static Logger logger = LoggerFactory.getLogger(Compress.class);

    public static byte[] gunzip(byte[] data) {
        if (data == null) {
            return null;
        }
        byte[] compressed = null;
        GZIPInputStream gzip_in = null;
        ByteArrayOutputStream out_stream = new ByteArrayOutputStream();
        ByteArrayInputStream in_stream = null;
        try {
            in_stream = new ByteArrayInputStream(data);
            gzip_in = new GZIPInputStream(in_stream);
            byte[] buffer = new byte[256];
            int n;
            while ((n = gzip_in.read(buffer)) >= 0) {
                out_stream.write(buffer, 0, n);
            }
            compressed = out_stream.toByteArray();
        } catch (IOException e) {
            logger.error("gzip uncompress data error", e);
        } finally {
            if(gzip_in != null) {
                try {
                    gzip_in.close();
                } catch (IOException e) {
                    logger.error("GZIPInputStream close failed", e);
                }
            }
            if (in_stream != null) {
                try {
                    in_stream.close();
                } catch (IOException e) {
                    logger.error("ByteArrayInputStream close failed", e);
                }
            }
            try {
                out_stream.close();
            } catch (IOException e) {
                logger.error("ByteArrayOutputStream close failed", e);
            }
        }
        return compressed;
    }

    public static byte[] gzip(byte[] data) {
        if (data == null) {
            return null;
        }
        byte[] compressed = null;
        GZIPOutputStream gzip_out = null;
        ByteArrayOutputStream out_stream = new ByteArrayOutputStream();
        try {
            gzip_out = new GZIPOutputStream(out_stream);
            gzip_out.write(data);
        } catch (IOException e) {
            logger.error("gzip compress data error", e);
        } finally {
            if(gzip_out != null) {
                try {
                    gzip_out.close();
                } catch (IOException e) {
                    logger.error("GZIPOutputStream close failed", e);
                }
            }
            try {
                compressed = out_stream.toByteArray();
                out_stream.close();
            } catch (IOException e) {
                logger.error("ByteArrayOutputStream close failed", e);
            }
        }
        return compressed;
    }

    public static byte[] snappyCompress(byte[] data) {
        try {
            return Snappy.compress(data);
        } catch (IOException e) {
            logger.error("snappy compress data error", e);
            return null;
        }
    }

    public static byte[] snappyUnCompress(byte[] data) {
        try {
            return Snappy.uncompress(data);
        } catch (IOException e) {
            logger.error("snappy uncompress data error", e);
            return null;
        }
    }
}
