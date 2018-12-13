package com.yahoo.ycsb.db.tikv;

import com.yahoo.ycsb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.raw.RawKVClient;
import shade.com.google.protobuf.ByteString;

import javax.annotation.concurrent.GuardedBy;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * TiKV binding for <a href="http://tikv.org/">TiKV</a>.
 *
 * See {@code tikv/README.md} for details.
 */
public class TiKVClient extends DB {

  private static final Logger LOGGER = LoggerFactory.getLogger(TiKVClient.class);

  @GuardedBy("TiKVClient.class") private static RawKVClient tikv = null;

  @Override
  public void init() throws DBException {
    synchronized(TiKVClient.class) {
      if(tikv == null) {

        try {
          tikv = initKVRawClient();
        } catch (final IOException e) {
          throw new DBException(e);
        }
      }
    }
  }

  /**
   * Initializes and opens the TiKV raw database.
   *
   * Should only be called with a {@code synchronized(TiKVClient.class)` block}.
   *
   * @return The initialized and open TiKV instance.
   */
  private RawKVClient initKVRawClient() throws IOException {
    return RawKVClient.create();
  }

  @Override
  public void cleanup() throws DBException {
    super.cleanup();

    synchronized (TiKVClient.class) {
      try {
        ((Closeable) tikv).close();
      } catch (final Exception e) {
        throw new DBException(e);
      }
    }
  }

  private ByteString getRowKey(final String table, final String key) {
    return ByteString.copyFromUtf8(String.format("%s:%s", table, key));
  }

  @Override
  public Status read(final String table, final String key, final Set<String> fields,
                     final Map<String, ByteIterator> result) {
    try {
      final ByteString values = tikv.get(ByteString.copyFrom(key.getBytes(UTF_8)));
      if(values == null) {
        return Status.NOT_FOUND;
      }
      deserializeValues(values, fields, result);
      return Status.OK;
    } catch(final Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    try {
      List<Kvrpcpb.KvPair> pairs = tikv.scan(ByteString.copyFrom(startkey.getBytes(UTF_8)), recordcount);
      for (Kvrpcpb.KvPair pair: pairs) {
        final HashMap<String, ByteIterator> values = new HashMap<>();
        deserializeValues(pair.getValue(), fields, values);
        result.add(values);
      }
      return Status.OK;
    } catch(final Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    //TODO(AR) consider if this would be faster with merge operator

    try {
      final Map<String, ByteIterator> result = new HashMap<>();
      final ByteString currentValues = tikv.get(getRowKey(table, key));
      if(currentValues == null) {
        return Status.NOT_FOUND;
      }
      deserializeValues(currentValues, null, result);

      //update
      result.putAll(values);

      //store
      tikv.put(getRowKey(table, key), serializeValues(result));

      return Status.OK;

    } catch(final Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      tikv.put(getRowKey(table, key), serializeValues(values));

      return Status.OK;
    } catch(final Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(final String table, final String key) {
    try {
      tikv.delete(getRowKey(table, key));

      return Status.OK;
    } catch(final Exception e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  private Map<String, ByteIterator> deserializeValues(final ByteString bsValues, final Set<String> fields,
                                                      final Map<String, ByteIterator> result) {
    final ByteBuffer buf = ByteBuffer.allocate(4);
    byte[] values = bsValues.toByteArray();

    int offset = 0;
    while(offset < values.length) {
      buf.put(values, offset, 4);
      buf.flip();
      final int keyLen = buf.getInt();
      buf.clear();
      offset += 4;

      final String key = new String(values, offset, keyLen);
      offset += keyLen;

      buf.put(values, offset, 4);
      buf.flip();
      final int valueLen = buf.getInt();
      buf.clear();
      offset += 4;

      if(fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }

      offset += valueLen;
    }

    return result;
  }

  private ByteString serializeValues(final Map<String, ByteIterator> values) throws IOException {
    try(final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for(final Map.Entry<String, ByteIterator> value : values.entrySet()) {
        final byte[] keyBytes = value.getKey().getBytes(UTF_8);
        final byte[] valueBytes = value.getValue().toArray();

        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);

        buf.clear();

        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);

        buf.clear();
      }
      return ByteString.copyFrom(baos.toByteArray());
    }
  }
}
