package com.yahoo.ycsb.db;

import com.yahoo.ycsb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.exception.TiKVException;
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

  private static final String PD_ADDRESSES = "tikv.pd.addresses";

  private static final String DEFAULT_PD_ADDRESSES = "127.0.0.1:2379";

  private static final Logger LOGGER = LoggerFactory.getLogger(TiKVClient.class);

  @GuardedBy("TiKVClient.class") private RawKVClient tikv = null;

  @Override
  public void init() throws DBException {
    synchronized(TiKVClient.class) {
      if(tikv == null) {
        LOGGER.info("TiKV Client initializing...");
        try {
          String pdAddr = getProperties().getProperty(PD_ADDRESSES, DEFAULT_PD_ADDRESSES);
          tikv = initKVRawClient(pdAddr);
        } catch (final TiKVException e) {
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
  private RawKVClient initKVRawClient(String pdAddr) throws TiKVException {
    TiSession session = TiSession.getInstance(TiConfiguration.createRawDefault(pdAddr));
    return session.createRawClient();
  }

  @Override
  public void cleanup() throws DBException {
    super.cleanup();

    synchronized (TiKVClient.class) {
      LOGGER.info("TiKV Client closing...");
      try {
        tikv.close();
      } catch (final TiKVException e) {
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
      LOGGER.debug("read table " + table + " key" + key);
      final ByteString values = tikv.get(getRowKey(table, key));
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
  public Status scan(final String table, final String startKey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    try {
      LOGGER.debug("scanning table " + table + " startKey " + startKey);
      List<Kvrpcpb.KvPair> pairs = tikv.scan(getRowKey(table, startKey), recordcount);
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
      LOGGER.debug("update table " + table + " key " + key);
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
      LOGGER.debug("insert table " + table + " key " + key);
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
      LOGGER.debug("delete table " + table + " key " + key);
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
