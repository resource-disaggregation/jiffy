package mmux.kv;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import mmux.directory.directory_service;
import mmux.directory.rpc_data_status;
import java.io.Closeable;
import java.nio.ByteBuffer;
import org.apache.thrift.TException;

public class KVClient implements Closeable {

  public class RedoException extends Exception {

    RedoException() {
      super();
    }
  }

  public class RedirectException extends Exception {

    private List<String> blocks;

    RedirectException(List<String> blocks) {
      super();
      this.blocks = blocks;
    }

    List<String> getBlocks() {
      return blocks;
    }
  }

  private int[] slots;
  private KVChainClient[] clients;
  private directory_service.Client fs;
  private String path;
  private BlockClientCache cache;

  public KVClient(directory_service.Client fs, String path, rpc_data_status dataStatus)
      throws TException {
    this.fs = fs;
    this.path = path;
    this.clients = new KVChainClient[dataStatus.data_blocks.size()];
    this.slots = new int[dataStatus.data_blocks.size()];
    this.cache = new BlockClientCache();
    for (int i = 0; i < clients.length; i++) {
      slots[i] = dataStatus.data_blocks.get(i).slot_begin;
      clients[i] = new KVChainClient(cache, dataStatus.data_blocks.get(i).block_names);
    }
  }

  @Override
  public void close() {
    for (KVChainClient client : clients) {
      client.close();
    }
  }

  private void refresh() throws TException {
    rpc_data_status dataStatus = fs.dstatus(path);
    this.clients = new KVChainClient[dataStatus.data_blocks.size()];
    this.slots = new int[dataStatus.data_blocks.size()];
    for (int i = 0; i < clients.length; i++) {
      slots[i] = dataStatus.data_blocks.get(i).slot_begin;
      clients[i] = new KVChainClient(cache, dataStatus.data_blocks.get(i).block_names);
    }
  }

  public boolean exists(String key) throws TException, RedoException, RedirectException {
    try {
      return Objects.equals(parseResponse(clients[blockId(key)].exists(key)), "true");
    } catch (RedoException e) {
      return Objects.equals(parseResponse(clients[blockId(key)].exists(key)), "true");
    } catch (RedirectException e) {
      return Objects
          .equals(parseResponse(new KVChainClient(cache, e.getBlocks()).redirectedExists(key)),
              "true");
    }
  }

  public String get(String key) throws TException, RedoException, RedirectException {
    try {
      return parseResponse(clients[blockId(key)].get(key));
    } catch (RedoException e) {
      return parseResponse(clients[blockId(key)].get(key));
    } catch (RedirectException e) {
      return parseResponse(new KVChainClient(cache, e.getBlocks()).redirectedGet(key));
    }
  }

  public String put(String key, String value) throws TException, RedoException, RedirectException {
    try {
      return parseResponse(clients[blockId(key)].put(key, value));
    } catch (RedoException e) {
      return parseResponse(clients[blockId(key)].put(key, value));
    } catch (RedirectException e) {
      return parseResponse(new KVChainClient(cache, e.getBlocks()).redirectedPut(key, value));
    }
  }

  public String update(String key, String value)
      throws TException, RedoException, RedirectException {
    try {
      return parseResponse(clients[blockId(key)].update(key, value));
    } catch (RedoException e) {
      return parseResponse(clients[blockId(key)].update(key, value));
    } catch (RedirectException e) {
      return parseResponse(new KVChainClient(cache, e.getBlocks()).redirectedUpdate(key, value));
    }
  }

  public String remove(String key) throws TException, RedoException, RedirectException {
    try {
      return parseResponse(clients[blockId(key)].remove(key));
    } catch (RedoException e) {
      return parseResponse(clients[blockId(key)].remove(key));
    } catch (RedirectException e) {
      return parseResponse(new KVChainClient(cache, e.getBlocks()).redirectedRemove(key));
    }
  }

  private int blockId(String key) {
    int hash = KVHash.get(key);
    int low = 0;
    int high = slots.length;
    while (low < high) {
      final int mid = (low + high) / 2;
      if (hash >= slots[mid]) {
        low = mid + 1;
      } else {
        high = mid;
      }
    }
    return low - 1;
  }

  private String parseResponse(ByteBuffer bb) throws TException, RedoException, RedirectException {
    String rawResponse = new String(bb.array(), StandardCharsets.UTF_8);
    // Actionable response
    if (rawResponse.charAt(0) == '!') {
      if (rawResponse.equals("!ok")) {
        return "ok";
      } else if (rawResponse.equals("!key_not_found")) {
        return null;
      } else if (rawResponse.equals("!duplicate_key")) {
        throw new IllegalArgumentException("Key already exists");
      } else if (rawResponse.equals("!args_error")) {
        throw new IllegalArgumentException("Incorrect arguments");
      } else if (rawResponse.equals("!block_moved")) {
        refresh();
        throw new RedoException();
      } else if (rawResponse.startsWith("!exporting")) {
        String[] parts = rawResponse.split("!");
        List<String> blocks = new ArrayList<>(parts.length - 1);
        blocks.addAll(Arrays.asList(parts).subList(2, parts.length));
        throw new RedirectException(blocks);
      }
    }
    return rawResponse;
  }
}
