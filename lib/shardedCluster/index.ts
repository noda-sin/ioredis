import Redis from "../redis";
import { IRedisOptions } from "../redis/RedisOptions";
import crc16 from "crc/crc16";

/**
 * Client for the sharded Redis Cluster
 *
 * @class ShardedCluster
 * @extends {EventEmitter}
 */
class ShardedCluster {
  private shard: Array<Redis>;

  /**
   * Creates an instance of Cluster.
   *
   * @param {(Array<object>)} startupNodes
   * @param {any} [options={}]
   * @memberof Cluster
   */
  constructor(
    startupNodes: Array<object>,
    options: IRedisOptions  = {}
  ) {
    this.shard = startupNodes.map(node => {
      return new Redis(node['port'], node['host'], options);
    });
  }

  public getNode(key: string): Redis {
    let slot = this.getHashSlot(key);
    let shardLen = this.shard.length;
    let redisIndex = slot % shardLen;
    return this.shard[redisIndex];
  }

  private getHashSlot(key: string): number {
    let k = key;
    let s = key.indexOf("{");
    if (s !== -1) {
      let e = key.indexOf("}");
      if (e !== -1 && s < e) {
        k = key.substring(s + 1, e);
      }
    }
    return crc16(k) % 16384;
  }
}

export default ShardedCluster;
