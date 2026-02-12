export class RedisCacheAdapter {
  constructor(redisCtx?: any, ttl?: number);
  connect(): Promise<void>;
  handleShutdown(): Promise<void>;
  get(key: string): Promise<any | null>;
  put(key: string, value: any, ttl?: number): Promise<void>;
  del(key: string): Promise<void>;
  clear(): Promise<void>;
  getAllKeys(): Promise<string[]>;
}
export default RedisCacheAdapter;
