export class CacheAdapter {
  get(key: string): Promise<any>;
  put(key: string, value: any, ttl?: number): Promise<void>;
  del(key: string): Promise<void>;
  clear(): Promise<void>;
}
