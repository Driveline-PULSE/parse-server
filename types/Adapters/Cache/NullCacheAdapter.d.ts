export class NullCacheAdapter {
  constructor();
  get(key: string): Promise<null>;
  put(key: string, value: any, ttl?: number): Promise<void>;
  del(key: string): Promise<void>;
  clear(): Promise<void>;
}
export default NullCacheAdapter;
