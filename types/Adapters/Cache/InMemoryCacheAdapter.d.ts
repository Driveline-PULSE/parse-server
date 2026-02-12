export class InMemoryCacheAdapter {
  constructor(ctx?: { ttl?: number; maxSize?: number });
  get(key: string): Promise<any | null>;
  put(key: string, value: any, ttl?: number): Promise<void>;
  del(key: string): Promise<void>;
  clear(): Promise<void>;
}
export default InMemoryCacheAdapter;
