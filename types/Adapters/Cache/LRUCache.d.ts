export class LRUCache {
  constructor(options?: { ttl?: number; maxSize?: number });
  get(key: string): any | null;
  put(key: string, value: any, ttl?: number): void;
  del(key: string): void;
  clear(): void;
}
export default LRUCache;
