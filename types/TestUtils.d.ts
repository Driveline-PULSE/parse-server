import type { Server, Socket } from 'net';

export function destroyAllDataPermanently(fast?: boolean): Promise<void>;
export function resolvingPromise<T = any>(): Promise<T> & { resolve: (value?: T) => void; reject: (reason?: any) => void };
export function sleep(ms: number): Promise<void>;
export function getConnectionsCount(server: Server): Promise<number>;

export class Connections {
  constructor();
  track(server: Server): void;
  destroyAll(): void;
  count(): number;
}
