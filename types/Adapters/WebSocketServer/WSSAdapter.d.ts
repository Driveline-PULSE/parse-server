export class WSSAdapter {
  onListen: () => void;
  onConnection: (ws: any) => void;
  onError: (error: Error) => void;
  constructor(options?: any);
  start(options?: any): void;
  close(): void;
}
export default WSSAdapter;
