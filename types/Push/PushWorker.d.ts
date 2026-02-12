export class PushWorker {
  subscriber: any;
  adapter: any;
  channel: string;
  constructor(pushAdapter: any, subscriberConfig?: any);
  run(workItem: { body: any; query: any; pushStatus: any; applicationId: string; UTCOffset?: any }): Promise<any>;
  sendToAdapter(body: any, installations: any[], pushStatus: any, config: any, UTCOffset?: any): Promise<any>;
}
export default PushWorker;
