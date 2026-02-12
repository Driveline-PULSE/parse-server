export class AnalyticsAdapter {
  appOpened(parameters: any, req: any): Promise<any>;
  trackEvent(eventName: string, parameters: any, req: any): Promise<any>;
}
export default AnalyticsAdapter;
