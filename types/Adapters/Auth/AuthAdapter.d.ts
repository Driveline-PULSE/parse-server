export class AuthAdapter {
  policy: 'default' | 'additional' | 'solo';
  constructor();
  validateAppId(appIds: any, authData: any, options: any, request: any): Promise<any>;
  validateAuthData(authData: any, options: any, request: any): Promise<any>;
  validateSetUp(authData: any, options: any, request: any): Promise<any>;
  validateLogin(authData: any, options: any, request: any): Promise<any>;
  validateUpdate(authData: any, options: any, request: any): Promise<any>;
  beforeFind(authData: any): void;
  challenge(challengeData: any, authData: any, options: any, request: any): Promise<any>;
  afterFind(authData: any, options: any, request: any): Promise<any>;
  validateOptions(options: any): void;
}
export default AuthAdapter;
