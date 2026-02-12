export interface Auth {
  config: any;
  cacheController: any;
  installationId: string;
  isMaster: boolean;
  isMaintenance: boolean;
  isReadOnly: boolean;
  user: any;
}

export function master(config: any): Auth;
export function maintenance(config: any): Auth;
export function nobody(config: any): Auth;
export function readOnly(config: any): Auth;
export function shouldUpdateSessionExpiry(config: any, session: any): boolean;
export function getAuthForSessionToken(options: { config: any; sessionToken: string; installationId?: string }): Promise<Auth>;
export function getAuthForLegacySessionToken(options: { config: any; sessionToken: string; installationId?: string }): Promise<Auth>;
export function findUsersWithAuthData(config: any, authData: any): Promise<any>;
export function hasMutatedAuthData(authData: any, userAuthData: any): boolean;
export function checkIfUserHasProvidedConfiguredProvidersForLogin(req: any, authData: any, provider: string, config: any): void;
export function handleAuthDataValidation(authData: any, req: any, foundUser: any): Promise<any>;
