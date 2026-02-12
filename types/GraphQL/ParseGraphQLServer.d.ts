import type { Application } from 'express';
import type { Server } from 'http';

export class ParseGraphQLServer {
  constructor(parseServer: any, config: { graphQLPath: string; playgroundPath?: string; subscriptionsPath?: string; graphQLCustomTypeDefs?: any; graphQLPublicIntrospection?: boolean });
  applyGraphQL(app: Application): void;
  applyPlayground(app: Application): void;
  createSubscriptions(server: Server): void;
  setGraphQLConfig(graphQLConfig: any): Promise<any>;
  applyRequestContextMiddleware(api: Application, options: any): void;
}
