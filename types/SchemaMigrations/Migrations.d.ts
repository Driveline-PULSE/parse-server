export type FieldValueType =
  | 'String'
  | 'Boolean'
  | 'File'
  | 'Number'
  | 'Relation'
  | 'Pointer'
  | 'Date'
  | 'GeoPoint'
  | 'Polygon'
  | 'Array'
  | 'Object'
  | 'ACL';

export interface FieldType {
  type: FieldValueType;
  required?: boolean;
  defaultValue?: any;
  targetClass?: string;
}

export interface ProtectedFieldsInterface {
  [key: string]: string[];
}

export interface IndexInterface {
  [key: string]: number;
}

export interface IndexesInterface {
  [key: string]: IndexInterface;
}

export type CLPOperation = 'find' | 'count' | 'get' | 'update' | 'create' | 'delete';

export interface JSONSchema {
  className: string;
  fields?: { [key: string]: FieldType };
  indexes?: IndexesInterface;
  classLevelPermissions?: {
    find?: { [key: string]: boolean };
    count?: { [key: string]: boolean };
    get?: { [key: string]: boolean };
    update?: { [key: string]: boolean };
    create?: { [key: string]: boolean };
    delete?: { [key: string]: boolean };
    addField?: { [key: string]: boolean };
    protectedFields?: ProtectedFieldsInterface;
  };
}

export interface SchemaOptions {
  definitions: JSONSchema[];
  strict?: boolean;
  deleteExtraFields?: boolean;
  recreateModifiedFields?: boolean;
  lockSchemas?: boolean;
  keepUnknownIndexes?: boolean;
  beforeMigration?: () => void | Promise<void>;
  afterMigration?: () => void | Promise<void>;
}

export class CLP {
  static allow(perms: { [key: string]: CLPOperation[] }): { [key: string]: { [key: string]: boolean } };
}

export function makeSchema(className: string, schema: JSONSchema): JSONSchema;
