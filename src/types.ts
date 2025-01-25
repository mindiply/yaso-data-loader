import DataLoader from 'dataloader';
import {Id, IDBTable, ReferencedTable, SQLExpression} from 'yaso';
import {ITask} from 'pg-promise';
import {ISQLOrderByField} from 'yaso/lib/query/types';

export interface LinkTable {
  parentId: Id;
  childId: Id;
}

type ArrayElementValue<T> = T extends (infer U)[] ? U : never;

interface NewRecordFieldValueGenerator<DataType, F extends keyof DataType> {
  (info: Partial<DataType>): DataType[F];
}

type NewRecordOptions<
  DataType,
  M extends keyof DataType,
  O extends keyof DataType
> = {
  mandatory: M[];
  optional: O[];
  generate?: {
    [F in keyof DataType]?: NewRecordFieldValueGenerator<DataType, F>;
  };
  dbGenerated?: Array<keyof DataType>;
};

export interface EntityTableOptions<
  DataType,
  M extends keyof DataType,
  O extends keyof DataType
> {
  dbTable: IDBTable<DataType>;
  newRecord: NewRecordOptions<DataType, M, O>;
  // updatableRecordFields: M[];
}

export type EntityTypeOfTableOptions<M> =
  M extends EntityTableOptions<any, any, any>
    ? M['dbTable'] extends IDBTable<infer D>
      ? D
      : never
    : never;

type MandatoryFieldsOfTableOptions<M> =
  M extends EntityTableOptions<any, any, any>
    ? M['dbTable'] extends IDBTable<infer D>
      ? M['newRecord']['mandatory'] extends ReadonlyArray<infer Mandatory>
        ? // ? Extract<keyof D, Mandatory> extends keyof D
          Mandatory
        : keyof D
      : // : never
        never
    : never;

type OptionalFieldsOfTableOptions<M> =
  M extends EntityTableOptions<any, any, any>
    ? M['dbTable'] extends IDBTable<infer D>
      ? M['newRecord']['optional'] extends ReadonlyArray<infer Optional>
        ? // ? Extract<keyof D, Optional> extends keyof D
          Optional
        : keyof D
      : // : never
        never
    : never;

export type MandatoryRecordInfo<T> =
  T extends EntityTableOptions<any, any, any>
    ? Pick<EntityTypeOfTableOptions<T>, MandatoryFieldsOfTableOptions<T>>
    : never;

export type OptionalRecordInfo<T> =
  T extends EntityTableOptions<any, any, any>
    ? Pick<EntityTypeOfTableOptions<T>, OptionalFieldsOfTableOptions<T>>
    : never;

export type DataTypeOfDef<T> =
  T extends EntityTableOptions<infer DataType, any, any>
    ? DataType
    : T extends IDBTable<infer DataType>
      ? DataType
      : never;

export type AddRecordInfo<T> =
  T extends EntityTableOptions<any, any, any>
    ? MandatoryRecordInfo<T> & Partial<OptionalRecordInfo<T>>
    : never;

export type UpdateRecordInfo<T> =
  T extends EntityTableOptions<any, any, any>
    ? Partial<MandatoryRecordInfo<T> & OptionalRecordInfo<T>>
    : never;

export type ExtractEntityTablesFromMap<M> = {
  [T in keyof M]: M[T] extends EntityTableOptions<any, any, any>
    ? EntityTableOptions<
        EntityTypeOfTableOptions<M[T]>,
        MandatoryFieldsOfTableOptions<M[T]>,
        OptionalFieldsOfTableOptions<M[T]>
      >
    : never;
};

export type ApiEntityTablesMap<M> = {
  [T in keyof M]: EntityTableOptions<any, any, any>;
};

export type ApiDataLoaders<ApiDataTypes> = {
  [K in keyof ApiDataTypes]: DataLoader<Id, DataTypeOfDef<ApiDataTypes[K]>>;
};

export interface DataApiTxDb<DT extends ApiEntityTablesMap<any>> {
  insertRecord: <K extends keyof DT>(
    recordType: K,
    data: AddRecordInfo<DT[K]>
  ) => Promise<DataTypeOfDef<DT[K]>>;

  deleteRecord: <K extends keyof DT>(
    recordType: K,
    recordId: Id
  ) => Promise<DataTypeOfDef<DT[K]>>;

  updateRecord: <K extends keyof DT>(
    recordType: K,
    recordId: Id,
    data: UpdateRecordInfo<DT[K]>
  ) => Promise<DataTypeOfDef<DT[K]>>;

  addLink: <P extends keyof DT, C extends keyof DT>(
    parentType: P,
    parentId: Id,
    fieldName: keyof DataTypeOfDef<DT[P]>,
    childTYpe: C,
    childId: Id
  ) => Promise<DataTypeOfDef<DT[P]>>;

  removeLink: <P extends keyof DT, C extends keyof DT>(
    parentType: P,
    parentId: Id,
    fieldName: keyof DataTypeOfDef<DT[P]>,
    childTYpe: C,
    childId: Id
  ) => Promise<DataTypeOfDef<DT[P]>>;

  inTxGetRecord: <K extends keyof DT>(
    recordType: K,
    recordId: Id
  ) => Promise<DataTypeOfDef<DT[K]> | null>;
}

export interface DataApi<DT extends ApiEntityTablesMap<any>> {
  getRecord: <RT extends keyof DT>(
    recordType: RT,
    recordId: Id
  ) => Promise<DataTypeOfDef<DT[RT]>>;

  bulkSelect: <
    RT extends keyof DT,
    PRMS extends Record<string | number | symbol, unknown>
  >(
    tableName: RT,
    qryOptions:
      | ((refTbl: ReferencedTable<DataTypeOfDef<DT[RT]>>) => {
          isSelectDistinct?: boolean;
          where?: SQLExpression;
          orderByFields?: ISQLOrderByField<DataTypeOfDef<DT[RT]>>[];
          maxRows?: number;
        })
      | null,
    prms: PRMS
  ) => Promise<DataTypeOfDef<DT[RT]>[]>;

  tx: <TO, Operation extends Record<string | symbol | number, unknown>>(
    operation: Operation,
    cb: (crudApi: DataApiTxDb<DT>) => Promise<TO>
  ) => Promise<TO>;
}

export interface InvokePgPromiseTask {
  <R>(fn: (tx: ITask<never>) => Promise<R>): Promise<R>;
}

export interface TxLog {
  _id: Id;
  when: Date;
  operation: Record<string | symbol | number, unknown>;
}

export enum ChangeType {
  AddLink = 'ADD_LINK',
  Delete = 'DELETE',
  Insert = 'INSERT',
  RemoveLink = 'REMOVE_LINK',
  Update = 'UPDATE'
}

export type LinkChangeData<
  DT extends ApiEntityTablesMap<any>,
  P extends keyof DT = keyof DT,
  C extends keyof DT = keyof DT
> = {
  childId: Id;
  childType: C;
  parentField: keyof DataTypeOfDef<DT[P]>;
  parentId: Id;
  parentType: P;
};

export interface TxRecordChange<DT extends ApiEntityTablesMap<any>> {
  _id: Id;
  txLogId: Id;
  recordType: keyof DT;
  recordId: Id;
  changeType: ChangeType;
  dataChange: Partial<DataTypeOfDef<DT[keyof DT]>> | LinkChangeData<DT>;
}

export enum RecordStatus {
  Active = 'ACTIVE',
  Archived = 'ARCHIVED',
  Inactive = 'INACTIVE',
  Deleted = 'DELETED'
}

export type DataApiLinkTables<DT extends ApiEntityTablesMap<any>> = {
  [P in keyof DT]?: {
    [C in keyof DT]?: {
      [F in keyof DataTypeOfDef<DT[P]>]?: IDBTable<LinkTable>;
    };
  };
};

/**
 * Props for creating a new DataApi instance.
 */
export interface DataApiCreateProps<
  ApiDataMappings extends ApiEntityTablesMap<any>
> {
  /**
   * This is the core data definitions for the API. It tells what DBTables are available
   * and what fields are in each.
   */
  dataDefinitions: ApiDataMappings;

  /**
   * This is the link tables that are used to link records in the DBTables together.
   * Each link field has its own table. Each table should have a corresponding DBTable that is
   * passed into the mappings.
   */
  linkTables: DataApiLinkTables<ApiDataMappings>;

  /**
   * This is the function that runs transactions in the db, and that expects a
   * function that receives a pg-promise IBaseProtocol object as its sole parameter.
   *
   * It allows you to inject your own initialised db object.
   */
  dataApiTxDb: InvokePgPromiseTask;

  /**
   * This is the function that runs tasks (no data manipulation) in the db, and that expects a
   * function that receives a pg-promise IBaseProtocol object as its sole parameter.
   *
   * It allows you to inject your own initialised db object.
   */
  dataApiQueryDb: InvokePgPromiseTask;

  /**
   * All DBTables expect to have the same id field name. If not provided,
   * the '_id' field is assumed.
   */
  idField?: keyof DataTypeOfDef<ApiDataMappings[keyof ApiDataMappings]>;

  /**
   * Generates a new ID for a record. If not provided, the default is to use
   * random bytes using the crypto library. If set to false, no id is
   * generated, even if not provided in the record information.
   *
   * You can use it when you generate id fields via a trigger in the database,
   * or if you use serial fields.
   *
   * @returns {Id}
   */
  idGenerator?: () => Id | false;

  /**
   * This is the function that returns the encryption key for the db.
   * If you don't use encrypted fields, it's not needed.
   *
   * @returns {string}
   */
  getEncryptionKey?: () => string;

  /**
   * This library by default assumes you will want to use soft-deletes, rather than
   * hard deletes. It looks for a 'status' field, and if present it will set the status
   * on creation and deletion of records.
   *
   * If you want to opt out explicitly, pass false. If you don't set the parameter, but
   * no 'status' field is found, it will also opt out.
   */
  statusField?:
    | keyof DataTypeOfDef<ApiDataMappings[keyof ApiDataMappings]>
    | false;

  /**
   * This is the table that will store the transaction logs, if you provided two IDBTables
   * for the transaction record and the transaction details records.
   */
  logTransactions?: {
    txTbl: IDBTable<TxLog>;
    txDetailsTbl: IDBTable<TxRecordChange<ApiDataMappings>>;
  };
}
