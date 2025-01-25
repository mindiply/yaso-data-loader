import {
  ApiDataLoaders,
  ApiEntityTablesMap,
  ChangeType,
  DataApi,
  DataApiCreateProps,
  DataApiTxDb,
  DataTypeOfDef,
  EntityTableOptions,
  EntityTypeOfTableOptions,
  ExtractEntityTablesFromMap,
  LinkChangeData,
  LinkTable,
  RecordStatus,
  TxRecordChange
} from './types';
import {
  and,
  equals,
  Id,
  IDBTable,
  list,
  prm,
  rawSql,
  sqlIn,
  tbl,
  usePg
} from 'yaso';
import DataLoader from 'dataloader';
import {escapeForJson} from './utils/conversions';
import {parametrizeRecordData} from './utils/sqlParametrize';
import {extractNonNullable} from './utils/nonNullable';
import {randomBytes} from 'crypto';

usePg();

export function toTableOptions<
  DataType,
  M extends keyof DataType,
  O extends keyof DataType
>(
  dbTable: IDBTable<DataType>,
  tblOptions: Omit<EntityTableOptions<DataType, M, O>, 'dbTable'>
): EntityTableOptions<DataType, M, O> {
  return {dbTable, ...tblOptions} as EntityTableOptions<DataType, M, O>;
}

export function toEntitiesTablesMap<M extends ApiEntityTablesMap<any>>(
  mappings: M
): ExtractEntityTablesFromMap<M> {
  return mappings as unknown as ExtractEntityTablesFromMap<M>;
}

export function createDataApi<ApiDataMappings extends ApiEntityTablesMap<any>>(
  props: DataApiCreateProps<ApiDataMappings>
): DataApi<ExtractEntityTablesFromMap<ApiDataMappings>> {
  const idField =
    props.idField ||
    ('_id' as keyof DataTypeOfDef<ApiDataMappings[keyof ApiDataMappings]>);
  const statusField =
    props.statusField !== false
      ? ((props.statusField || 'status') as keyof DataTypeOfDef<
          ApiDataMappings[keyof ApiDataMappings]
        >)
      : false;
  const getEncryptionKey = props.getEncryptionKey || (() => 'NOTSET');

  function hasEncryptedFields(tblName: keyof ApiDataMappings) {
    const tblRecord = props.dataDefinitions[tblName].dbTable;
    for (const field of tblRecord.fields) {
      if (field.isEncrypted) {
        return true;
      }
    }
    return false;
  }

  const _loaders: Partial<ApiDataLoaders<ApiDataMappings>> = {};
  for (const tblName in props.dataDefinitions) {
    const tbl = tblName as keyof ApiDataMappings;
    _loaders[tbl] = new DataLoader<
      Id,
      DataTypeOfDef<ApiDataMappings[typeof tbl]>
    >(async keys => _bulkSelectByIds(tblName, keys));
  }
  const loaders = _loaders as ApiDataLoaders<ApiDataMappings>;

  async function _bulkSelectByIds<RT extends keyof ApiDataMappings>(
    tableName: RT,
    recordsIds: readonly Id[]
  ) {
    const refTbl = tbl(props.dataDefinitions[tableName].dbTable);
    const sql = refTbl.selectQrySql(refTbl => ({
      fields:
        refTbl.tbl.calculatedFields && refTbl.tbl.calculatedFields.length
          ? [
              refTbl,
              ...refTbl.tbl.calculatedFields.map(calcField => calcField.name)
            ]
          : [refTbl],
      where: sqlIn(refTbl.cols[idField], list(rawSql('$[recordsIds:csv]')))
    }));
    const records = await props.dataApiTxDb(wspDb =>
      wspDb.any<DataTypeOfDef<ApiDataMappings[RT]>>(
        sql,
        hasEncryptedFields(tableName)
          ? {
              recordsIds,
              encryptionKey: getEncryptionKey()
            }
          : {recordsIds}
      )
    );
    const recordById = new Map(
      records.map(record => {
        if (loaders) {
          loaders[tableName].prime(record[idField], record);
        }
        return [record[idField], record];
      })
    );
    return recordsIds.map(
      recordId =>
        recordById.get(recordId) ||
        new Error(`No result for ${String(tableName)}:${String(recordId)}`)
    );
  }

  return {
    getRecord: async (recordType, recordId) => {
      return loaders![recordType].load(recordId) as DataTypeOfDef<
        ExtractEntityTablesFromMap<ApiDataMappings>[typeof recordType]
      >;
    },
    bulkSelect: async (tableName, qryOptions, prms) => {
      const refTbl = tbl(props.dataDefinitions[tableName].dbTable);

      const sql = refTbl.selectQrySql(refTbl => ({
        fields:
          refTbl.tbl.calculatedFields && refTbl.tbl.calculatedFields.length
            ? [
                refTbl,
                ...refTbl.tbl.calculatedFields.map(calcField => calcField.name)
              ]
            : [refTbl],
        ...(qryOptions ? qryOptions(refTbl) : {})
      }));
      if (refTbl.tbl.fields.some(field => field.isEncrypted)) {
        (prms as any).encryptionKey = getEncryptionKey();
      }
      console.log('bulkSelect', sql, prms);
      const records = await props.dataApiQueryDb(wspDb =>
        wspDb.any<
          DataTypeOfDef<
            ExtractEntityTablesFromMap<ApiDataMappings>[typeof tableName]
          >
        >(sql, prms)
      );
      for (const record of records) {
        loaders![tableName].prime(record[idField], record);
      }
      return records as DataTypeOfDef<
        ExtractEntityTablesFromMap<ApiDataMappings>[typeof tableName]
      >[];
    },
    tx: async (operation, cb) => {
      return props.dataApiTxDb(async wspDb => {
        const txLogId = await insertTxLogRecord(operation);

        const _selectById = async <RT extends keyof ApiDataMappings>(
          recordType: RT,
          recordId: Id
        ) => {
          const record = await wspDb.oneOrNone<
            EntityTypeOfTableOptions<ApiDataMappings[RT]>
          >(
            tbl(props.dataDefinitions[recordType].dbTable).selectQrySql(
              refTbl => ({
                fields:
                  refTbl.tbl.calculatedFields &&
                  refTbl.tbl.calculatedFields.length
                    ? [
                        refTbl,
                        ...refTbl.tbl.calculatedFields.map(
                          calcField => calcField.name
                        )
                      ]
                    : [refTbl],
                where: equals(refTbl.cols[idField], prm('recordId'))
              })
            ),
            hasEncryptedFields(recordType)
              ? {
                  recordId,
                  encryptionKey: getEncryptionKey()
                }
              : {recordId}
          );
          if (record && loaders) {
            loaders[recordType].prime(recordId, record);
          }
          return record;
        };

        async function insertTxLogRecord(
          operation: Record<string | symbol | number, unknown>
        ) {
          if (!props.logTransactions) {
            return 'NOTRANSACTIONS';
          }
          const data = {
            _id: props.idGenerator ? props.idGenerator() : randomChars(16),
            operation: escapeForJson(operation),
            when: new Date()
          };
          const sql = tbl(props.logTransactions.txTbl).insertQrySql({
            returnFields: true,
            fields: parametrizeRecordData(data)
          });
          const record = await wspDb.one<TxRecordChange<ApiDataMappings>>(
            sql,
            data
          );
          return record._id;
        }

        async function insertTxRecordChangeEntry<
          K extends keyof ApiDataMappings
        >(
          txLogId: Id,
          recordType: K,
          recordId: Id,
          changeType: ChangeType,
          dataChange: Partial<DataTypeOfDef<ApiDataMappings[K]>>
        ) {
          if (!props.logTransactions) {
            return;
          }
          let changes = dataChange;
          for (const fieldName in dataChange) {
            const fieldValue = dataChange[fieldName];

            if (
              fieldValue &&
              typeof fieldValue === 'object' &&
              // @ts-expect-error Cannot safely check for Buffer
              fieldValue instanceof Buffer
            ) {
              changes = Object.assign({}, changes);
              changes[fieldName] = fieldValue.toString('base64');
            }
          }
          const data = {
            _id: props.idGenerator ? props.idGenerator() : randomChars(16),
            txLogId,
            recordType,
            recordId,
            changeType,
            dataChange: escapeForJson(changes)
          };
          const prms = parametrizeRecordData(data);
          const sql = tbl(props.logTransactions.txDetailsTbl).insertQrySql({
            returnFields: true,
            fields: {...prms, dataChange: rawSql(`$[dataChange:json]`)}
          });
          const record = await wspDb.one(sql, data);
          return record._id;
        }

        async function insertLinkChangeRecord<
          P extends keyof ApiDataMappings,
          C extends keyof ApiDataMappings
        >(
          txLogId: Id,
          changeType: ChangeType.AddLink | ChangeType.RemoveLink,
          parentType: P,
          parentId: Id,
          fieldName: keyof DataTypeOfDef<ApiDataMappings[P]>,
          childType: C,
          childId: Id
        ) {
          if (!props.logTransactions) {
            return;
          }
          const entityChange: LinkChangeData<ApiDataMappings> = {
            parentType,
            parentId: String(parentId),
            parentField: fieldName,
            childType,
            childId: String(childId)
          };
          const prms = {
            _id: props.idGenerator ? props.idGenerator() : randomChars(16),
            changeType,
            txLogId,
            recordType: parentType as string,
            recordId: String(parentId),
            dataChange: escapeForJson(entityChange)
          };
          const sql = tbl(props.logTransactions.txDetailsTbl).insertQrySql({
            fields: {...prms, dataChange: rawSql(`$[dataChange:json]`)}
          });
          return wspDb.none(sql, prms);
        }

        const crudApi: DataApiTxDb<
          ExtractEntityTablesFromMap<ApiDataMappings>
        > = {
          inTxGetRecord: _selectById,
          insertRecord: async (recordType, data) => {
            let newRecord: DataTypeOfDef<ApiDataMappings[typeof recordType]>;

            const dbTbl = props.dataDefinitions[recordType].dbTable;
            if (
              statusField &&
              dbTbl.fields.some(field => field.name === statusField) &&
              !(statusField in data)
            ) {
              (data as any)[statusField] = RecordStatus.Active;
            }

            if (!(data as any)[idField]) {
              (data as any)[idField] = props.idGenerator
                ? props.idGenerator()
                : randomChars(16);
            }
            const sql = tbl(dbTbl).insertQrySql({
              returnFields: true,
              fields: parametrizeRecordData(data)
            });
            if (hasEncryptedFields(recordType)) {
              (data as any).encryptionKey = getEncryptionKey();
            }
            newRecord = (await wspDb.one(sql, data)) as DataTypeOfDef<
              ApiDataMappings[typeof recordType]
            >;

            if (newRecord) {
              await insertTxRecordChangeEntry(
                txLogId,
                recordType,
                newRecord[
                  idField as keyof DataTypeOfDef<
                    ApiDataMappings[typeof recordType]
                  >
                ] as Id,
                ChangeType.Insert,
                newRecord
              );
              return newRecord;
            }
            throw new Error('Unable to insert new record');
          },
          deleteRecord: async (recordType, recordId) => {
            const tblDef = props.dataDefinitions[recordType].dbTable;
            let deletedRecord: DataTypeOfDef<
              ApiDataMappings[typeof recordType]
            >;
            if (
              statusField &&
              tblDef.fields.some(field => field.name === statusField)
            ) {
              const sql = tbl(tblDef).updateQrySql(stm => ({
                returnFields: true,
                where: equals(stm.cols[idField], prm('recordId')),
                fields: {status: String(RecordStatus.Deleted)}
              }));
              deletedRecord = await wspDb.one(sql, {recordId});
            } else {
              const existingRecord = await _selectById(recordType, recordId);
              if (!existingRecord) {
                throw new Error('Record not found');
              }
              deletedRecord = existingRecord;

              await wspDb.none(
                tbl(tblDef).deleteQrySql(stm => ({
                  where: equals(stm.cols[idField], prm('recordId'))
                })),
                {recordId}
              );
            }

            if (deletedRecord) {
              await insertTxRecordChangeEntry(
                txLogId,
                recordType,
                deletedRecord[
                  idField as keyof DataTypeOfDef<
                    ApiDataMappings[typeof recordType]
                  >
                ] as Id,
                ChangeType.Delete,
                deletedRecord
              );

              return deletedRecord;
            }
            throw new Error('No deleted record returned');
          },
          updateRecord: async (recordType, recordId, data) => {
            const tblDef = props.dataDefinitions[recordType].dbTable;
            const sql = tbl(tblDef).updateQrySql(stm => ({
              returnFields: true,
              where: equals(stm.cols[idField], prm('recordId')),
              // @ts-expect-error Problems with extraNonNullable typing
              fields: parametrizeRecordData(extractNonNullable(data))
            }));
            const updatedRecord = await wspDb.one(
              sql,
              hasEncryptedFields(recordType)
                ? {
                    ...data,
                    recordId,
                    encryptionKey: getEncryptionKey()
                  }
                : {...data, recordId}
            );
            if (updatedRecord) {
              await insertTxRecordChangeEntry(
                txLogId,
                recordType,
                updatedRecord._id,
                ChangeType.Update,
                updatedRecord
              );
              return updatedRecord;
            }
            throw new Error('Unsupported record type');
          },
          addLink: async (
            parentType,
            parentId,
            fieldName,
            childType,
            childId
          ) => {
            const parentChildTypes = props.linkTables[parentType];
            if (!parentChildTypes) {
              throw new Error(`No links for parent type ${String(parentType)}`);
            }
            const childFields = parentChildTypes[childType];
            if (!childFields) {
              throw new Error(
                `No link fields for link ${String(parentType)}->${String(childType)}`
              );
            }
            const linkTbl = childFields[fieldName];
            if (!linkTbl) {
              throw new Error(
                `No linked field ${String(parentType)}.${String(fieldName)}`
              );
            }
            const sql = tbl(
              linkTbl! as unknown as IDBTable<LinkTable>
            ).insertQrySql({
              fields: {
                childId: prm('childId'),
                parentId: prm('parentId')
              }
            });
            await wspDb.none(sql, {parentId, childId});
            const updatedParent = await _selectById(parentType, parentId);
            if (!updatedParent) {
              throw new Error(
                `Parent record not found ${String(parentType)}.${parentId}`
              );
            }
            await insertLinkChangeRecord(
              txLogId,
              ChangeType.AddLink,
              parentType,
              parentId,
              fieldName,
              childType,
              childId
            );
            return updatedParent;
          },
          removeLink: async (
            parentType,
            parentId,
            fieldName,
            childType,
            childId
          ) => {
            const parentChildTypes = props.linkTables[parentType];
            if (!parentChildTypes) {
              throw new Error(`No links for parent type ${String(parentType)}`);
            }
            const childFields = parentChildTypes[childType];
            if (!childFields) {
              throw new Error(
                `No link fields for link ${String(parentType)}->${String(childType)}`
              );
            }
            const linkTbl = childFields[fieldName];
            if (!linkTbl) {
              throw new Error(
                `No linked field ${String(parentType)}.${String(fieldName)}`
              );
            }
            const sql = tbl(
              linkTbl as unknown as IDBTable<LinkTable>
            ).deleteQrySql(ltb => ({
              where: and([
                equals(ltb.cols.parentId, prm('parentId')),
                equals(ltb.cols.childId, prm('childId'))
              ])
            }));
            await wspDb.none(sql, {parentId, childId});
            const updatedParent = await _selectById(parentType, parentId);
            if (!updatedParent) {
              throw new Error(
                `Parent record not found ${String(parentType)}.${parentId}`
              );
            }
            await insertLinkChangeRecord(
              txLogId,
              ChangeType.RemoveLink,
              parentType,
              parentId,
              fieldName,
              childType,
              childId
            );
            return updatedParent;
          }
        };
        return cb(crudApi);
      });
    }
  };
}

function randomChars(nChars: number, charSet?: string): string {
  const chars =
    charSet || 'abcdefghijklmnopqrstuwxyzABCDEFGHIJKLMNOPQRSTUWXYZ0123456789';
  const rnd = randomBytes(nChars);
  const value: string[] = new Array<string>(nChars);
  const len: number = Math.min(256, chars.length);
  const d = 256 / len;
  for (let i = 0; i < nChars; i++) {
    value[i] = chars[Math.floor(rnd[i] / d)];
  }
  return value.join('');
}
