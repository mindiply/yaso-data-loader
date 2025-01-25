import {
  aggregateWith,
  and,
  createDBTbl,
  equals,
  functionCall,
  Id,
  not,
  rawSql,
  selectFrom,
  TableDefinition,
} from "yaso";
import { LinkTable, RecordStatus } from "../../src/types";
import { getDb } from "./dbProvider";
import {
  createDataApi,
  toEntitiesTablesMap,
  toTableOptions,
} from "../../src/dataApi";

export interface TstDoc {
  _id: string;
  name: string;
  base64: string;
  status: RecordStatus;
}

export const tstDocTblDef: TableDefinition<TstDoc> = {
  name: "tstDoc",
  dbName: "tst_doc",
  fields: [
    {
      name: "_id",
      dbName: "tst_doc_id",
    },
    {
      name: "name",
      dbName: "tst_doc_name",
    },
    {
      name: "base64",
      dbName: "tst_doc_base64",
    },
    {
      name: "status",
      dbName: "tst_doc_status",
    },
  ],
};

const tstDocLinkTblDef: TableDefinition<LinkTable> = {
  name: "tst_lnk_doc",
  dbName: "tst_log",
  fields: [
    {
      dbName: "tst_log_id",
      name: "parentId",
    },
    {
      dbName: "tst_doc_id",
      name: "childId",
    },
  ],
};

export interface Tst {
  _id: string;
  name: string;
  amount: number;
  status: RecordStatus;
  when: Date;
  nullable: string | null;
  docsIds: Id[];
}

export const tstTblDef: TableDefinition<Tst> = {
  dbName: "tst",
  name: "tst",
  fields: [
    {
      dbName: "tst_id",
      name: "_id",
    },
    {
      dbName: "tst_amount",
      name: "amount",
    },
    {
      dbName: "tst_name",
      name: "name",
      isEncrypted: true,
    },
    {
      dbName: "tst_when",
      name: "when",
    },
    {
      dbName: "tst_nullable",
      name: "nullable",
    },
  ],
  calculatedFields: [
    {
      dbName: "docsIds",
      name: "docsIds",
      calculation: (tst) =>
        selectFrom([tstDocTblDef, tstDocLinkTblDef], (qry, docTbl, lnkTbl) => {
          qry
            .fields(
              functionCall(
                "coalesce",
                aggregateWith("array_agg", docTbl.cols._id),
                rawSql("array[]::text[]"),
              ),
            )
            .where(
              and([
                equals(lnkTbl.cols.parentId, tst.cols._id),
                equals(lnkTbl.cols.childId, docTbl.cols._id),
                not(equals(docTbl.cols.status, RecordStatus.Deleted)),
              ]),
            );
        }),
    },
  ],
};

interface TstLog {
  _id: string;
  historyEntry: string;
  createdAt: Date;
  commitId: string;
}

export const tstLogTblDef: TableDefinition<TstLog> = {
  name: "tstLog",
  dbName: "tst_log",
  fields: [
    {
      name: "_id",
      dbName: "tst_log_id",
    },
    {
      name: "historyEntry",
      dbName: "tst_log_history_entry",
    },
    {
      name: "createdAt",
      dbName: "tst_log_created_at",
    },
    {
      name: "commitId",
      dbName: "tst_log_commit_id",
    },
  ],
};

export const getTestDataApi = () =>
  createDataApi({
    idField: "_id",
    getEncryptionKey: () => "testEncryptionKey",
    linkTables: {
      Tst: {
        TstDoc: {
          docsIds: createDBTbl(tstDocLinkTblDef),
        },
      },
    },
    dataDefinitions: toEntitiesTablesMap({
      Tst: toTableOptions(createDBTbl(tstTblDef), {
        newRecord: {
          mandatory: ["name", "amount", "when"] as const,
          optional: ["nullable"] as const,
          generate: { status: () => RecordStatus.Active },
          dbGenerated: ["_id"],
        },
      }),
      TstDoc: toTableOptions(createDBTbl(tstDocTblDef), {
        newRecord: {
          mandatory: ["name", "base64", "status"] as const,
          optional: [] as const,
        },
      }),
    }),
    dataApiTxDb: async (cb) => {
      const db = await getDb();
      return db.tx(cb);
    },
    dataApiQueryDb: async (cb) => {
      const db = await getDb();
      return db.task(cb);
    },
    logTransactions: {
      txTbl: createDBTbl({
        name: "txLOg",
        dbName: "tx_log",
        fields: [
          {
            name: "_id",
            dbName: "tx_log_id",
          },
          {
            name: "when",
            dbName: "tx_log_when",
          },
          { name: "operation", dbName: "tx_log_operation" },
        ],
      }),
      txDetailsTbl: createDBTbl({
        name: "txDetails",
        dbName: "tx_details",
        fields: [
          {
            name: "_id",
            dbName: "tx_details_id",
          },
          {
            name: "txLogId",
            dbName: "tx_details_tx_log_id",
          },
          {
            name: "changeType",
            dbName: "tx_change_type",
          },
          {
            name: "dataChange",
            dbName: "tx_data_change",
          },
          {
            name: "recordType",
            dbName: "tx_record_type",
          },
          {
            name: "recordId",
            dbName: "tx_record_id",
          }
        ],
      }),
    },
  });
