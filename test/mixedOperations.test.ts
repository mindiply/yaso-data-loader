import { describe, test, expect, beforeAll, afterAll } from "@jest/globals";
import { clearTestDb, initTestDb } from "./db/init";
import { getTestDataApi, Tst } from "./db/tstTableDef";
import { RecordStatus } from "../src/types";
import { list, sqlIn, value } from "yaso";

beforeAll(async () => {
  await clearTestDb(false);
  return initTestDb();
});

afterAll(async () => {
  return clearTestDb(true);
});

describe("mixedOperations", () => {
  test("Add single tst record", async () => {
    const api = getTestDataApi();
    const when = new Date();
    const addedRecord = await api.tx({ op: "CreateTest" }, async (db) => {
      return db.insertRecord("Tst", {
        name: "test",
        amount: 1,
        when,
      });
    });
    const retrievedRecord = await api.getRecord("Tst", addedRecord._id);
    expect(retrievedRecord).toMatchObject({ name: "test", amount: 1, when });
  });

  test("Add multiple tst records", async () => {
    const api = getTestDataApi();
    const when = new Date();
    const addedRecords = await api.tx({ op: "CreateTest" }, async (db) => {
      const added = [
        await db.insertRecord("Tst", {
          name: "test1",
          amount: 1,
          when,
        }),
        await db.insertRecord("Tst", {
          name: "test2",
          amount: 2,
          when,
        }),
      ];
      const inTxRecord = await db.inTxGetRecord("Tst", added[0]._id);
      expect(inTxRecord).toMatchObject({ name: "test1", amount: 1, when });
      return added;
    });
    expect(addedRecords.length).toBe(2);
    const retrievedRecords: Tst[] = [];
    for (const record of addedRecords) {
      retrievedRecords.push(await api.getRecord("Tst", record._id));
    }

    // @ts-expect-error record signature is not complete
    expect(retrievedRecords).toMatchObject(addedRecords);
  });

  test("Add records with a link", async () => {
    const api = getTestDataApi();
    const when = new Date();
    const [addedTst, addedDoc] = await api.tx(
      { op: "CreateTest" },
      async (db) => {
        const tstRecord = await db.insertRecord("Tst", {
          name: "test",
          amount: 1,
          when,
        });
        const docRecord = await db.insertRecord("TstDoc", {
          name: "doc",
          base64: "base64",
          status: RecordStatus.Active,
        });
        await db.addLink(
          "Tst",
          tstRecord._id,
          "docsIds",
          "TstDoc",
          docRecord._id,
        );
        return [tstRecord, docRecord];
      },
    );
    const retrievedTst = await api.getRecord("Tst", addedTst._id);
    expect(retrievedTst.docsIds.length).toBe(1);
    expect(retrievedTst.docsIds[0]).toBe(addedDoc._id);
  });

  test("Add and then removes a link", async () => {
    const api = getTestDataApi();
    const when = new Date();
    const [addedTst, addedDoc] = await api.tx(
      { op: "CreateTest" },
      async (db) => {
        const tstRecord = await db.insertRecord("Tst", {
          name: "test",
          amount: 1,
          when,
        });
        const docRecord = await db.insertRecord("TstDoc", {
          name: "doc",
          base64: "base64",
          status: RecordStatus.Active,
        });
        await db.addLink(
          "Tst",
          tstRecord._id,
          "docsIds",
          "TstDoc",
          docRecord._id,
        );
        return [tstRecord, docRecord];
      },
    );
    const retrievedTst = await api.getRecord("Tst", addedTst._id);
    expect(retrievedTst.docsIds.length).toBe(1);
    expect(retrievedTst.docsIds[0]).toBe(addedDoc._id);
    const retrievedTstAfter = await api.tx({ op: "RemoveLink" }, async (db) => {
      return db.removeLink(
        "Tst",
        addedTst._id,
        "docsIds",
        "TstDoc",
        addedDoc._id,
      );
    });
    // Needed because otherwise dataloader caches the previous result
    const nextReqApi = getTestDataApi();
    const afterTxTst = await nextReqApi.getRecord("Tst", addedTst._id);
    expect(retrievedTstAfter.docsIds.length).toBe(0);
    expect(afterTxTst.docsIds.length).toBe(0);
  });

  test("Update a record", async () => {
    const api = getTestDataApi();
    const when = new Date();
    const addedRecord = await api.tx({ op: "CreateTest" }, async (db) => {
      return db.insertRecord("Tst", {
        name: "test",
        amount: 1,
        when,
      });
    });
    const updatedRecord = await api.tx({ op: "UpdateTest" }, async (db) => {
      return db.updateRecord("Tst", addedRecord._id, { amount: 2 });
    });
    expect(updatedRecord).toMatchObject({ name: "test", amount: 2, when });
    // Needed because otherwise dataloader caches the previous result
    const nextReqApi = getTestDataApi();
    const retrievedRecord = await nextReqApi.getRecord("Tst", addedRecord._id);
    expect(retrievedRecord).toMatchObject({ name: "test", amount: 2, when });
  });

  test("Using bulk select", async () => {
    const api = getTestDataApi();
    const when = new Date();
    const addedRecords = await api.tx({ op: "CreateTest" }, async (db) => {
      const added = [
        await db.insertRecord("Tst", {
          name: "test1",
          amount: 1,
          when,
        }),
        await db.insertRecord("Tst", {
          name: "test2",
          amount: 2,
          when,
        }),
      ];
      return added;
    });

    const nextReqApi = getTestDataApi();
    const retrievedRecords = await nextReqApi.bulkSelect(
      "Tst",
      (tst) => ({
        orderByFields: [{ field: tst.cols.name }],
        where: sqlIn(
          tst.cols._id,
          list(value(addedRecords[0]._id), value(addedRecords[1]._id)),
        ),
      }),
      {},
    );
    console.log(retrievedRecords);
    expect(retrievedRecords[0]).toMatchObject({ name: "test1", amount: 1, when });
    expect(retrievedRecords[1]).toMatchObject({ name: "test2", amount: 2, when });
  });
});
