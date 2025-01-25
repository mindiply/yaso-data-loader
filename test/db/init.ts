import { getDb } from "./dbProvider";

export async function initTestDb() {
  const dbt = await getDb();
  return dbt.tx(async (db) => {
    await db.none('CREATE EXTENSION IF NOT EXISTS "pgcrypto"');
    await db.none("drop table if exists tx_details");
    await db.none("drop table if exists tx_log");
    await db.none("drop table if exists tst_log");
    await db.none("drop table if exists tst_doc");
    await db.none("drop table if exists tst");
    await db.none(`
create table tx_log (
  tx_log_id text primary key,
  tx_log_operation jsonb,
  tx_log_when timestamp not null
)`);
    await db.none(`
create table tx_details (
  tx_details_id text primary key,
  tx_details_tx_log_id text,
  tx_record_type text,
  tx_record_id text,
  tx_change_type text,
  tx_data_change jsonb
)`);
    await db.none(`
create table tst (
  tst_id text primary key,
  tst_name text not null,
  tst_amount int not null,
  tst_when timestamp not null,
  tst_nullable text
)`);
    await db.none(`
create table tst_log (
  tst_log_id text,
  tst_doc_id text
)`);
    await db.none(
      "alter table tst_log add constraint pk_tst_log primary key (tst_log_id, tst_doc_id)",
    );
    await db.none(`
create table tst_doc (
  tst_doc_id text primary key,
  tst_doc_name text not null,
  tst_doc_base64 text not null,
  tst_doc_status text not null
)`);
  });
}

export async function clearTestDb(closePool: boolean) {
  const dbt = await getDb();
  await dbt.tx(async (db) => {
    await db.none("drop table if exists tst_log");
    await db.none("drop table if exists tst");
    await db.none("drop table if exists tst_doc");
  });
  if (closePool) {
    return dbt.$pool.end();
  }
}
