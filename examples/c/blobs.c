/* blobs: a TEXT and a LONGBLOB written, read and deleted in the three
 * ways a program runs key operations: synchronously, the values in a
 * holder; on a connection, a query holding them, by primary key and
 * through the unique index; and through an executor, each operation
 * holding its own. Every result is checked, and the run ends with "all
 * checks passed".
 *
 *   ./blobs [connectstring]
 *
 * Copyright (c) 2026 Hopsworks and/or its affiliates.
 * Licensed under the MIT License. See LICENSE in the repository root. */
#include "t1.h"

/* The table: id its key, code unique, doc TEXT, data LONGBLOB. */
#define EX_T4 "t4"
/* The hash index a MySQL server makes for UNIQUE KEY t4_code. */
#define EX_T4_INDEX "t4_code$unique"

/* A row of ic_ex.t4 as the records below lay it out. The blob fields
 * doc and data take no room in it: their values, NULL included, are
 * kept beside it, by the query, the operation or a holder. */
struct t4_row {
  uint32_t id;    /* INT UNSIGNED NOT NULL, the primary key */
  uint32_t code;  /* INT UNSIGNED NOT NULL, the unique key */
};

/* A record over struct t4_row of the fields named; a blob field's
 * offset and null bit are not used. */
static IC_RECORD *t4_record(const IC_TABLE_DEF *table,
                            const char *const *names, uint32_t count)
{
  IC_FIELD_SPEC spec[4];
  IC_RECORD *rec;
  uint32_t i;

  for (i = 0; i < count; i++) {
    spec[i].field_id = ex_field(table, names[i]);
    spec[i].offset = 0;
    if (strcmp(names[i], "code") == 0)
      spec[i].offset = offsetof(struct t4_row, code);
    spec[i].null_byte_offset = IC_NO_NULL_BIT;
    spec[i].null_bit = 0;
  }
  EX_CHECK(ic_record_create(table, spec, count, sizeof(struct t4_row),
                            &rec));
  return rec;
}

/* A value of len bytes that differs from seed to seed, letters for a
 * TEXT. */
static uint8_t *make_value(size_t len, uint32_t seed, int text)
{
  uint8_t *bytes = malloc(len);
  uint32_t n;
  size_t i;

  if (bytes == NULL)
    ex_fail("malloc", 12);
  for (i = 0; i < len; i++) {
    n = ((uint32_t)i * 2654435761u + seed) >> 13;
    bytes[i] = text ? (uint8_t)('a' + n % 26) : (uint8_t)n;
  }
  return bytes;
}

/* True if a value given back is the len bytes at want; NULL wanted as
 * a NULL want. */
static int same(const uint8_t *value, size_t value_len,
                const uint8_t *want, size_t len)
{
  if (want == NULL)
    return value == NULL;
  return value != NULL && value_len == len && memcmp(value, want, len) == 0;
}

int main(int argc, char **argv)
{
  static const char *const all[] = {"id", "code", "doc", "data"};
  static const char *const id_only[] = {"id"};
  static const char *const code_only[] = {"code"};
  static const char *const values_only[] = {"doc", "data"};
  const size_t text_len = 10000;
  const size_t bytes_len = 100000;
  const size_t new_len = 50000;
  IC_APID_CONNECTION *conn;
  IC_APID_GLOBAL *global = ex_connect(argc, argv, "ex_blobs", &conn);
  IC_TABLE_DEF *table;
  IC_INDEX_DEF *index;
  IC_RECORD *key_rec;
  IC_RECORD *rec;
  IC_RECORD *values_rec;
  IC_RECORD *code_rec;
  IC_BLOB_VALUES *values;
  IC_READ_KEY_ARGS read_args;
  IC_WRITE_KEY_ARGS write_args;
  IC_TRANSACTION_HINT hint;
  IC_QUERY_ID updater;
  IC_QUERY_ID reader;
  IC_TRANS_ID trans;
  IC_EXECUTOR_OPTIONS options;
  IC_APID_EXECUTOR *executor;
  IC_APID_SUBMITTER *submitter;
  IC_TABLE_DEF *x_table;
  IC_RECORD *x_key;
  IC_RECORD *x_values;
  IC_OP_SHAPE *shape;
  IC_REQUEST *request;
  struct t4_row row;
  struct t4_row key;
  uint8_t *text = make_value(text_len, 1, 1);
  uint8_t *bytes = make_value(bytes_len, 2, 0);
  uint8_t *new_bytes = make_value(new_len, 3, 0);
  const uint8_t *value;
  uint8_t *buffer;
  uint8_t *rows;
  size_t len;
  uint32_t doc;
  uint32_t data;
  uint32_t op;
  bool found;

  EX_CHECK(ic_apid_conn_table_bind(conn, EX_DB, EX_T4, &table));
  doc = ex_field(table, "doc");
  data = ex_field(table, "data");
  key_rec = t4_record(table, id_only, 1);
  rec = t4_record(table, all, 4);
  values_rec = t4_record(table, values_only, 2);

  /* 1. Synchronously, the values in a holder made for the record: the
   *    write goes in as many rounds as its parts take. */
  memset(&key, 0, sizeof key);
  key.id = 1;
  row = key;
  row.code = 100;
  EX_CHECK(ic_blob_values_create(rec, &values));
  EX_CHECK(ic_blob_values_set(values, doc, text, text_len));
  EX_CHECK(ic_blob_values_set(values, data, bytes, bytes_len));
  EX_CHECK(ic_apid_conn_write_blobs(conn, IC_WRITE_KIND_WRITE, key_rec,
                                    (const uint8_t *)&key, sizeof key, rec,
                                    (const uint8_t *)&row, sizeof row,
                                    values));
  ic_blob_values_clear(values);
  EX_CHECK(ic_apid_conn_read_committed_blobs(
      conn, key_rec, (const uint8_t *)&key, sizeof key, rec,
      (uint8_t *)&row, sizeof row, values, &found));
  ex_check(found, "row 1 is there");
  EX_CHECK(ic_blob_values_get(values, doc, &value, &len));
  ex_check(same(value, len, text, text_len), "the TEXT read back");
  EX_CHECK(ic_blob_values_get(values, data, &value, &len));
  ex_check(same(value, len, bytes, bytes_len), "the LONGBLOB read back");
  printf("written and read back: %zu and %zu\n", text_len, bytes_len);

  /* 2. On a connection: an update by primary key, the TEXT set NULL
   *    and the LONGBLOB filled in place, then a read through the
   *    unique index on code. */
  EX_CHECK(ic_apid_query_create(conn, table, key_rec, values_rec,
                                &updater));
  rows = ic_apid_query_key_row(conn, updater, &len);
  memcpy(rows, &key, sizeof key);
  EX_CHECK(ic_apid_query_set_blob_null(conn, updater, doc));
  EX_CHECK(ic_apid_query_blob_buffer(conn, updater, data, new_len,
                                     &buffer));
  memcpy(buffer, new_bytes, new_len);
  hint = ic_apid_conn_hint_for_key(conn, table, key_rec,
                                   (const uint8_t *)&key, sizeof key);
  EX_CHECK(ic_apid_conn_start_transaction(conn, hint, table, &trans));
  ic_write_key_args_init(&write_args, sizeof write_args);
  write_args.kind = IC_WRITE_KIND_UPDATE;
  EX_CHECK(ic_apid_conn_write_key(conn, updater, trans, &write_args));
  EX_CHECK(ic_apid_conn_commit_transaction(conn, trans));
  ex_finish(conn, trans);
  EX_CHECK(ic_apid_query_error(conn, updater));

  EX_CHECK(ic_apid_conn_index_bind(conn, EX_DB, EX_T4_INDEX, EX_T4,
                                   &index));
  code_rec = t4_record(table, code_only, 1);
  EX_CHECK(ic_apid_query_create_unique(conn, index, code_rec, values_rec,
                                       &reader));
  rows = ic_apid_query_key_row(conn, reader, &len);
  memcpy(rows, &row, sizeof row);
  hint = ic_apid_conn_hint_for_unique_key(conn, index, code_rec, rows, len);
  EX_CHECK(ic_apid_conn_start_transaction_on_index(conn, hint, index,
                                                   &trans));
  /* Through an index a read always locks, and holds its lock to the
   * commit. */
  ic_read_key_args_init(&read_args, sizeof read_args);
  read_args.kind = IC_READ_KIND_COMMITTED;
  EX_CHECK(ic_apid_conn_read_key(conn, reader, trans, &read_args));
  EX_CHECK(ic_apid_conn_commit_transaction(conn, trans));
  ex_finish(conn, trans);
  EX_CHECK(ic_apid_query_error(conn, reader));
  EX_CHECK(ic_apid_query_blob(conn, reader, doc, &value, &len));
  ex_check(value == NULL, "the TEXT now NULL");
  EX_CHECK(ic_apid_query_blob(conn, reader, data, &value, &len));
  ex_check(same(value, len, new_bytes, new_len), "the new LONGBLOB");
  printf("updated by key, read by code: NULL and %zu\n", new_len);

  /* 3. Through an executor: a read, then a delete, which takes the
   *    parts with the row. */
  ic_executor_options_init(&options, sizeof options);
  EX_CHECK(ic_apid_executor_start(global, &options, &executor));
  EX_CHECK(ic_apid_executor_submitter(executor, &submitter));
  EX_CHECK(ic_apid_submitter_table_bind(submitter, EX_DB, EX_T4, &x_table));
  x_key = t4_record(x_table, id_only, 1);
  x_values = t4_record(x_table, values_only, 2);
  EX_CHECK(ic_op_shape_create(x_table, x_key, x_values, &shape));
  EX_CHECK(ic_request_create(&request));
  EX_CHECK(ic_request_add_read(request, shape, IC_READ_KIND_COMMITTED,
                               &op));
  rows = ic_request_key_row(request, op, &len);
  memcpy(rows, &key, sizeof key);
  EX_CHECK(ic_apid_submitter_execute(submitter, request));
  EX_CHECK(ic_request_blob(request, op, data, &value, &len));
  ex_check(same(value, len, new_bytes, new_len), "read by the executor");
  EX_CHECK(ic_request_clear(request));
  EX_CHECK(ic_request_add_write(request, shape, IC_WRITE_KIND_DELETE, &op));
  rows = ic_request_key_row(request, op, &len);
  memcpy(rows, &key, sizeof key);
  EX_CHECK(ic_apid_submitter_execute(submitter, request));
  EX_CHECK(ic_apid_conn_read_committed_blobs(
      conn, key_rec, (const uint8_t *)&key, sizeof key, rec,
      (uint8_t *)&row, sizeof row, values, &found));
  ex_check(!found, "row 1 deleted, its parts with it");
  printf("read and deleted through an executor\n");

  ic_request_free(request);
  ic_op_shape_free(shape);
  ic_record_free(x_values);
  ic_record_free(x_key);
  ic_table_def_free(x_table);
  ic_apid_submitter_free(submitter);
  ic_apid_executor_free(executor);
  EX_CHECK(ic_apid_query_free(conn, reader));
  EX_CHECK(ic_apid_query_free(conn, updater));
  ic_blob_values_free(values);
  ic_record_free(code_rec);
  ic_record_free(values_rec);
  ic_record_free(rec);
  ic_record_free(key_rec);
  ic_index_def_free(index);
  ic_table_def_free(table);
  ic_apid_conn_free(conn);
  ic_apid_global_free(global);
  free(new_bytes);
  free(bytes);
  free(text);
  printf("all checks passed\n");
  return 0;
}
