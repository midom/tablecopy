#define _POSIX_C_SOURCE 200809L

#include <glib.h>
#include <mysql.h>
#include <stdio.h>
#include <string.h>

void *sq_init(int max);
gpointer sq_pop(void *s);
void sq_push(void *s, gpointer);

char *query;
char *charset = "binary";
int nthreads = 16;
gboolean crazy, force;

static GOptionEntry entries[] = {{"query", 'q', 0, G_OPTION_ARG_STRING, &query,
                                  "Override SELECT query", NULL},
                                 {"threads", 't', 0, G_OPTION_ARG_INT,
                                  &nthreads, "Number of writer threads", NULL},
                                 {"crazy", 0, 0, G_OPTION_ARG_NONE, &crazy,
                                  "Crazy fast, super unsafe, not safe for prod",
                                  NULL},
                                 {"force", 0, 0, G_OPTION_ARG_NONE, &force,
                                  "Ignore insertion failures", NULL},
                                 {"charset", 0, 0, G_OPTION_ARG_NONE, &charset,
                                  "Connection character set", NULL},
                                 {NULL}};

/* Picks argument like host:port/db/table and connects to specified host/db  */
MYSQL *establish_connection(const char *dest) {
  char *host, **hostport;
  int port = 3306;

  gchar **args = g_strsplit(dest, "/", 3);
  if (!args) {
    g_critical("could not parse arguments");
    return NULL;
  }
  if (!args) {
    g_critical("Could not parse connection details: empty input");
    return NULL;
  }

  hostport = g_strsplit(args[0], ":", 2);
  host = hostport[0];
  if (hostport[1]) {
    port = atoi(hostport[1]);
  }

  if (!args[1]) {
    g_critical("No database name specified");
    return NULL;
  }

  MYSQL *m = mysql_init(NULL);
  mysql_options(m, MYSQL_READ_DEFAULT_GROUP, "mysqlcp");
  if (!mysql_real_connect(m, host, NULL, NULL, args[1], port, NULL, 0)) {
    g_critical("Could not connect to %s: %s", args[0], mysql_error(m));
    mysql_close(m);
    return NULL;
  }

  mysql_query(m, g_strdup_printf("SET NAMES %s", charset));
  mysql_query(m, "SET wait_timeout=3600");

  if (crazy) {
    if (mysql_query(m, "SET wait_timeout=3600, sql_log_bin=0, unique_checks=0, "
                       "rocksdb_write_disable_wal=1")) {
      g_warning("Could not set crazy variables: %s", mysql_error(m));
    }
  }

  g_strfreev(hostport);
  g_strfreev(args);
  return m;
}

/* Reads CREATE TABLE statement from the source */
char *get_schema_definition(MYSQL *m, char *table_name) {
  char *qs = g_strdup_printf("SHOW CREATE TABLE %s", table_name);
  if (mysql_query(m, qs)) {
    g_critical("Could not read schema: %s", mysql_error(m));
    exit(EXIT_FAILURE);
  }
  MYSQL_RES *result = mysql_store_result(m);
  if (!result || !mysql_field_count(m) || !mysql_affected_rows(m)) {
    g_critical("Should not happen: %s", mysql_error(m));
    exit(EXIT_FAILURE);
  }
  MYSQL_ROW row = mysql_fetch_row(result);
  return strdup(row[1]);
}

/* Rewrites the first line of CREATE TABLE string to use the new name */
char *rewrite_table_name(const char *ddl, char *new_name) {
  if (!new_name)
    return strdup(ddl);

  char *p = strchr(ddl, '\n');
  if (!p)
    return NULL;

  return g_strdup_printf("CREATE TABLE `%s` (\n%s", new_name, p);
}

/* Reads the table name from host/db/port spec */
char *get_table_name(char *s) {
  int n = 1;
  while (*s++) {
    if (*s == '/' && !n--) {
      s++;
      break;
    }
  }
  if (!s || !s[0])
    return NULL;
  return strdup(s);
}

struct worker_config {
  void *queue;
  char *dst;
};

/* Thread worker that executes INSERT queries */
void *worker(void *p) {
  struct worker_config *ctx = p;
  MYSQL *mysql = establish_connection(ctx->dst);
  GString *s;
  while ((s = sq_pop(ctx->queue))) {
    if (mysql_real_query(mysql, s->str, s->len)) {
      if (!force) {
        g_critical("Could not insert data: %s", mysql_error(mysql));
        exit(EXIT_FAILURE);
      }

      g_warning("Could not insert data: %s", mysql_error(mysql));
    }
    g_string_free(s, TRUE);
  }
  return NULL;
}

/* Main routine that does the needful */
int main(int ac, char *argv[]) {
  mysql_library_init(0, NULL, NULL);

  GOptionContext *optctx = g_option_context_new("Copy tables between DBs");
  g_option_context_add_main_entries(optctx, entries, NULL);
  g_option_context_set_summary(
      optctx, "Quickly dump-and-load data from one table to another: "
              "\ntable_copy srchost[:port]/db/table dsthost[:port]/db/[table]");
  GError *error = NULL;
  if (!g_option_context_parse(optctx, &ac, &argv, &error)) {
    g_critical("Option parsing failed: %s", error->message);
    exit(EXIT_FAILURE);
  }

  if (ac < 3) {
    g_critical("Bye!");
    exit(EXIT_FAILURE);
  }
  MYSQL *src = establish_connection(argv[1]);
  if (!src) {
    exit(EXIT_FAILURE);
  }
  char *table_name = get_table_name(argv[1]);
  char *def = get_schema_definition(src, table_name);
  if (!def) {
    exit(EXIT_FAILURE);
  }

  MYSQL *dst = establish_connection(argv[2]);
  char *new_name = get_table_name(argv[2]);
  char *newdef = rewrite_table_name(def, new_name);
  free(def);

  if (!dst) {
    exit(EXIT_FAILURE);
  }
  if (mysql_query(dst, newdef)) {
    g_warning("Cannot create table: %s", mysql_error(dst));
  }

  if (mysql_query(src, query
                           ? query
                           : g_strdup_printf("SELECT * FROM %s", table_name))) {
    g_critical("%s", mysql_error(src));
  }
  MYSQL_RES *result = mysql_use_result(src);
  int num_fields = mysql_num_fields(result);

  GString *buf = g_string_sized_new(1024 * 1024 * 2);
  GString *escaped = g_string_sized_new(1024);

  char *insert_prefix = g_strdup_printf("INSERT INTO %s VALUES \n", new_name);

  struct worker_config conf;
  conf.dst = argv[2];
  conf.queue = sq_init(100);

  GThread **threads = g_new0(GThread *, nthreads);

  for (int n = 0; n < nthreads; n++) {
    threads[n] = g_thread_new("cp-writer", worker, &conf);
  }

  /* At the end of the resultset or when the batch is full we issue the
   * background INSERT. If there's plenty of space in the buffer, we write
   * multiple rows - field by field. */
  for (;;) {
    MYSQL_ROW row = mysql_fetch_row(result);
    if (!row || buf->len > 1024 * 1024) {
      sq_push(conf.queue, buf);
      buf = g_string_sized_new(1024 * 1024 * 10);
      if (!row)
        break;
    }

    if (!buf->len) {
      g_string_append(buf, insert_prefix);
    } else {
      g_string_append(buf, ",\n");
    }

    g_string_append_c(buf, '(');
    unsigned long *lengths = mysql_fetch_lengths(result);
    for (int i = 0; i < num_fields; i++) {
      if (i) {
        g_string_append_c(buf, ',');
      }
      g_string_append_c(buf, '\'');

      if (row[i]) {
        g_string_set_size(escaped, lengths[i] * 2 + 1);
        mysql_real_escape_string(dst, escaped->str, row[i], lengths[i]);
        g_string_append(buf, escaped->str);
      } else {
        g_string_append(buf, "NULL");
      }
      g_string_append_c(buf, '\'');
    }
    g_string_append_c(buf, ')');
  }

  /* All running threads should shutdown, then we wait for them to finish their
   * work */
  for (int n = 0; n < nthreads; n++)
    sq_push(conf.queue, NULL);

  for (int n = 0; n < nthreads; n++) {
    g_thread_join(threads[n]);
  }
}
