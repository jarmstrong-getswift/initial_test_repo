CREATE TABLE scylla_load_log (
activity_date timestamp,
activity_status text,
table_updated text,
PRIMARY KEY (activity_status, activity_date)
) WITH CLUSTERING ORDER BY (activity_date desc);

INSERT INTO scylla_load_log (activity_date, activity_status, table_updated) VALUES (toTimeStamp(now()), 'SUCCESS','scylla_load_log');

CREATE TABLE test1 (
  space text,
  key text,
  value text,
  PRIMARY KEY (space, key)
) with CLUSTERING ORDER BY (key ASC);

INSERT INTO test1 (space, key, value) VALUES ('foo', 'blah', 'meh');

UPDATE test1 SET value = 'profit!' WHERE space = 'foo' AND key = 'blah';
