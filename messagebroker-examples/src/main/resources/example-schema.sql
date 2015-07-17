DROP TABLE IF EXISTS example_message_consumer;
DROP TABLE IF EXISTS example_message;

CREATE TABLE example_message (
  id             VARCHAR(127) NOT NULL,
  sequence_no    SERIAL,
  published      TIMESTAMP    NOT NULL,
  queue_id       VARCHAR(127) NOT NULL,
  message_string TEXT,
  message_bytes  BYTEA,
  PRIMARY KEY (id)
);

CREATE TABLE example_message_consumer (
  message_id    VARCHAR(127) NOT NULL,
  consumer_id   VARCHAR(127) NOT NULL,
  version_id    VARCHAR(127) NOT NULL,
  status        VARCHAR(32)  NOT NULL,
  last_updated  TIMESTAMP    NOT NULL,
  times_out     TIMESTAMP    NOT NULL,
  retries       INTEGER      NOT NULL,
  error_message TEXT,
  PRIMARY KEY (message_id, consumer_id),
  FOREIGN KEY (message_id) REFERENCES example_message (id)
);