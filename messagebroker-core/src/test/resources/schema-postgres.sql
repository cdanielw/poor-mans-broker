DROP TABLE IF EXISTS message_consumer;
DROP TABLE IF EXISTS message;

CREATE TABLE message (
  id               VARCHAR(127) NOT NULL,
  sequence_no      SERIAL,
  publication_time TIMESTAMP    NOT NULL,
  queue_id         VARCHAR(127) NOT NULL,
  message_string   TEXT,
  message_bytes    BYTEA,
  PRIMARY KEY (id)
);

CREATE TABLE message_consumer (
  message_id    VARCHAR(127) NOT NULL,
  consumer_id   VARCHAR(127) NOT NULL,
  version_id    VARCHAR(127) NOT NULL,
  status        VARCHAR(32)  NOT NULL,
  last_updated  TIMESTAMP    NOT NULL,
  times_out     TIMESTAMP    NOT NULL,
  retries       INTEGER      NOT NULL,
  error_message TEXT,
  PRIMARY KEY (message_id, consumer_id),
  FOREIGN KEY (message_id) REFERENCES message (id)
);