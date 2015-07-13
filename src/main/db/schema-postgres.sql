DROP TABLE IF EXISTS message_consumer;
DROP TABLE IF EXISTS message;

CREATE TABLE message (
  id          VARCHAR(127),
  sequence_no SERIAL,
  published   TIMESTAMP    NOT NULL,
  queue_id    VARCHAR(127) NOT NULL,
  message     TEXT,
  PRIMARY KEY (id)
);

CREATE TABLE message_consumer (
  message_id   VARCHAR(127) NOT NULL,
  consumer_id  VARCHAR(127) NOT NULL,
  version_id   INTEGER      NOT NULL,
  status       VARCHAR(32)  NOT NULL,
  last_updated TIMESTAMP    NOT NULL,
  times_out    TIMESTAMP    NOT NULL,
  retries      INTEGER      NOT NULL,
  error        TEXT,
  PRIMARY KEY (message_id, consumer_id)
);