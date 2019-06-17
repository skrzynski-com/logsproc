DROP TABLE logs IF EXISTS;

CREATE TABLE logs
(
  id      VARCHAR(32),
  state    VARCHAR(32),
  timestamp   BIGINT,
  type VARCHAR(32),
  host VARCHAR(256),
  duration BIGINT,
  alert BOOLEAN,
  version INT,
  PRIMARY KEY(ID)
);
