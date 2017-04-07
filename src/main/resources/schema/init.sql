CREATE TABLE IF NOT EXISTS teleporter_config (
  k VARCHAR(100),
  v TEXT,
  PRIMARY KEY (k)
);
CREATE TABLE IF NOT EXISTS teleporter_runtime (
  k VARCHAR(100),
  v TEXT,
  PRIMARY KEY (k)
);
INSERT IGNORE INTO teleporter_config VALUE ('id', 0);
INSERT IGNORE INTO teleporter_runtime VALUE ('id', 0);