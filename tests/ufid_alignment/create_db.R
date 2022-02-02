library(DBI)

fdb <- dbConnect(RSQLite::SQLite(), "ufid1.sqlite")

dbExecute(fdb, "PRAGMA foreign_keys = ON;")

dbExecute(fdb, "CREATE TABLE feature (
  ufid INTEGER PRIMARY KEY,
  mz REAL NOT NULL,
  isotopologue TEXT,
  adduct TEXT,
  compound_name TEXT,
  cas TEXT,
  smiles TEXT,
  inchi TEXT,
  formula TEXT,
  comment TEXT,
  polarity TEXT NOT NULL,
  date_added INTEGER NOT NULL
);")

dbExecute(fdb, "CREATE TABLE tag (
          tag_id INTEGER PRIMARY KEY,
          tag_text TEXT NOT NULL UNIQUE
          );")

dbExecute(fdb, "CREATE TABLE feature_tag (
  tag_id INTEGER,
  ufid INTEGER,
  PRIMARY KEY (tag_id, ufid),
  FOREIGN KEY (ufid)
    REFERENCES feature (ufid)
      ON DELETE CASCADE
      ON UPDATE CASCADE,
  FOREIGN KEY (tag_id)
    REFERENCES tag (tag_id)
      ON DELETE CASCADE
      ON UPDATE CASCADE
);")

dbExecute(fdb, "CREATE TABLE retention_time (
          method TEXT NOT NULL,
          rt REAL NOT NULL,
          ufid INTEGER NOT NULL,
          FOREIGN KEY (ufid)
            REFERENCES feature (ufid)
              ON DELETE CASCADE
              ON UPDATE CASCADE
);")

dbExecute(fdb, "CREATE TABLE ms1 (
          method TEXT NOT NULL,
          mz REAL NOT NULL,
          rel_int REAL NOT NULL,
          ufid INTEGER NOT NULL,
          FOREIGN KEY (ufid)
            REFERENCES feature (ufid)
              ON DELETE CASCADE
              ON UPDATE CASCADE
);")

dbExecute(fdb, "CREATE TABLE ms2 (
          method TEXT NOT NULL,
          mz REAL NOT NULL,
          rel_int REAL NOT NULL,
          ufid INTEGER NOT NULL,
          FOREIGN KEY (ufid)
            REFERENCES feature (ufid)
              ON DELETE CASCADE
              ON UPDATE CASCADE
);")

dbListTables(fdb)

dbDisconnect(fdb)
