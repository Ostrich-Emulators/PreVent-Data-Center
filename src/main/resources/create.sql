CREATE TABLE status (
  id INTEGER PRIMARY KEY,
  description VARCHAR(20)
);

CREATE TABLE workitem (
  id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 100000, INCREMENT BY 1),
  uuid VARCHAR(50) UNIQUE NOT NULL,
  bytes INT,
  containerId VARCHAR(50),
  started TIMESTAMP,
  finished TIMESTAMP,
  format VARCHAR(10),
  status INT REFERENCES status(id),
  message VARCHAR(200),
  inputpath VARCHAR(500),
  outputpath VARCHAR(500)
);

CREATE TABLE file (
  id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 100000, INCREMENT BY 1),
  path VARCHAR(500),
  version VARCHAR(30)
);

CREATE TABLE workitem_conversion (
  workitem_id INTEGER REFERENCES workitem(id) ON DELETE CASCADE,
  file_id INTEGER REFERENCES file(id) ON DELETE CASCADE
);

CREATE TABLE signal (
  id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 100000, INCREMENT BY 1),
  name VARCHAR(100),
  wave BOOLEAN,
  uom VARCHAR(10)
);

CREATE TABLE file_signal (
  file_id INTEGER REFERENCES file(id),
  signal_id INTEGER REFERENCES signal(id),
  starttime TIMESTAMP,
  endtime TIMESTAMP,
  maxvalue FLOAT(10),
  minvalue FLOAT(10)
);

INSERT INTO status(id,description) VALUES (0,'ADDED');
INSERT INTO status(id,description) VALUES (1,'QUEUED');
INSERT INTO status(id,description) VALUES (2,'RUNNING');
INSERT INTO status(id,description) VALUES (3,'FINISHED');
INSERT INTO status(id,description) VALUES (4,'ERROR');
INSERT INTO status(id,description) VALUES (5,'KILLED');
INSERT INTO status(id,description) VALUES (6,'PREPROCESSING');

