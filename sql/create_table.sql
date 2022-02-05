CREATE TABLE IF NOT EXISTS personalData (
  id SERIAL,
  name varchar(255) NOT NULL,
  numberrange FLOAT NOT NULL,
  currency varchar(100) NOT NULL,
  PRIMARY KEY (id)
);