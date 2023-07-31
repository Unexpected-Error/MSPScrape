CREATE TABLE IF NOT EXISTS MSPs
(
    ID   INTEGER primary key AUTOINCREMENT,
    Name TEXT,
    URL  TEXT,
    CompanyNumber TEXT
);

CREATE TABLE IF NOT EXISTS VIPs
(
    ID   INTEGER primary key AUTOINCREMENT,
    MSPID INTEGER,
    FirstName TEXT,
    LastName TEXT,
    Title TEXT,
    Email TEXT,
    Phone TEXT,
    FOREIGN KEY (MSPID) REFERENCES MSPs(ID)
)
