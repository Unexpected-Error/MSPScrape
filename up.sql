create table MSPs
(
    id   INTEGER primary key AUTOINCREMENT,
    name TEXT,
    url  TEXT
);

create table VIPs
(
    id   INTEGER primary key AUTOINCREMENT,
    msp_id INTEGER,
    firstName TEXT,
    lastName TEXT,
    title TEXT,
    email TEXT,
    phoneNumber TEXT,
    FOREIGN KEY (msp_id) REFERENCES MSPs(id)
)

--  first name, last name, title, email, phone numbers