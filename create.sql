DROP TABLE BufferedFiles CASCADE;
DROP TABLE BufferedData CASCADE;
DROP TABLE Snapshot CASCADE;
DROP TABLE ManifestEntry CASCADE;
DROP TABLE Schema CASCADE;
DROP TABLE Tables CASCADE;
DROP TABLE Namespace CASCADE;

CREATE TABLE BufferedFiles(
    file_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_name VARCHAR NOT NULL,
    finalized BOOL NOT NULL,
    size INT NOT NULL,
    isDelete BOOL NOT NULL
);

CREATE TABLE BufferedData(
    file_id INT REFERENCES BufferedFiles,
    part INT NOT NULL,
    content BYTEA NOT NULL,
    size INT NOT NULL,
    PRIMARY KEY (file_id, part)
);

CREATE VIEW BufferedView AS (
    SELECT  d.file_id,
            COALESCE (
                SUM(d.size) OVER (PARTITION BY d.file_id ORDER BY d.part ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                0
            ) as file_offset,
            d.size,
            d.content
    FROM BufferedData d, BufferedFiles f
    WHERE d.file_id = f.file_id AND f.finalized
);

CREATE TABLE Tables(
                       table_uuid VARCHAR PRIMARY KEY,
                       name VARCHAR NOT NULL,
                       last_sequence_number INT NOT NULL,
                       last_updated_ms INT NOT NULL,
                       current_snapshot_id INT NOT NULL,
                       namespace VARCHAR NOT NULL,
                       metadata VARCHAR NOT NULL
);

CREATE TABLE Snapshot(
    table_uuid VARCHAR NOT NULL REFERENCES tables,
    snapshot_id INT NOT NULL,
    sequence_number INT NOT NULL, -- same as snapshot id for us
    timestamp_ms INT NOT NULL,
    manifest_list VARCHAR NOT NULL,
    summary_operation VARCHAR NOT NULL,
    PRIMARY KEY (table_uuid, snapshot_id)
);

CREATE TABLE ManifestEntry(
    file_name VARCHAR,
    record_count INT,
    file_size_in_bytes INT
);




CREATE TABLE Namespace(
    name VARCHAR PRIMARY KEY
);

CREATE TABLE SCHEMA(
    column_id INT NOT NULL PRIMARY KEY,
    table_uuid VARCHAR NOT NULL,
    column_name VARCHAR NOT NULL,
    required BOOL NOT NULL,
    type VARCHAR NOT NULL
);

INSERT INTO SCHEMA VALUES
    (0, '1732-73ab-12ef', 'c_0', true, 'int'),
    (1, '1732-73ab-12ef', 'c_1', true, 'int'),
    (2, '1732-73ab-12ef', 'c_2', true, 'int'),
    (3, '1732-73ab-12ef', 'c_3', true, 'int'),
    (4, '1732-73ab-12ef', 'c_4', true, 'int'),
    (5, '1732-73ab-12ef', 'c_5', true, 'int'),
    (6, '1732-73ab-12ef', 'c_6', true, 'int'),
    (7, '1732-73ab-12ef', 'c_7', true, 'string'),
    (8, '1732-73ab-12ef', 'c_8', true, 'string'),
    (9, '1732-73ab-12ef', 'c_9', true, 'string'),
    (10, '1732-73ab-12ef', 'c_10', true, 'int'),
    (11, '1732-73ab-12ef', 'c_11', true, 'int'),
    (12, '1732-73ab-12ef', 'c_12', true, 'int'),
    (13, '1732-73ab-12ef', 'c_13', true, 'string'),
    (14, '1732-73ab-12ef', 'c_14', true, 'string'),
    (15, '1732-73ab-12ef', 'c_15', true, 'string')
;
