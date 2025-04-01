CREATE TABLE BufferedFiles(
    file_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    file_name VARCHAR,
    finalized BOOL,
    size INT
);

CREATE TABLE BufferedData(
    file_id INT REFERENCES BufferedFiles,
    part INT,
    content BYTEA,
    size INT,
    PRIMARY KEY (file_id, part)
);

CREATE VIEW BufferedView AS (
    SELECT  d.file_id,
            coalesce (
                sum(d.size) over (partition by d.file_id order by d.part rows between unbounded preceding and 1 preceding),
                0
            ) as file_offset,
            d.size,
            d.content
    FROM BufferedData d, BufferedFiles f
    WHERE d.file_id = f.file_id AND f.finalized
);
