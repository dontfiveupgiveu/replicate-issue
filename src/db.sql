-- CREATE DATABASE "replicate-issue";
CREATE TABLE x(
    status jsonb NOT NULL
);

CREATE TABLE c(
    claimable jsonb NOT NULL
);

INSERT INTO c (claimable) VALUES('{"id": 1}');
