CREATE TABLE ma_precinct_votes (
    election_id VARCHAR(256),
    precinct VARCHAR(256),
    party VARCHAR(256),
    candidate VARCHAR(256),
    votes INT,
    PRIMARY KEY (election_id, precinct, party, candidate)
);

CREATE TABLE ma_county_votes (
    election_id VARCHAR(256),
    county VARCHAR(256),
    party VARCHAR(256),
    candidate VARCHAR(256),
    votes INT,
    PRIMARY KEY (election_id, county, party, candidate)
);