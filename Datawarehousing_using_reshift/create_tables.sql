CREATE TABLE IF NOT EXISTS staging_events (
    event TEXT,
    event_tag TEXT,
    sport TEXT,
    sport_code TEXT,
    sport_url TEXT
);

COPY staging_events
FROM 's3://paris-2024-olympic-summer-games-test/events.csv'
IAM_ROLE 'arn:aws:iam::339713044034:role/service-role/AmazonRedshift-CommandsAccessRole-20240708T174538'
CSV
IGNOREHEADER 1;

SELECT * FROM staging_events;