CREATE TABLE categorized_pages (
    id BIGSERIAL PRIMARY KEY,
    url TEXT NOT NULL UNIQUE,
    blekko_category TEXT,
    blekko_rank INTEGER,
    web_title TEXT,
    web_body TEXT
);
