
import client from './client.js';

async function setup() {
    try {
        await client.connect();
        const createTableQuery = `
            -- 1. Header Table (The Main Post Data)
            CREATE TABLE IF NOT EXISTS posts (
                post_id VARCHAR(50) PRIMARY KEY,
                platform VARCHAR(50),
                content_type VARCHAR(50),
                topic VARCHAR(100),
                language VARCHAR(10),
                region VARCHAR(50),
                post_datetime TIMESTAMP,
                views INTEGER,
                likes INTEGER,
                comments INTEGER,
                shares INTEGER,
                engagement_rate NUMERIC,
                sentiment_score NUMERIC,
                is_viral BOOLEAN
            );
            -- 2. Details Table (Unique Hashtags)
            CREATE TABLE IF NOT EXISTS hashtags (
                id SERIAL PRIMARY KEY,
                tag VARCHAR(100) UNIQUE NOT NULL
            );
            -- 3. Correlation Table (Connecting Posts to Hashtags)
            CREATE TABLE IF NOT EXISTS post_hashtags (
                post_id VARCHAR(50) REFERENCES posts(post_id),
                hashtag_id INTEGER REFERENCES hashtags(id),
                PRIMARY KEY (post_id, hashtag_id)
            );
        `;
        await client.query(createTableQuery);
        console.log("Tables created successfully with header/details correlation!");
    } catch (err) {
        console.error("Error setting up tables:", err);
    } finally {
        await client.end();
    }
}

setup();