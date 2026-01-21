import csvParser from 'csv-parser';

import fs from 'fs';

import client from './client.js';

const filePath = 'archive/social_media_viral_content_dataset.csv';

async function importRawHashtags() {
    try {
        await client.connect();
        const stream = fs.createReadStream(filePath)
            .pipe(csvParser())
            .on('data', async (row) => {
                const { hashtags } = row;
                // await client.query('INSERT INTO hashtags (tag) VALUES ($1) ON CONFLICT DO NOTHING', [hashtag]);
                console.log(hashtags);
            })
            .on('end', () => {
                console.log('Hashtags imported successfully!');
            })
            .on('error', (err) => {
                console.error('Error reading CSV file:', err);
            });
    } catch (err) {
        console.error('Error importing hashtags:', err);
    } finally {
        await client.end();
    }
}

async function importRawPosts() {
    try {
        await client.connect();
        const stream = fs.createReadStream(filePath)
            .pipe(csvParser())
            .on('data', async (row) => {
                const { post_id, platform, content_type, topic, language, region, post_datetime, views, likes, comments, shares, engagement_rate, sentiment_score, is_viral, hashtag } = row;
                await client.query('INSERT INTO posts (post_id, platform, content_type, topic, language, region, post_datetime, views, likes, comments, shares, engagement_rate, sentiment_score, is_viral) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) ON CONFLICT DO NOTHING', [post_id, platform, content_type, topic, language, region, post_datetime, views, likes, comments, shares, engagement_rate, sentiment_score, is_viral]);
            })
            .on('end', () => {
                console.log('Posts imported successfully!');
            })
            .on('error', (err) => {
                console.error('Error reading CSV file:', err);
            });
    } catch (err) {
        console.error('Error importing posts:', err);
    } finally {
        await client.end();
    }
}

importRawHashtags();