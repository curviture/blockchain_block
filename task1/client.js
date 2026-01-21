import { Client } from 'pg';
const client = new Client({
    user: 'postgres',
    host: 'localhost',
    database: 'postgres', // Or your specific DB name
    password: 'postgres',
    port: 5432,
});


export default client;