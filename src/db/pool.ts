import pg from 'pg';
import { config } from '../config.js';

export const pool = new pg.Pool({
  connectionString: config.databaseUrl,
  max: config.dbPoolMax,
});

// Without this handler, an error on an idle connection (for example when
// Postgres restarts) would crash the whole process.
pool.on('error', (err) => {
  console.error('idle pg client error', err);
});
