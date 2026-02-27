const { Pool } = require("pg");

const connectionString = process.env.DATABASE_URL;

if (!connectionString) {
  console.error("❌ DATABASE_URL is not set");
}

const pool = new Pool({
  connectionString,
  // Render Postgres requires SSL in most cases
  ssl: connectionString ? { rejectUnauthorized: false } : false,
});

module.exports = pool;
