const { Pool } = require("pg");

const pool = new Pool({
  user: "gurpreetkaur",
  host: "localhost",
  database: "fuellink_portal",
  password: "",
  port: 5432,
});

module.exports = pool;