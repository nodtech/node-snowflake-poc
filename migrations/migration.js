const snowflake = require('snowflake-sdk');
require('dotenv').config();

// Snowflake connection setup
const connection = snowflake.createConnection({
  account: process.env.SNOWFLAKE_ACCOUNT,
  username: process.env.SNOWFLAKE_USERNAME,
  password: process.env.SNOWFLAKE_PASSWORD,
  warehouse: process.env.SNOWFLAKE_WAREHOUSE,
  database: process.env.SNOWFLAKE_DATABASE,
  schema: process.env.SNOWFLAKE_SCHEMA,
});

// Connect to Snowflake
connection.connect((err) => {
  if (err) {
    console.error('Unable to connect to Snowflake: ' + err.message);
    return;
  }
  console.log('Connected to Snowflake.');
  runMigrations();
});

const snowflakeMigrationSql = [
    `
    CREATE TABLE IF NOT EXISTS users (
        id STRING DEFAULT UUID_STRING(),
        name STRING,
        location STRING,
        tag_line STRING        
    );
    `,
    `
    CREATE TABLE IF NOT EXISTS work_experiences (
        id STRING DEFAULT UUID_STRING(),
        user_id STRING,
        designation STRING,
        start_date DATE,
        end_date DATE,
        org_name STRING
    );
    `,
    `
    CREATE TABLE IF NOT EXISTS educations (
        id STRING DEFAULT UUID_STRING(),
        user_id STRING,
        title STRING,
        start_date DATE,
        end_date DATE
    );
    `,
]

async function runMigrations() {
  for (const sql of snowflakeMigrationSql) {
    try {
      await connection.execute({
        sqlText: sql,
        complete: (err) => {
          if (err) {
            console.error(`Error executing:`, err);
          } else {
            console.log(`Successfully executed`);
          }
        }
      });
    } catch (err) {
      console.error(`Failed to execute migration: ${err.message}`);
    }
  }

  connection.destroy((err) => {
    if (err) {
      console.error('Failed to close connection: ' + err.message);
    } else {
      console.log('Connection closed.');
    }
  });
}
