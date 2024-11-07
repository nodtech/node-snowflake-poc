const express = require('express');
const snowflake = require('snowflake-sdk');
const { uuid } = require('uuidv4');
require('dotenv').config();

const app = express();
app.use(express.json());

// Snowflake connection
const connection = snowflake.createConnection({
    account: process.env.SNOWFLAKE_ACCOUNT,
    username: process.env.SNOWFLAKE_USERNAME,
    password: process.env.SNOWFLAKE_PASSWORD,
    warehouse: process.env.SNOWFLAKE_WAREHOUSE,
    database: process.env.SNOWFLAKE_DATABASE,
    schema: process.env.SNOWFLAKE_SCHEMA,
});

connection.connect((err, _) => {
    if (err) {
        console.error('Failed to connect to Snowflake: ' + err.message);
    } else {
        console.log('Successfully connected to Snowflake.');
    }
});

app.post('/api/users', async (req, res) => {
    const { name, location, tag_line, work_experiences, educations } = req.body;

    try {
        const userId = uuid()
        // Insert into users table and get user_id
        const userInsertQuery = `INSERT INTO users (id, name, location, tag_line) VALUES (?, ?, ?, ?);`;
        await connection.execute({
            sqlText: userInsertQuery,
            binds: [userId, name, location, tag_line],
        });

        for (const exp of work_experiences) {
            const workExpQuery = `INSERT INTO work_experiences (user_id, designation, start_date, end_date, org_name) VALUES (?, ?, ?, ?, ?)`;
            await connection.execute({
                sqlText: workExpQuery,
                binds: [userId, exp.designation, exp.start_date, exp.end_date, exp.org_name],
            });
        }

        // Insert educations records
        for (const edu of educations) {
            const eduQuery = `INSERT INTO educations (user_id, title, start_date, end_date)VALUES (?, ?, ?, ?)`;
            await connection.execute({
                sqlText: eduQuery,
                binds: [userId, edu.title, edu.start_date, edu.end_date],
            });
        }

        res.status(201).send({ message: 'User data stored successfully' });

    } catch (error) {
        console.error('Error inserting data: ', error);
        res.status(423).send({ error: 'Failed to store user data' });
    }
});

// Start the server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
