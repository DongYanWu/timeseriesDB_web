const express = require("express");
const multer = require("multer");
const { Pool } = require("pg");
const fs = require("fs");
const path = require("path");
const cors = require("cors");

const app = express();
const port = 3002; // Ensure this port matches your environment variable

// Enable CORS for all routes
app.use(cors());

// PostgreSQL connection pool
const pool = new Pool({
  user: "wudongyan",
  host: "localhost",
  database: "postgres",
  password: "huiopzx4",
  port: 5432,
});

// Multer configuration for file uploads
const upload = multer({ dest: "uploads/" });

// Function to import CSV data into the hypertable
async function importCsvToDb(filePath, tableName) {
  const client = await pool.connect();
  try {
    const query = `COPY ${tableName}(timestamp, company, price) FROM '${filePath}' WITH CSV HEADER`;
    await client.query(query);
  } catch (err) {
    console.error(`Error importing ${filePath}:`, err);
  } finally {
    client.release();
  }
}

// API endpoint to upload CSV files
app.post("/upload", upload.single("file"), async (req, res) => {
  const filePath = req.file.path;
  const tableName = req.body.tableName; // specify table name in the request body

  if (!tableName) {
    return res.status(400).send("Table name is required");
  }

  try {
    await importCsvToDb(filePath, tableName);
    res.send(`File uploaded and data imported into ${tableName}`);
  } catch (err) {
    res.status(500).send("Error uploading file");
  } finally {
    fs.unlinkSync(filePath); // Remove the file after processing
  }
});

// API endpoint to fetch distinct company names from a table
app.get("/companies/:tableName", async (req, res) => {
  const tableName = req.params.tableName;

  const client = await pool.connect();
  try {
    const query = `SELECT DISTINCT company FROM ${tableName} ORDER BY company`;
    const result = await client.query(query);
    res.json(result.rows);
  } catch (err) {
    console.error(`Error fetching company names from ${tableName}:`, err);
    res.status(500).send("Error fetching company names");
  } finally {
    client.release();
  }
});

// API endpoint to fetch paginated and filtered data from a table
app.get("/data/:tableName", async (req, res) => {
  const tableName = req.params.tableName;
  const limit = parseInt(req.query.limit, 10) || 15; // Default limit is 15
  const offset = parseInt(req.query.offset, 10) || 0; // Default offset is 0
  const company = req.query.company ? req.query.company.trim() : null;
  const timestamp = req.query.timestamp;
  const price = req.query.price;

  const client = await pool.connect();
  try {
    let query = `SELECT * FROM ${tableName} WHERE 1=1`;
    const queryParams = [];
    let paramIndex = 1; // Parameter index for PostgreSQL

    if (company) {
      query += ` AND company = $${paramIndex++}`;
      queryParams.push(company);
    }

    if (timestamp) {
      query += ` AND timestamp = $${paramIndex++}`;
      queryParams.push(timestamp);
    }

    if (price) {
      query += ` AND price = $${paramIndex++}`;
      queryParams.push(price);
    }

    query += ` ORDER BY id LIMIT $${paramIndex++} OFFSET $${paramIndex}`;
    queryParams.push(limit);
    queryParams.push(offset);

    console.log("SQL Query:", query); // Debugging line
    console.log("Query Params:", queryParams); // Debugging line

    const result = await client.query(query, queryParams);
    res.json(result.rows);
  } catch (err) {
    console.error(`Error fetching data from ${tableName}:`, err.message);
    res.status(500).send(`Error fetching data: ${err.message}`);
  } finally {
    client.release();
  }
});

app.listen(port, () => {
  console.log(`Server running on http://localhost:${port}`);
});
