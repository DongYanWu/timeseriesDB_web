const express = require("express");
const multer = require("multer");
const { Pool } = require("pg");
const fs = require("fs");
const path = require("path");
const cors = require("cors");
const app = express();
const port = 3002;
const dayjs = require("dayjs");

app.use(cors());

const pool = new Pool({
  user: "wudongyan",
  host: "localhost",
  database: "postgres",
  password: "huiopzx4",
  port: 5432,
});

// Multer configuration for file uploads
const upload = multer({ dest: "uploads/" });

// Function to create a new table if it doesn't exist
const createTableIfNotExists = async (tableName) => {
  const client = await pool.connect();
  try {
    const query = `
      CREATE TABLE IF NOT EXISTS ${tableName} (
        timestamp TIMESTAMPTZ,
        company VARCHAR(255),
        price NUMERIC
      )
    `;
    await client.query(query);
    console.log(`Table ${tableName} is ready`);
  } catch (err) {
    console.error("Error creating table:", err);
  } finally {
    client.release();
  }
};

// Function to import CSV data into the target table
const importCsvToDb = async (csvFilePath, tableName) => {
  const client = await pool.connect();
  try {
    await createTableIfNotExists(tableName);
    const query = `COPY ${tableName}(timestamp, company, price) FROM '${csvFilePath}' WITH CSV HEADER`;
    await client.query(query);
    console.log(`Imported ${csvFilePath} into the table ${tableName}`);
  } catch (err) {
    console.error("Error importing CSV:", err);
  } finally {
    client.release();
  }
};

// API endpoint to handle CSV file upload
app.post("/upload", upload.single("csvFile"), async (req, res) => {
  const csvFilePath = path.join(__dirname, req.file.path);
  const targetTable = "target_table"; // The name of the new table
  try {
    await importCsvToDb(csvFilePath, targetTable);
    res.status(200).send("File uploaded and imported successfully");
  } catch (err) {
    res.status(500).send("Error uploading file");
  } finally {
    // Delete the file after importing
    fs.unlinkSync(csvFilePath);
  }
});

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

// app.get("/similariy", async (req, res) => {
//   const { targetCompany, company, startTimestamp, endTimestamp, methodChoose } =
//     req.query;
//   const client = await pool.connect();

//   // Ensure timestamps are in the correct format
//   const formattedStartTimestamp = dayjs(startTimestamp).toISOString();
//   const formattedEndTimestamp = dayjs(endTimestamp).toISOString();

//   console.log("Received request with parameters:", {
//     targetCompany,
//     company,
//     startTimestamp: formattedStartTimestamp,
//     endTimestamp: formattedEndTimestamp,
//     methodChoose: methodChoose,
//   });

//   try {
//     const query = `
//     SELECT *
//     FROM calculate_dtw_with_timerange(
//       $1::text, $2::text[], $3::boolean, $4::timestamp, $5::timestamp
//     );
//   `;
//     const values = [
//       targetCompany,
//       company ? company.split(",") : [],
//       true,
//       formattedStartTimestamp,
//       formattedEndTimestamp,
//     ];

//     console.log("Executing query:", query);
//     console.log("With values:", values);

//     const result = await client.query(query, values);

//     console.log("Query result:", result.rows);

//     res.json(result.rows);
//   } catch (err) {
//     console.error("Error executing query:", err);
//     res.status(500).send("Error fetching data");
//   } finally {
//     client.release();
//   }
// });

app.get("/similarity", async (req, res) => {
  const { targetCompany, company, startTimestamp, endTimestamp, methodChoose } =
    req.query;
  const client = await pool.connect();

  // Ensure timestamps are in the correct format
  const formattedStartTimestamp = dayjs(startTimestamp).toISOString();
  const formattedEndTimestamp = dayjs(endTimestamp).toISOString();

  console.log("Received request with parameters:", {
    targetCompany,
    company,
    startTimestamp: formattedStartTimestamp,
    endTimestamp: formattedEndTimestamp,
    methodChoose: methodChoose,
  });

  let query;

  if (methodChoose === "0") {
    query = `
      SELECT * 
      FROM calculate_pure_euclidean_with_timerange(
        $1::text, $2::text[], $3::boolean, $4::timestamp, $5::timestamp
      );
    `;
  } else if (methodChoose === "1") {
    query = `
      SELECT * 
      FROM calculate_vshift_with_timerange(
        $1::text, $2::text[], $3::boolean, $4::timestamp, $5::timestamp
      );
    `;
  } else {
    query = `
      SELECT * 
      FROM calculate_dtw_with_timerange(
        $1::text, $2::text[], $3::boolean, $4::timestamp, $5::timestamp
      );
    `;
  }

  try {
    const values = [
      targetCompany,
      company ? company.split(",") : [],
      true,
      formattedStartTimestamp,
      formattedEndTimestamp,
    ];

    console.log("Executing query:", query);
    console.log("With values:", values);

    const result = await client.query(query, values);

    console.log("Query result:", result.rows);

    res.json(result.rows);
  } catch (err) {
    console.error("Error executing query:", err);
    res.status(500).send("Error fetching data");
  } finally {
    client.release();
  }
});

app.get("/similaritywithouttime", async (req, res) => {
  const { targetCompany, company, methodChoose } = req.query;
  const client = await pool.connect();

  console.log("Received request with parameters:", {
    targetCompany,
    company,
    methodChoose: methodChoose,
  });

  let query;

  if (methodChoose === "0") {
    query = `
      SELECT * 
      FROM calculate_pure_euclidean_results(
        $1::text, $2::text[], $3::boolean
      );
    `;
  } else if (methodChoose === "1") {
    query = `
      SELECT * 
      FROM calculate_vshift_results(
        $1::text, $2::text[], $3::boolean
      );
    `;
  } else {
    query = `
      SELECT * 
      FROM calculate_dtw_results(
        $1::text, $2::text[], $3::boolean
      );
    `;
  }

  try {
    const values = [targetCompany, company ? company.split(",") : [], true];

    console.log("Executing query:", query);
    console.log("With values:", values);

    const result = await client.query(query, values);

    console.log("Query result:", result.rows);

    res.json(result.rows);
  } catch (err) {
    console.error("Error executing query:", err);
    res.status(500).send("Error fetching data");
  } finally {
    client.release();
  }
});
const PORT = process.env.PORT || 3003; // Changed the port to 3003 or another available port
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
// API endpoint to upload CSV files
// app.post("/upload", upload.single("file"), async (req, res) => {
//   const filePath = req.file.path;
//   const tableName = req.body.tableName; // specify table name in the request body

//   if (!tableName) {
//     return res.status(400).send("Table name is required");
//   }

//   try {
//     await importCsvToDb(filePath, tableName);
//     res.send(`File uploaded and data imported into ${tableName}`);
//   } catch (err) {
//     res.status(500).send("Error uploading file");
//   } finally {
//     fs.unlinkSync(filePath); // Remove the file after processing
//   }
// });

// API endpoint to fetch paginated and filtered data from a table
app.get("/data/:tableName", async (req, res) => {
  const tableName = req.params.tableName;
  const limit = parseInt(req.query.limit, 10) || 15; // Default limit is 15
  const offset = parseInt(req.query.offset, 10) || 0; // Default offset is 0
  const company = req.query.company ? req.query.company.trim() : null;
  const startTimestamp = req.query.startTimestamp || null;
  const endTimestamp = req.query.endTimestamp || null;
  const price = req.query.price;

  console.log("startTimestamp:", startTimestamp); // Debugging line
  console.log("endTimestamp:", endTimestamp); // Debugging line

  const client = await pool.connect();
  try {
    let query = `SELECT * FROM ${tableName} WHERE 1=1`;
    const queryParams = [];
    let paramIndex = 1; // Parameter index for PostgreSQL

    if (company) {
      query += ` AND company = $${paramIndex++}`;
      queryParams.push(company);
    }

    if (startTimestamp) {
      query += ` AND timestamp >= $${paramIndex++}`;
      queryParams.push(startTimestamp);
    }

    if (endTimestamp) {
      query += ` AND timestamp < $${paramIndex++}`;
      queryParams.push(endTimestamp);
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
