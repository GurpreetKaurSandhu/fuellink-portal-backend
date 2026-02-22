const express = require("express");
const router = express.Router();
const fs = require("fs");
const path = require("path");
const multer = require("multer");
const XLSX = require("xlsx");
const pool = require("../db");
const authMiddleware = require("../middleware/authMiddleware");

const uploadDir = path.join(__dirname, "../uploads/rates");
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir, { recursive: true });
}

const storage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, uploadDir),
  filename: (_req, file, cb) => {
    const ext = path.extname(file.originalname || "").toLowerCase();
    const unique = `${Date.now()}-${Math.round(Math.random() * 1e9)}`;
    cb(null, `${unique}${ext}`);
  },
});

const upload = multer({ storage });

const normalizeHeader = (value) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[\s_-]+/g, "");

const isValidDateString = (value) => {
  if (!value) return false;
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(value))) return false;
  const d = new Date(value);
  return !Number.isNaN(d.getTime());
};

const formatDate = (value) => {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  return String(value);
};

const resolveCustomerId = async (customer_id, customer_number) => {
  const customerNumber = String(customer_number || "").trim();
  if (customerNumber) {
    const byNumber = await pool.query(
      "SELECT id FROM customers WHERE customer_number = $1",
      [customerNumber]
    );
    if (byNumber.rows.length === 0) {
      throw new Error("customer_number does not exist");
    }
    return byNumber.rows[0].id;
  }

  const customerIdNum = parseInt(customer_id, 10);
  if (!Number.isInteger(customerIdNum) || customerIdNum < 1) {
    throw new Error("Invalid customer_id");
  }
  const byId = await pool.query("SELECT id FROM customers WHERE id = $1", [customerIdNum]);
  if (byId.rows.length === 0) {
    throw new Error("customer_id does not exist");
  }
  return customerIdNum;
};

const parseRatesFile = (filePath) => {
  const workbook = XLSX.readFile(filePath, { cellDates: true, raw: true });
  const sheetName = workbook.SheetNames[0];
  if (!sheetName) {
    throw new Error("No worksheet found in file");
  }

  const sheet = workbook.Sheets[sheetName];
  const rows = XLSX.utils.sheet_to_json(sheet, { defval: null });

  if (!Array.isArray(rows) || rows.length === 0) {
    throw new Error("File is empty or missing header row");
  }

  const headerMap = {};
  for (const key of Object.keys(rows[0] || {})) {
    const normalized = normalizeHeader(key);
    if (normalized === "sitename") headerMap.site_name = key;
    if (normalized === "province") headerMap.province = key;
    if (normalized === "price") headerMap.price = key;
  }

  if (!headerMap.site_name || !headerMap.province || !headerMap.price) {
    throw new Error("Missing required columns: site_name, province, price");
  }

  const parsed = [];

  rows.forEach((row, idx) => {
    const rawSite = row[headerMap.site_name];
    const rawProvince = row[headerMap.province];
    const rawPrice = row[headerMap.price];

    if (rawSite == null && rawProvince == null && rawPrice == null) {
      return;
    }

    const site_name = String(rawSite || "").trim();
    const province = String(rawProvince || "").trim();

    let price = rawPrice;
    if (typeof price === "string") {
      price = price.replace(/[$,]/g, "").trim();
    }
    const priceNum = typeof price === "number" ? price : parseFloat(String(price || ""));

    if (!site_name) {
      throw new Error(`Missing site_name at row ${idx + 2}`);
    }
    if (!province) {
      throw new Error(`Missing province at row ${idx + 2}`);
    }
    if (province.length > 10) {
      throw new Error(`Province too long at row ${idx + 2}`);
    }
    if (!Number.isFinite(priceNum)) {
      throw new Error(`Invalid price at row ${idx + 2}`);
    }

    parsed.push({ site_name, province, price: priceNum });
  });

  if (parsed.length === 0) {
    throw new Error("No valid data rows found");
  }

  return parsed;
};

router.post("/upload", authMiddleware, upload.single("file"), async (req, res) => {
  try {
    if (!req.user || req.user.role !== "admin") {
      return res.status(403).json({ message: "Access denied" });
    }

    if (!req.file) {
      return res.status(400).json({ message: "No file received" });
    }

    const ext = path.extname(req.file.originalname || "").toLowerCase();
    if (ext !== ".xlsx" && ext !== ".csv") {
      return res.status(400).json({ message: "Invalid file type. Use .xlsx or .csv" });
    }

    let customerIdNum;
    try {
      customerIdNum = await resolveCustomerId(req.body.customer_id, req.body.customer_number);
    } catch (err) {
      return res.status(400).json({ message: err.message });
    }

    const effectiveDate = req.body.effective_date ? String(req.body.effective_date).trim() : null;
    if (effectiveDate && !isValidDateString(effectiveDate)) {
      return res.status(400).json({ message: "Invalid effective_date (use YYYY-MM-DD)" });
    }

    let rows;
    try {
      rows = parseRatesFile(req.file.path);
    } catch (err) {
      return res.status(400).json({ message: err.message });
    }

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      const fileResult = await client.query(
        `INSERT INTO rates_files
         (customer_id, original_filename, stored_filename, uploaded_by_user_id, effective_date)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING id, effective_date`,
        [
          customerIdNum,
          req.file.originalname,
          req.file.filename,
          req.user.id || null,
          effectiveDate || null,
        ]
      );

      const ratesFileId = fileResult.rows[0].id;

      const values = [];
      const placeholders = rows.map((row, idx) => {
        const base = idx * 5;
        values.push(ratesFileId, customerIdNum, row.site_name, row.province, row.price);
        return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5})`;
      });

      await client.query(
        `INSERT INTO rates_lines
         (rates_file_id, customer_id, site_name, province, price)
         VALUES ${placeholders.join(",")}`,
        values
      );

      await client.query("COMMIT");

      res.json({
        message: "Rates uploaded",
        rates_file_id: ratesFileId,
        effective_date: formatDate(fileResult.rows[0].effective_date),
        rows_inserted: rows.length,
      });
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Server error" });
  }
});

router.get("/", authMiddleware, async (req, res) => {
  try {
    const { customer_id, effective_date, province, site_name } = req.query;
    const isAdmin = req.user && req.user.role === "admin";

    let customerIdFilter = null;
    if (isAdmin) {
      if (customer_id) {
        const customerIdNum = parseInt(customer_id, 10);
        if (!Number.isInteger(customerIdNum) || customerIdNum < 1) {
          return res.status(400).json({ message: "Invalid customer_id" });
        }
        customerIdFilter = customerIdNum;
      }
    } else {
      if (!req.user || !req.user.customer_id) {
        return res.status(400).json({ message: "Missing customer_id" });
      }
      customerIdFilter = req.user.customer_id;
    }

    let effectiveDate = null;
    if (effective_date) {
      if (!isValidDateString(effective_date)) {
        return res.status(400).json({ message: "Invalid effective_date (use YYYY-MM-DD)" });
      }
      effectiveDate = String(effective_date).trim();
    } else {
      const efValues = [];
      let efWhere = "";
      if (customerIdFilter) {
        efValues.push(customerIdFilter);
        efWhere = "WHERE customer_id = $1";
      }
      const efResult = await pool.query(
        `SELECT effective_date
         FROM rates_files
         ${efWhere}
         ORDER BY effective_date DESC, id DESC
         LIMIT 1`,
        efValues
      );

      if (efResult.rows.length === 0) {
        return res.json({ effective_date: null, data: [] });
      }

      effectiveDate = efResult.rows[0].effective_date;
    }

    const where = [];
    const values = [];
    const addClause = (clause, value) => {
      values.push(value);
      where.push(clause.replace("?", `$${values.length}`));
    };

    addClause("rf.effective_date = ?", effectiveDate);

    if (customerIdFilter) {
      addClause("rl.customer_id = ?", customerIdFilter);
    }

    if (province) {
      addClause("rl.province ILIKE ?", String(province).trim());
    }

    if (site_name) {
      addClause("rl.site_name ILIKE ?", `%${String(site_name).trim()}%`);
    }

    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

    const result = await pool.query(
      `SELECT rl.id, rl.rates_file_id, rl.customer_id, rl.site_name,
              rl.province, rl.price, rl.created_at, rf.effective_date
       FROM rates_lines rl
       JOIN rates_files rf ON rl.rates_file_id = rf.id
       ${whereSql}
       ORDER BY rl.site_name ASC, rl.id ASC`,
      values
    );

    res.json({
      effective_date: formatDate(effectiveDate),
      data: result.rows,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Server error" });
  }
});

module.exports = router;
