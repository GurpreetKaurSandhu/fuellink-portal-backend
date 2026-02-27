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

/**
 * Robust header normalization:
 * - remove newlines/tabs
 * - remove # and punctuation
 * - collapse spaces
 * - lowercase
 * - remove spaces for stable matching
 */
const normalizeHeader = (value) => {
  let text = String(value || "");
  text = text.replace(/[\r\n\t]+/g, " ");
  text = text.replace(/#+/g, " ");
  text = text.replace(/[^a-zA-Z0-9 ]+/g, " ");
  text = text.replace(/\s+/g, " ").trim().toLowerCase();
  return text.replace(/\s+/g, "");
};

const isValidDateString = (value) => {
  if (!value) return false;
  const v = String(value).trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(v)) return false;
  const d = new Date(v);
  return !Number.isNaN(d.getTime());
};

const formatDate = (value) => {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  return String(value).slice(0, 10);
};

/**
 * Resolve customer_id for CUSTOMER-SPECIFIC uploads.
 * For BASE uploads: return null (allowed).
 *
 * Priority:
 * 1) customer_number (trim, case-insensitive)
 * 2) customer_id (integer)
 * 3) nothing provided => base upload => null
 */
const resolveCustomerIdOrNull = async (customer_id, customer_number) => {
  const customerNumber = String(customer_number || "").trim();
  if (customerNumber) {
    const byNumber = await pool.query(
      "SELECT id FROM customers WHERE lower(trim(customer_number)) = lower($1) LIMIT 1",
      [customerNumber]
    );
    if (byNumber.rows.length === 0) {
      throw new Error("customer_number does not exist");
    }
    return byNumber.rows[0].id;
  }

  const rawId = String(customer_id || "").trim();
  if (!rawId) {
    // BASE rates upload
    return null;
  }

  const customerIdNum = parseInt(rawId, 10);
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
  if (!sheetName) throw new Error("No worksheet found in file");

  const sheet = workbook.Sheets[sheetName];
  const rows = XLSX.utils.sheet_to_json(sheet, { defval: null });

  if (!Array.isArray(rows) || rows.length === 0) {
    throw new Error("File is empty or missing header row");
  }

  const headerMap = {};

  // accept variations
  for (const key of Object.keys(rows[0] || {})) {
    const normalized = normalizeHeader(key);

    // site name
    if (!headerMap.site_name && (normalized === "sitename" || normalized === "site" || normalized === "location")) {
      headerMap.site_name = key;
    }

    // province
    if (
      !headerMap.province &&
      (normalized === "province" || normalized === "prov" || normalized === "prv" || normalized === "pr")
    ) {
      headerMap.province = key;
    }

    // price
    if (
      !headerMap.base_price &&
      (normalized === "price" || normalized === "price*" || normalized === "rate" || normalized === "priceperlitre")
    ) {
      headerMap.base_price = key;
    }
  }

  if (!headerMap.site_name || !headerMap.province || !headerMap.base_price) {
    throw new Error("Missing required columns: Site Name, Province, Price");
  }

  const parsed = [];

  rows.forEach((row, idx) => {
    const rawSite = row[headerMap.site_name];
    const rawProvince = row[headerMap.province];
    const rawPrice = row[headerMap.base_price];

    // skip totally blank lines
    if (rawSite == null && rawProvince == null && rawPrice == null) return;

    const site_name = String(rawSite || "").trim();
    const province = String(rawProvince || "").trim();

    let basePrice = rawPrice;
    if (typeof basePrice === "string") {
      basePrice = basePrice.replace(/[$,]/g, "").trim();
    }
    const basePriceNum =
      typeof basePrice === "number" ? basePrice : parseFloat(String(basePrice || ""));

    if (!site_name) throw new Error(`Missing site_name at row ${idx + 2}`);
    if (!province) throw new Error(`Missing province at row ${idx + 2}`);
    if (province.length > 10) throw new Error(`Province too long at row ${idx + 2}`);
    if (!Number.isFinite(basePriceNum)) throw new Error(`Invalid price at row ${idx + 2}`);

    parsed.push({ site_name, province, base_price: basePriceNum });
  });

  if (parsed.length === 0) throw new Error("No valid data rows found");

  return parsed;
};

const ensureRatesSchemaCompat = async (client) => {
  // Make base-rates model work even on older DBs.
  await client.query("ALTER TABLE rates_files ALTER COLUMN customer_id DROP NOT NULL");
  await client.query("ALTER TABLE rates_lines ALTER COLUMN customer_id DROP NOT NULL");
  await client.query("ALTER TABLE rates_lines ADD COLUMN IF NOT EXISTS base_price NUMERIC(10,4)");

  await client.query(`
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'rates_lines'
          AND column_name = 'price'
      ) THEN
        UPDATE rates_lines
        SET base_price = price
        WHERE base_price IS NULL AND price IS NOT NULL;
      END IF;
    END $$;
  `);
};

/**
 * POST /api/rates/upload
 * Admin can upload:
 * - BASE rates (customer_id NULL) if no customer_id/customer_number provided
 * - CUSTOMER rates if customer_id/customer_number provided
 */
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

    // customerIdNum can be null for BASE upload
    let customerIdNum;
    try {
      customerIdNum = await resolveCustomerIdOrNull(req.body.customer_id, req.body.customer_number);
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
      await ensureRatesSchemaCompat(client);

      const rfColsResult = await client.query(
        `SELECT column_name
         FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name = 'rates_files'`
      );
      const rfCols = new Set(rfColsResult.rows.map((r) => r.column_name));

      const fileColumns = [];
      const fileValues = [];
      const addFileColumn = (name, value) => {
        if (rfCols.has(name)) {
          fileColumns.push(name);
          fileValues.push(value);
        }
      };
      addFileColumn("customer_id", customerIdNum);
      addFileColumn("original_filename", req.file.originalname);
      addFileColumn("stored_filename", req.file.filename);
      addFileColumn("uploaded_by_user_id", req.user.id || null);
      addFileColumn("effective_date", effectiveDate || null);

      if (fileColumns.length < 2) {
        throw new Error("rates_files schema is missing required columns");
      }

      const fileResult = await client.query(
        `INSERT INTO rates_files
         (${fileColumns.join(", ")})
         VALUES (${fileColumns.map((_, idx) => `$${idx + 1}`).join(", ")})
         RETURNING id, effective_date, customer_id`,
        fileValues
      );

      const ratesFileId = fileResult.rows[0].id;

      const rlColsResult = await client.query(
        `SELECT column_name
         FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name = 'rates_lines'`
      );
      const rlCols = new Set(rlColsResult.rows.map((r) => r.column_name));
      const hasBasePrice = rlCols.has("base_price");
      const hasPrice = rlCols.has("price");
      if (!hasBasePrice && !hasPrice) {
        throw new Error("rates_lines is missing price/base_price column");
      }

      // Insert lines (supports legacy price and new base_price schemas)
      const lineColumns = ["rates_file_id", "customer_id", "site_name", "province"];
      if (hasBasePrice) lineColumns.push("base_price");
      if (hasPrice) lineColumns.push("price");

      const values = [];
      const placeholders = rows.map((row, idx) => {
        const rowValues = [ratesFileId, customerIdNum, row.site_name, row.province];
        if (hasBasePrice) rowValues.push(row.base_price);
        if (hasPrice) rowValues.push(row.base_price);
        const base = values.length;
        values.push(...rowValues);
        return `(${rowValues.map((_, valueIdx) => `$${base + valueIdx + 1}`).join(", ")})`;
      });

      await client.query(
        `INSERT INTO rates_lines
         (${lineColumns.join(", ")})
         VALUES ${placeholders.join(",")}`,
        values
      );

      await client.query("COMMIT");

      res.json({
        message: customerIdNum ? "Customer rates uploaded" : "Base rates uploaded",
        rates_file_id: ratesFileId,
        customer_id: customerIdNum,
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
    console.error("Rates upload error:", err);
    res.status(500).json({ message: "Server error" });
  }
});

/**
 * GET /api/rates/dates
 * Used by frontend dropdown.
 * Admin:
 *  - if customer_id provided => dates for that customer
 *  - else => dates for BASE (customer_id IS NULL)
 * Customer:
 *  - dates for their own customer_id
 */
router.get("/dates", authMiddleware, async (req, res) => {
  try {
    const isAdmin = req.user && req.user.role === "admin";
    const values = [];
    let whereSql = "WHERE customer_id IS NULL";

    if (isAdmin && req.query.customer_id) {
      const n = parseInt(String(req.query.customer_id), 10);
      if (!Number.isInteger(n) || n < 1) {
        return res.status(400).json({ message: "Invalid customer_id" });
      }
      values.push(n);
      whereSql = "WHERE customer_id = $1";
    }

    const result = await pool.query(
      `SELECT DISTINCT effective_date
       FROM rates_files
       ${whereSql}
       ORDER BY effective_date DESC NULLS LAST`,
      values
    );

    res.json(result.rows.map((r) => formatDate(r.effective_date)));
  } catch (err) {
    console.error("Rates dates error:", err);
    res.status(500).json({ message: "Failed to load rate dates" });
  }
});

/**
 * GET /api/rates
 * Admin:
 *  - if customer_id provided => customer rates
 *  - else => BASE rates (customer_id IS NULL)
 * Customer:
 *  - only their own customer_id
 */
router.get("/", authMiddleware, async (req, res) => {
  try {
    const { customer_id, effective_date, province, site_name } = req.query;
    const isAdmin = req.user && req.user.role === "admin";

    if (!isAdmin) {
      if (!req.user || !req.user.customer_id) {
        return res.status(400).json({ message: "Missing customer_id" });
      }

      let effectiveDate = null;
      if (effective_date) {
        if (!isValidDateString(effective_date)) {
          return res.status(400).json({ message: "Invalid effective_date (use YYYY-MM-DD)" });
        }
        effectiveDate = String(effective_date).trim();
      } else {
        const efResult = await pool.query(
          `SELECT effective_date
           FROM rates_files
           WHERE customer_id IS NULL
           ORDER BY effective_date DESC NULLS LAST, id DESC
           LIMIT 1`
        );
        if (efResult.rows.length === 0) {
          return res.json({ effective_date: null, data: [] });
        }
        effectiveDate = efResult.rows[0].effective_date;
      }

      const values = [req.user.customer_id, effectiveDate];
      const where = [
        "rf.customer_id IS NULL",
        "rl.customer_id IS NULL",
        "rl.base_price IS NOT NULL",
        "rf.effective_date = $2",
      ];

      if (province) {
        values.push(String(province).trim());
        where.push(`rl.province ILIKE $${values.length}`);
      }
      if (site_name) {
        values.push(`%${String(site_name).trim()}%`);
        where.push(`rl.site_name ILIKE $${values.length}`);
      }

      const result = await pool.query(
        `SELECT
           rl.id,
           rl.rates_file_id,
           rl.site_name,
           rl.province,
           rl.base_price,
           COALESCE(rg.markup_per_liter, 0) AS markup_per_liter,
           (rl.base_price + COALESCE(rg.markup_per_liter, 0))::numeric AS price,
           rf.effective_date
         FROM rates_lines rl
         JOIN rates_files rf ON rl.rates_file_id = rf.id
         JOIN customers c ON c.id = $1
         LEFT JOIN rate_groups rg ON rg.id = c.rate_group_id
         WHERE ${where.join(" AND ")}
         ORDER BY rl.site_name ASC, rl.id ASC`,
        values
      );

      return res.json({
        effective_date: formatDate(effectiveDate),
        data: result.rows,
      });
    }

    let customerIdFilter = null;
    if (customer_id) {
      const customerIdNum = parseInt(String(customer_id), 10);
      if (!Number.isInteger(customerIdNum) || customerIdNum < 1) {
        return res.status(400).json({ message: "Invalid customer_id" });
      }
      customerIdFilter = customerIdNum;
    }

    let effectiveDate = null;
    if (effective_date) {
      if (!isValidDateString(effective_date)) {
        return res.status(400).json({ message: "Invalid effective_date (use YYYY-MM-DD)" });
      }
      effectiveDate = String(effective_date).trim();
    } else {
      const efValues = [];
      let efWhere = "WHERE customer_id IS NULL";
      if (customerIdFilter != null) {
        efValues.push(customerIdFilter);
        efWhere = "WHERE customer_id = $1";
      }

      const efResult = await pool.query(
        `SELECT effective_date
         FROM rates_files
         ${efWhere}
         ORDER BY effective_date DESC NULLS LAST, id DESC
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
    if (customerIdFilter == null) {
      where.push("rl.customer_id IS NULL");
      where.push("rf.customer_id IS NULL");
    } else {
      addClause("rl.customer_id = ?", customerIdFilter);
    }
    if (province) {
      addClause("rl.province ILIKE ?", String(province).trim());
    }
    if (site_name) {
      addClause("rl.site_name ILIKE ?", `%${String(site_name).trim()}%`);
    }

    const result = await pool.query(
      `SELECT rl.id, rl.rates_file_id, rl.customer_id, rl.site_name,
              rl.province, rl.base_price,
              rl.base_price AS price,
              rl.created_at, rf.effective_date
       FROM rates_lines rl
       JOIN rates_files rf ON rl.rates_file_id = rf.id
       WHERE ${where.join(" AND ")}
       ORDER BY rl.site_name ASC, rl.id ASC`,
      values
    );

    return res.json({
      effective_date: formatDate(effectiveDate),
      data: result.rows,
    });
  } catch (err) {
    console.error("Rates get error:", err);
    res.status(500).json({ message: "Server error" });
  }
});

module.exports = router;
