const express = require("express");
const fs = require("fs");
const path = require("path");
const multer = require("multer");
const XLSX = require("xlsx");
const pool = require("../db");
const authMiddleware = require("../middleware/authMiddleware");

const router = express.Router();

const uploadDir = path.join(__dirname, "../uploads/billed-transactions");
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir, { recursive: true });
}

const storage = multer.diskStorage({
  destination: (_req, _file, cb) => cb(null, uploadDir),
  filename: (_req, file, cb) => {
    const ext = path.extname(file.originalname || "").toLowerCase();
    const safeName = String(file.originalname || "file")
      .replace(/\s+/g, "-")
      .replace(/[^a-zA-Z0-9._-]/g, "");
    const finalName = ext && !safeName.endsWith(ext) ? `${safeName}${ext}` : safeName;
    cb(null, `${Date.now()}-${Math.round(Math.random() * 1e9)}-${finalName}`);
  },
});

const upload = multer({
  storage,
  limits: { fileSize: 30 * 1024 * 1024 },
});

const isAdmin = (req) => req.user && req.user.role === "admin";
let schemaInitPromise = null;

const ensureSchema = async () => {
  if (!schemaInitPromise) {
    schemaInitPromise = (async () => {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS billed_transaction_uploads (
          id BIGSERIAL PRIMARY KEY,
          uploaded_by_user_id INTEGER,
          original_filename TEXT NOT NULL,
          stored_filename TEXT NOT NULL,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
      `);

      await pool.query(`
        CREATE TABLE IF NOT EXISTS billed_transactions (
          id BIGSERIAL PRIMARY KEY,
          upload_id BIGINT REFERENCES billed_transaction_uploads(id) ON DELETE SET NULL,
          customer_id INTEGER REFERENCES customers(id) ON DELETE SET NULL,
          company_name TEXT,
          card_number TEXT,
          transaction_date DATE,
          location TEXT,
          city TEXT,
          province TEXT,
          document_number TEXT,
          product TEXT,
          volume_liters NUMERIC(14, 3),
          base_rate NUMERIC(14, 4),
          fet NUMERIC(14, 4),
          pft NUMERIC(14, 4),
          rate_per_ltr NUMERIC(14, 4),
          subtotal NUMERIC(14, 2),
          gst NUMERIC(14, 2),
          pst NUMERIC(14, 2),
          qst NUMERIC(14, 2),
          amount NUMERIC(14, 2),
          driver_name TEXT,
          invoice_number TEXT,
          invoice_date DATE,
          period_billed TEXT,
          raw_row JSONB,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
      `);

      await pool.query(`CREATE INDEX IF NOT EXISTS idx_billed_transactions_customer ON billed_transactions(customer_id, transaction_date DESC)`);
      await pool.query(`CREATE INDEX IF NOT EXISTS idx_billed_transactions_invoice ON billed_transactions(invoice_number)`);
      await pool.query(`CREATE INDEX IF NOT EXISTS idx_billed_transactions_upload ON billed_transactions(upload_id)`);
    })().catch((err) => {
      schemaInitPromise = null;
      throw err;
    });
  }
  return schemaInitPromise;
};

const normalizeHeader = (value) =>
  String(value || "")
    .replace(/[\r\n\t]+/g, " ")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");

const normalizeName = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");

const parseNumber = (value) => {
  if (value == null || value === "") return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  const parsed = parseFloat(String(value).replace(/[$,]/g, "").trim());
  return Number.isFinite(parsed) ? parsed : null;
};

const parseDateOnly = (value) => {
  if (!value) return null;
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) return null;
    return value.toISOString().slice(0, 10);
  }
  const raw = String(value).trim();
  if (!raw) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return raw;
  if (/^\d{4}\/\d{2}\/\d{2}$/.test(raw)) return raw.replace(/\//g, "-");
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
};

const parseSheetRows = (filePath) => {
  const workbook = XLSX.readFile(filePath, { cellDates: true, raw: true });
  const sheetName = workbook.SheetNames[0];
  if (!sheetName) throw new Error("No worksheet found in file");
  const sheet = workbook.Sheets[sheetName];
  return XLSX.utils.sheet_to_json(sheet, { defval: null });
};

const buildHeaderMap = (firstRow) => {
  const map = {};
  for (const key of Object.keys(firstRow || {})) {
    const h = normalizeHeader(key);
    if (h === "card") map.card_number = key;
    if (h === "companyname") map.company_name = key;
    if (h === "date") map.transaction_date = key;
    if (h === "location") map.location = key;
    if (h === "city") map.city = key;
    if (h === "prov") map.province = key;
    if (h === "document") map.document_number = key;
    if (h === "product") map.product = key;
    if (h === "volumel") map.volume_liters = key;
    if (h === "baserate") map.base_rate = key;
    if (h === "fet") map.fet = key;
    if (h === "pft") map.pft = key;
    if (h === "rateperltr") map.rate_per_ltr = key;
    if (h === "subtoatl" || h === "subtotal") map.subtotal = key;
    if (h === "gsthst") map.gst = key;
    if (h === "pst") map.pst = key;
    if (h === "qst") map.qst = key;
    if (h === "amount") map.amount = key;
    if (h === "drivername") map.driver_name = key;
    if (h === "invoicenumber") map.invoice_number = key;
    if (h === "invoicedate") map.invoice_date = key;
    if (h.startsWith("periodbilled")) map.period_billed = key;
    if (h === "customernumber") map.customer_number = key;
  }

  if (!map.company_name || !map.amount) {
    throw new Error("Missing required columns: Company Name, Amount $");
  }

  return map;
};

router.post(
  "/admin/billed-transactions/upload",
  authMiddleware,
  upload.single("file"),
  async (req, res) => {
    try {
      await ensureSchema();
    } catch (err) {
      if (req.file?.path) fs.unlink(req.file.path, () => {});
      return res.status(500).json({ message: "Billed schema init failed", error: err.message });
    }

    if (!isAdmin(req)) {
      if (req.file?.path) fs.unlink(req.file.path, () => {});
      return res.status(403).json({ message: "Access denied" });
    }

    if (!req.file) {
      return res.status(400).json({ message: "No file uploaded. Use form-data field name: file" });
    }

    const ext = path.extname(req.file.originalname || "").toLowerCase();
    if (![".csv", ".xlsx", ".xls"].includes(ext)) {
      fs.unlink(req.file.path, () => {});
      return res.status(400).json({ message: "Invalid file type. Use CSV/XLSX" });
    }

    let rows = [];
    try {
      rows = parseSheetRows(req.file.path);
      if (!Array.isArray(rows) || rows.length === 0) {
        throw new Error("No data rows found");
      }
    } catch (err) {
      fs.unlink(req.file.path, () => {});
      return res.status(400).json({ message: `Failed to parse file: ${err.message}` });
    }

    let headerMap;
    try {
      headerMap = buildHeaderMap(rows[0] || {});
    } catch (err) {
      fs.unlink(req.file.path, () => {});
      return res.status(400).json({ message: err.message, headers: Object.keys(rows[0] || {}) });
    }

    const customerResult = await pool.query(
      "SELECT id, company_name, customer_number FROM customers"
    );

    const byCustomerNumber = new Map();
    const byCompanyName = new Map();
    for (const customer of customerResult.rows) {
      if (customer.customer_number) byCustomerNumber.set(String(customer.customer_number).trim().toLowerCase(), customer.id);
      const key = normalizeName(customer.company_name);
      if (key) byCompanyName.set(key, customer.id);
    }

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      const uploadInsert = await client.query(
        `INSERT INTO billed_transaction_uploads (uploaded_by_user_id, original_filename, stored_filename)
         VALUES ($1, $2, $3)
         RETURNING id`,
        [req.user.id || null, req.file.originalname, req.file.filename]
      );
      const uploadId = uploadInsert.rows[0].id;

      let inserted = 0;
      let unmatched = 0;
      let skipped = 0;
      const unmatchedCompanies = new Set();
      const errors = [];

      for (let i = 0; i < rows.length; i += 1) {
        const row = rows[i];
        try {
          const companyName = String(row[headerMap.company_name] || "").trim();
          if (!companyName) {
            skipped += 1;
            continue;
          }

          const amount = parseNumber(row[headerMap.amount]);
          if (!Number.isFinite(amount)) {
            skipped += 1;
            continue;
          }

          const customerNumberRaw = headerMap.customer_number
            ? String(row[headerMap.customer_number] || "").trim()
            : "";

          let customerId = null;
          if (customerNumberRaw) {
            customerId = byCustomerNumber.get(customerNumberRaw.toLowerCase()) || null;
          }
          if (!customerId) {
            customerId = byCompanyName.get(normalizeName(companyName)) || null;
          }
          if (!customerId) {
            unmatched += 1;
            if (unmatchedCompanies.size < 50) unmatchedCompanies.add(companyName);
          }

          await client.query(
            `INSERT INTO billed_transactions (
              upload_id, customer_id, company_name, card_number, transaction_date,
              location, city, province, document_number, product,
              volume_liters, base_rate, fet, pft, rate_per_ltr,
              subtotal, gst, pst, qst, amount,
              driver_name, invoice_number, invoice_date, period_billed, raw_row
            ) VALUES (
              $1,$2,$3,$4,$5,
              $6,$7,$8,$9,$10,
              $11,$12,$13,$14,$15,
              $16,$17,$18,$19,$20,
              $21,$22,$23,$24,$25
            )`,
            [
              uploadId,
              customerId,
              companyName,
              String(row[headerMap.card_number] || "").trim() || null,
              parseDateOnly(row[headerMap.transaction_date]),
              String(row[headerMap.location] || "").trim() || null,
              String(row[headerMap.city] || "").trim() || null,
              String(row[headerMap.province] || "").trim() || null,
              String(row[headerMap.document_number] || "").trim() || null,
              String(row[headerMap.product] || "").trim() || null,
              parseNumber(row[headerMap.volume_liters]),
              parseNumber(row[headerMap.base_rate]),
              parseNumber(row[headerMap.fet]),
              parseNumber(row[headerMap.pft]),
              parseNumber(row[headerMap.rate_per_ltr]),
              parseNumber(row[headerMap.subtotal]),
              parseNumber(row[headerMap.gst]),
              parseNumber(row[headerMap.pst]),
              parseNumber(row[headerMap.qst]),
              amount,
              String(row[headerMap.driver_name] || "").trim() || null,
              String(row[headerMap.invoice_number] || "").trim() || null,
              parseDateOnly(row[headerMap.invoice_date]),
              String(row[headerMap.period_billed] || "").trim() || null,
              JSON.stringify(row),
            ]
          );
          inserted += 1;
        } catch (err) {
          skipped += 1;
          if (errors.length < 50) {
            errors.push({ row: i + 2, error: err.message });
          }
        }
      }

      await client.query("COMMIT");

      return res.json({
        message: "Billed transactions uploaded",
        upload_id: uploadId,
        inserted,
        skipped,
        unmatched,
        unmatched_companies: Array.from(unmatchedCompanies),
        errors,
      });
    } catch (err) {
      await client.query("ROLLBACK");
      fs.unlink(req.file.path, () => {});
      return res.status(500).json({ message: "Upload failed", error: err.message });
    } finally {
      client.release();
    }
  }
);

router.get("/admin/billed-transactions", authMiddleware, async (req, res) => {
  try {
    await ensureSchema();
  } catch (err) {
    return res.status(500).json({ message: "Billed schema init failed", error: err.message });
  }

  if (!isAdmin(req)) return res.status(403).json({ message: "Access denied" });

  try {
    const values = [];
    const where = [];

    if (req.query.customer_id) {
      values.push(parseInt(req.query.customer_id, 10));
      where.push(`bt.customer_id = $${values.length}`);
    }
    if (req.query.invoice_number) {
      values.push(String(req.query.invoice_number).trim());
      where.push(`bt.invoice_number = $${values.length}`);
    }
    if (req.query.q) {
      values.push(`%${String(req.query.q).trim()}%`);
      where.push(`(bt.company_name ILIKE $${values.length} OR bt.card_number ILIKE $${values.length} OR bt.document_number ILIKE $${values.length})`);
    }

    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

    const result = await pool.query(
      `SELECT bt.id, bt.upload_id, bt.customer_id, bt.company_name, bt.card_number,
              bt.transaction_date, bt.location, bt.city, bt.province, bt.document_number,
              bt.product, bt.volume_liters, bt.base_rate, bt.rate_per_ltr,
              bt.subtotal, bt.gst, bt.pst, bt.qst, bt.amount,
              bt.driver_name, bt.invoice_number, bt.invoice_date, bt.period_billed,
              bt.created_at, c.customer_number
       FROM billed_transactions bt
       LEFT JOIN customers c ON c.id = bt.customer_id
       ${whereSql}
       ORDER BY bt.transaction_date DESC NULLS LAST, bt.id DESC
       LIMIT 2000`,
      values
    );

    return res.json(result.rows);
  } catch (err) {
    return res.status(500).json({ message: "Failed to load billed transactions", error: err.message });
  }
});

router.get("/customer/billed-transactions", authMiddleware, async (req, res) => {
  try {
    await ensureSchema();
  } catch (err) {
    return res.status(500).json({ message: "Billed schema init failed", error: err.message });
  }

  if (!req.user?.customer_id || req.user?.role === "admin") {
    return res.status(403).json({ message: "Access denied" });
  }

  try {
    const values = [req.user.customer_id];
    const where = ["bt.customer_id = $1"];

    if (req.query.invoice_number) {
      values.push(String(req.query.invoice_number).trim());
      where.push(`bt.invoice_number = $${values.length}`);
    }
    if (req.query.date_from) {
      values.push(String(req.query.date_from).trim());
      where.push(`bt.transaction_date >= $${values.length}::date`);
    }
    if (req.query.date_to) {
      values.push(String(req.query.date_to).trim());
      where.push(`bt.transaction_date <= $${values.length}::date`);
    }

    const result = await pool.query(
      `SELECT bt.id, bt.company_name, bt.card_number, bt.transaction_date,
              bt.location, bt.city, bt.province, bt.document_number,
              bt.product, bt.volume_liters, bt.rate_per_ltr, bt.amount,
              bt.driver_name, bt.invoice_number, bt.invoice_date, bt.period_billed
       FROM billed_transactions bt
       WHERE ${where.join(" AND ")}
       ORDER BY bt.transaction_date DESC NULLS LAST, bt.id DESC
       LIMIT 5000`,
      values
    );

    const totals = result.rows.reduce(
      (acc, row) => {
        acc.count += 1;
        acc.total_amount += Number(row.amount || 0);
        acc.total_liters += Number(row.volume_liters || 0);
        return acc;
      },
      { count: 0, total_amount: 0, total_liters: 0 }
    );

    return res.json({ data: result.rows, totals });
  } catch (err) {
    return res.status(500).json({ message: "Failed to load billed transactions", error: err.message });
  }
});

router.get("/customer/billed-transactions/export", authMiddleware, async (req, res) => {
  try {
    await ensureSchema();
  } catch (err) {
    return res.status(500).json({ message: "Billed schema init failed", error: err.message });
  }

  if (!req.user?.customer_id || req.user?.role === "admin") {
    return res.status(403).json({ message: "Access denied" });
  }

  try {
    const values = [req.user.customer_id];
    const where = ["bt.customer_id = $1"];

    if (req.query.invoice_number) {
      values.push(String(req.query.invoice_number).trim());
      where.push(`bt.invoice_number = $${values.length}`);
    }
    if (req.query.date_from) {
      values.push(String(req.query.date_from).trim());
      where.push(`bt.transaction_date >= $${values.length}::date`);
    }
    if (req.query.date_to) {
      values.push(String(req.query.date_to).trim());
      where.push(`bt.transaction_date <= $${values.length}::date`);
    }

    const result = await pool.query(
      `SELECT bt.company_name, bt.card_number, bt.transaction_date, bt.location,
              bt.city, bt.province, bt.document_number, bt.product,
              bt.volume_liters, bt.rate_per_ltr, bt.amount,
              bt.driver_name, bt.invoice_number, bt.invoice_date, bt.period_billed
       FROM billed_transactions bt
       WHERE ${where.join(" AND ")}
       ORDER BY bt.transaction_date DESC NULLS LAST, bt.id DESC`,
      values
    );

    const header = [
      "Company Name",
      "Card #",
      "Date",
      "Location",
      "City",
      "Prov",
      "Document #",
      "Product",
      "Volume (L)",
      "Rate Per Ltr",
      "Amount $",
      "Driver Name",
      "Invoice Number",
      "Invoice Date",
      "Period Billed",
    ];

    const escapeCsv = (v) => {
      const s = String(v == null ? "" : v);
      if (s.includes('"') || s.includes(",") || s.includes("\n")) {
        return `"${s.replace(/"/g, '""')}"`;
      }
      return s;
    };

    const lines = [header.join(",")];
    for (const row of result.rows) {
      lines.push(
        [
          row.company_name,
          row.card_number,
          row.transaction_date,
          row.location,
          row.city,
          row.province,
          row.document_number,
          row.product,
          row.volume_liters,
          row.rate_per_ltr,
          row.amount,
          row.driver_name,
          row.invoice_number,
          row.invoice_date,
          row.period_billed,
        ].map(escapeCsv).join(",")
      );
    }

    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", `attachment; filename="billed-transactions-${new Date().toISOString().slice(0, 10)}.csv"`);
    return res.send(`${lines.join("\n")}\n`);
  } catch (err) {
    return res.status(500).json({ message: "Failed to export billed transactions", error: err.message });
  }
});

module.exports = router;
