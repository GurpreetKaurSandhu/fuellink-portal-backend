const express = require("express");
const router = express.Router();
const multer = require("multer");
const pdf = require("pdf-parse");
const fs = require("fs");
const path = require("path");
const XLSX = require("xlsx");
const pool = require("../db");  // ← ADD THIS
const authMiddleware = require("../middleware/authMiddleware");
const { recalculateTransactions } = require("../services/recalculateTransactions");
const bcrypt = require("bcrypt");
const crypto = require("crypto");

const upload = multer({
  dest: path.join(__dirname, "../uploads"),
});

const safeUnlink = (filePath) => {
  if (!filePath) return;
  try {
    fs.unlinkSync(filePath);
  } catch (err) {
    console.error("Failed to delete file:", filePath, err.message);
  }
};

const extractProvince = (location) => {
  if (!location) return null;
  const match = String(location).match(/\b(AB|BC|MB|NB|NL|NS|NT|NU|ON|PE|QC|SK|YT)\b/i);
  return match ? match[1].toUpperCase() : null;
};

const normalizeHeader = (value) =>
  String(value || "")
    .replace(/[\r\n\t]+/g, " ")
    .replace(/\$/g, "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");

const normalizeLocationForMatch = (value) => {
  let text = String(value || "").trim();
  text = text.replace(/^P\s+/i, "");
  return text.trim();
};

const parseNumber = (value) => {
  if (value == null) return null;
  if (typeof value === "number") return value;
  const cleaned = String(value).replace(/[$,]/g, "").trim();
  if (!cleaned) return null;
  const parsed = parseFloat(cleaned);
  return Number.isFinite(parsed) ? parsed : null;
};

const parseDateTime = (value) => {
  if (!value) return null;
  if (value instanceof Date) return value;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
};

const formatDateOnly = (value) => {
  const d = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString().slice(0, 10);
};

const round4 = (value) => {
  if (value == null || !Number.isFinite(value)) return null;
  return Math.round(value * 10000) / 10000;
};

const parseBooleanish = (value, fallback = true) => {
  if (value == null) return fallback;
  if (typeof value === "boolean") return value;
  const normalized = String(value).trim().toLowerCase();
  if (
    normalized === "true" ||
    normalized === "1" ||
    normalized === "yes" ||
    normalized === "active" ||
    normalized === "enabled"
  ) return true;
  if (
    normalized === "false" ||
    normalized === "0" ||
    normalized === "no" ||
    normalized === "inactive" ||
    normalized === "disabled"
  ) return false;
  return fallback;
};

const isAdmin = (user) => user && user.role === "admin";

const requireAdmin = (req, res) => {
  if (!isAdmin(req.user)) {
    res.status(403).json({ message: "Access denied" });
    return false;
  }
  return true;
};

const generateTempPassword = () => crypto.randomBytes(6).toString("hex");

const ensureCustomersSchemaCompat = async () => {
  await pool.query("ALTER TABLE customers ADD COLUMN IF NOT EXISTS owner_name TEXT");
  await pool.query("ALTER TABLE customers ADD COLUMN IF NOT EXISTS city TEXT");
  await pool.query("ALTER TABLE customers ADD COLUMN IF NOT EXISTS email TEXT");
  await pool.query("ALTER TABLE customers ADD COLUMN IF NOT EXISTS contact_email TEXT");
  await pool.query("ALTER TABLE customers ADD COLUMN IF NOT EXISTS phone TEXT");
  await pool.query("ALTER TABLE customers ADD COLUMN IF NOT EXISTS fuellink_card INTEGER");
  await pool.query("ALTER TABLE customers ADD COLUMN IF NOT EXISTS otp_setup TEXT");
  await pool.query("ALTER TABLE customers ADD COLUMN IF NOT EXISTS deposit NUMERIC(12,2)");
  await pool.query("ALTER TABLE customers ADD COLUMN IF NOT EXISTS address TEXT");
  await pool.query("ALTER TABLE customers ADD COLUMN IF NOT EXISTS security_deposit_invoice TEXT");
  await pool.query("ALTER TABLE customers ADD COLUMN IF NOT EXISTS customer_status BOOLEAN DEFAULT true");
  await pool.query("ALTER TABLE customers ADD COLUMN IF NOT EXISTS reference_name TEXT");
  await pool.query("ALTER TABLE customers ADD COLUMN IF NOT EXISTS comment TEXT");
  await pool.query("ALTER TABLE customers ADD COLUMN IF NOT EXISTS rate_group_id INTEGER");
};

const ensureCustomerRateGroupAssignmentsSchemaCompat = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS customer_rate_group_assignments (
      id BIGSERIAL PRIMARY KEY,
      customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
      rate_group_id INTEGER NOT NULL REFERENCES rate_groups(id) ON DELETE RESTRICT,
      start_date DATE NOT NULL,
      end_date DATE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      CHECK (end_date IS NULL OR end_date >= start_date)
    )
  `);
  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS uq_crga_customer_group_range
      ON customer_rate_group_assignments (customer_id, rate_group_id, start_date, end_date)
  `);
};

const ensureCardsHistorySchemaCompat = async (client) => {
  await client.query(`
    DO $$
    BEGIN
      IF to_regclass('public.cards_history') IS NOT NULL THEN
        ALTER TABLE public.cards_history
          ADD COLUMN IF NOT EXISTS status VARCHAR(20),
          ADD COLUMN IF NOT EXISTS driver_name VARCHAR(150),
          ADD COLUMN IF NOT EXISTS changed_by_user_id INTEGER;
      END IF;
    END $$;
  `);
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
  return customerIdNum;
};

const parseSheetRows = (filePath) => {
  const workbook = XLSX.readFile(filePath, { cellDates: true, raw: true });
  const sheetName = workbook.SheetNames[0];
  if (!sheetName) {
    throw new Error("No worksheet found in file");
  }
  const sheet = workbook.Sheets[sheetName];
  return XLSX.utils.sheet_to_json(sheet, { defval: null });
};

const customerColumns = `
  id,
  customer_number,
  company_name,
  owner_name,
  city,
  email,
  contact_email,
  phone,
  address,
  fuellink_card,
  otp_setup,
  deposit,
  security_deposit_invoice,
  customer_status,
  reference_name,
  comment,
  rate_group_id,
  created_at
`;

const parsePdfTransactionLines = (text) => {
  const lines = String(text || "").split(/\r?\n/);
  const parsed = [];

  for (const rawLine of lines) {
    const line = String(rawLine || "").trim();
    if (!/^\d{4,}/.test(line)) continue;

    const tokens = line.split(/\s+/).filter(Boolean);
    if (tokens.length < 7) continue;

    const card_number = tokens[0];
    const dateIdx = tokens.findIndex((token, idx) => idx > 0 && (
      /^\d{4}[/-]\d{2}[/-]\d{2}$/.test(token) ||
      /^\d{2}[/-]\d{2}[/-]\d{4}$/.test(token)
    ));
    if (dateIdx < 0 || dateIdx + 3 >= tokens.length) continue;

    const timeToken = tokens[dateIdx + 1];
    if (!/^\d{2}:\d{2}(:\d{2})?$/.test(timeToken)) continue;

    const driver_name = tokens.slice(1, dateIdx).join(" ").trim() || null;
    const dateToken = tokens[dateIdx].replace(/\//g, "-");
    const purchase_datetime = parseDateTime(`${dateToken} ${timeToken}`);
    if (!purchase_datetime) continue;

    const product = tokens[dateIdx + 2] || null;
    const volume_liters = parseNumber(tokens[dateIdx + 3]);
    if (!Number.isFinite(volume_liters)) continue;

    const amountToken = tokens[dateIdx + 4];
    const amount = parseNumber(amountToken);
    const document_number = tokens[dateIdx + 5] || null;
    const location = tokens.slice(dateIdx + 6).join(" ").trim() || null;

    const numericExtras = tokens
      .map((token) => parseNumber(token))
      .filter((value) => Number.isFinite(value));

    parsed.push({
      card_number,
      driver_name,
      purchase_datetime,
      location,
      city: null,
      province: extractProvince(location),
      document_number,
      product,
      volume_liters,
      amount: Number.isFinite(amount) ? amount : null,
      source_raw_json: {
        amount: Number.isFinite(amount) ? amount : null,
        numeric_tokens: numericExtras,
        raw_line: line,
      },
    });
  }

  return parsed;
};




router.post(
  "/upload-transactions",
  authMiddleware,
  upload.single("file"),
  async (req, res) => {
    try {
      console.log("REQ.FILE OBJECT:", req.file);

      if (!req.file) {
        return res.status(400).json({
          message: "No file received by server"
        });
      }

      if (!isAdmin(req.user)) {
        safeUnlink(req.file?.path);
        return res.status(403).json({
          message: "Access denied"
        });
      }

      const filePath = path.join(__dirname, "../uploads", req.file.filename);

const dataBuffer = fs.readFileSync(filePath);
const data = await pdf(dataBuffer);

console.log("PDF raw text length:", data.text.length);

const lines = data.text.split("\n");
console.log("PDF parsed lines:", lines.length);

const autoCalculate = String(req.body?.auto_calculate || "").toLowerCase() === "true";
let minDate = null;
let maxDate = null;
let insertedCount = 0;
const errors = [];

for (let line of lines) {
  line = line.trim();

  // Only process lines that start with 4 digit card number
  if (/^\d{4}/.test(line)) {

    const parts = line.split(/\s+/);

    try {
      const card_number = parts[0];
      const driver_name = parts[1] + " " + parts[2];
      const purchase_date = parts[3];
      const purchase_time = parts[4];
      const product = parts[5];
      const volume = parseFloat(parts[6]);
      const amount = parseFloat(parts[7]);
      const document_number = parts[8];

      const location = parts.slice(9).join(" ");

      const purchaseDateTime = `${purchase_date} ${purchase_time}`;
      const locationText = location;
      const province = extractProvince(locationText);

      await pool.query(
        `INSERT INTO transactions 
        (customer_id, card_number, driver_name, purchase_datetime, product, volume_liters, total_amount, document_number, location, province)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`,
        [
          1, // TEMP customer_id
          card_number,
          driver_name,
          purchaseDateTime,
          product,
          volume,
          amount,
          document_number,
          locationText,
          province,
        ]
      );

      insertedCount += 1;

      const dt = new Date(purchaseDateTime);
      if (!Number.isNaN(dt.getTime())) {
        if (!minDate || dt < minDate) minDate = dt;
        if (!maxDate || dt > maxDate) maxDate = dt;
      }

    } catch (err) {
      if (errors.length < 50) {
        errors.push({ line, error: err.message });
      }
      console.log("Skipped line:", line);
    }
  }
}

if (insertedCount === 0) {
  safeUnlink(filePath);
  return res.status(400).json({
    message: "No transactions parsed from PDF",
    inserted_count: insertedCount,
    errors,
  });
}

let recalcResult = null;
if (autoCalculate && minDate && maxDate) {
  try {
    recalcResult = await recalculateTransactions({
      date_from: minDate.toISOString(),
      date_to: maxDate.toISOString(),
      customer_id: 1,
    });
  } catch (err) {
    console.error("Recalculate error:", err);
  }
}

res.json({
  message: "Transactions parsed and inserted successfully",
  inserted_count: insertedCount,
  errors,
  auto_calculated: autoCalculate,
  recalculation: recalcResult,
});

    } catch (err) {
      safeUnlink(req.file?.path);
      console.error("FULL ERROR:", err);
      res.status(500).json({
        message: "Server error while processing PDF",
        error: err.message
      });
    }
  }
);

// Example:
// curl -X POST http://localhost:8000/api/admin/transactions/upload-pdfs \
//   -H "Authorization: Bearer <ADMIN_TOKEN>" \
//   -F "files=@/path/one.pdf" -F "files=@/path/two.pdf"
router.post(
  "/transactions/upload-pdfs",
  authMiddleware,
  upload.array("files", 10),
  async (req, res) => {
    const files = Array.isArray(req.files) ? req.files : [];
    if (!isAdmin(req.user)) {
      files.forEach((file) => safeUnlink(file?.path));
      return res.status(403).json({ message: "Access denied" });
    }
    if (files.length === 0) {
      return res.status(400).json({ message: "No files received. Expected multipart field name: files" });
    }

    const summary = [];

    for (const file of files) {
      let uploadId = null;
      const client = await pool.connect();
      try {
        const insertUpload = await client.query(
          `INSERT INTO transaction_uploads
           (uploaded_by_user_id, original_filename, stored_filename, source_type, parse_status)
           VALUES ($1, $2, $3, $4, $5)
           RETURNING id`,
          [req.user.id || null, file.originalname || null, file.filename || null, "pdf", "processing"]
        );
        uploadId = insertUpload.rows[0].id;

        const fileBuffer = fs.readFileSync(file.path);
        const parsedPdf = await pdf(fileBuffer);
        const rows = parsePdfTransactionLines(parsedPdf.text);

        let inserted = 0;
        let skipped = 0;
        let unmatched = 0;

        await client.query("BEGIN");

        for (const row of rows) {
          try {
            const cardCustomer = await client.query(
              "SELECT customer_id FROM cards WHERE card_number = $1 LIMIT 1",
              [row.card_number]
            );
            const customerId = cardCustomer.rows[0]?.customer_id || null;
            if (!customerId) {
              unmatched += 1;
            }

            const amountKey = row.amount;
            const dedupe = await client.query(
              `SELECT id
               FROM transactions
               WHERE customer_id IS NOT DISTINCT FROM $1
                 AND card_number = $2
                 AND document_number IS NOT DISTINCT FROM $3
                 AND purchase_datetime = $4
                 AND volume_liters = $5
                 AND COALESCE(
                   CASE
                     WHEN COALESCE(source_raw_json->>'amount', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                     THEN (source_raw_json->>'amount')::numeric
                     ELSE NULL
                   END,
                   total_amount
                 ) IS NOT DISTINCT FROM $6
               LIMIT 1`,
              [customerId, row.card_number, row.document_number, row.purchase_datetime, row.volume_liters, amountKey]
            );
            if (dedupe.rows.length > 0) {
              skipped += 1;
              continue;
            }

            await client.query(
              `INSERT INTO transactions
               (customer_id, card_number, purchase_datetime, location, city, province, document_number, product,
                volume_liters, total_amount, driver_name, source_upload_id, source_type, source_raw_json)
               VALUES
               ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
              [
                customerId,
                row.card_number,
                row.purchase_datetime,
                row.location,
                row.city,
                row.province,
                row.document_number,
                row.product,
                row.volume_liters,
                row.amount,
                row.driver_name,
                uploadId,
                "pdf",
                row.source_raw_json,
              ]
            );
            inserted += 1;
          } catch (_rowErr) {
            skipped += 1;
          }
        }

        await client.query("COMMIT");

        await client.query(
          `UPDATE transaction_uploads
           SET parse_status = 'done',
               parse_error = NULL,
               rows_inserted = $2,
               rows_skipped = $3,
               rows_unmatched = $4
           WHERE id = $1`,
          [uploadId, inserted, skipped, unmatched]
        );

        summary.push({
          upload_id: uploadId,
          original_filename: file.originalname,
          rows_inserted: inserted,
          rows_skipped: skipped,
          rows_unmatched: unmatched,
          parse_status: "done",
        });
      } catch (err) {
        try {
          await client.query("ROLLBACK");
        } catch (_rollbackErr) {}

        if (uploadId) {
          try {
            await client.query(
              `UPDATE transaction_uploads
               SET parse_status = 'failed',
                   parse_error = $2
               WHERE id = $1`,
              [uploadId, String(err.message || "Unknown parse error")]
            );
          } catch (_updateErr) {}
        }

        summary.push({
          upload_id: uploadId,
          original_filename: file.originalname,
          parse_status: "failed",
          parse_error: String(err.message || "Unknown parse error"),
        });
      } finally {
        client.release();
        safeUnlink(file?.path);
      }
    }

    return res.json({
      message: "PDF upload processing complete",
      files: summary,
    });
  }
);

router.post(
  "/upload-transactions-csv",
  authMiddleware,
  upload.single("file"),
  async (req, res) => {
    try {
      if (!isAdmin(req.user)) {
        safeUnlink(req.file?.path);
        return res.status(403).json({ message: "Access denied" });
      }

      if (!req.file) {
        return res.status(400).json({ message: "No file received. Expected multipart field name: file" });
      }

      const ext = path.extname(req.file.originalname || "").toLowerCase();
      if (ext !== ".csv" && ext !== ".xlsx") {
        safeUnlink(req.file?.path);
        return res.status(400).json({ message: "Invalid file type. Use .csv or .xlsx" });
      }

      const autoCalculate = String(req.body?.auto_calculate || "").toLowerCase() === "true";

      let rows;
      try {
        rows = parseSheetRows(req.file.path);
      } catch (err) {
        safeUnlink(req.file?.path);
        return res.status(400).json({ message: `Failed to parse file: ${err.message}` });
      }

      if (!Array.isArray(rows) || rows.length === 0) {
        safeUnlink(req.file?.path);
        return res.status(400).json({ message: "No data rows found" });
      }

      console.log("upload-transactions-csv first row keys:", Object.keys(rows[0] || {}));

      const headerMap = {};
      for (const key of Object.keys(rows[0] || {})) {
        const normalized = normalizeHeader(key);
        if (normalized === "cardnumber" || normalized === "card") headerMap.card_number = key;
        if (normalized === "purchasedatetime") headerMap.purchase_datetime = key;
        if (normalized === "purchasedate") headerMap.purchase_date = key;
        if (normalized === "purchasetime") headerMap.purchase_time = key;
        if (normalized === "date") headerMap.purchase_date = key;
        if (normalized === "location") headerMap.location = key;
        if (normalized === "city") headerMap.city = key;
        if (normalized === "province" || normalized === "prov") headerMap.province = key;
        if (normalized === "documentnumber" || normalized === "document") headerMap.document_number = key;
        if (normalized === "product") headerMap.product = key;
        if (normalized === "volumeliters" || normalized === "volumel" || normalized === "volume") headerMap.volume_liters = key;
        if (normalized === "totalamount" || normalized === "amount") headerMap.total_amount = key;
        if (normalized === "drivername") headerMap.driver_name = key;
        if (normalized === "drivername1") headerMap.driver_name_1 = key;
        if (normalized === "drivername2") headerMap.driver_name_2 = key;
        if (normalized === "extax") headerMap.ex_tax = key;
        if (normalized === "intax") headerMap.in_tax = key;
        if (normalized === "fetmemo" || normalized === "fet") headerMap.fet = key;
        if (normalized === "pftmemo" || normalized === "pft") headerMap.pft = key;
        if (normalized === "fctpctmemo" || normalized === "fctpct") headerMap.fct_pct = key;
        if (normalized === "urbanmemo" || normalized === "urban") headerMap.urban = key;
        if (normalized === "gsthst") headerMap.gst = key;
        if (normalized === "pst") headerMap.pst = key;
        if (normalized === "qst") headerMap.qst = key;
      }

      const hasDateTime = !!headerMap.purchase_datetime;
      const hasDateAndTime = !!headerMap.purchase_date && !!headerMap.purchase_time;
      const isSuperPass = !!headerMap.ex_tax;

      if (!headerMap.card_number ||
          (!hasDateTime && !hasDateAndTime) ||
          !headerMap.location ||
          !headerMap.product ||
          !headerMap.volume_liters ||
          !headerMap.total_amount ||
          (isSuperPass && !headerMap.ex_tax)) {
        safeUnlink(req.file?.path);
        return res.status(400).json({
          message: "Missing required columns. Required: card_number, purchase date/time, location, product, volume_liters, total_amount",
          first_row_keys: Object.keys(rows[0] || {}),
        });
      }

      let insertedCount = 0;
      let updatedCount = 0;
      let missingRateCount = 0;
      let unmatchedCount = 0;
      const errors = [];
      const unmatchedRows = [];
      const unmatchedCards = new Set();
      let minDate = null;
      let maxDate = null;

      const client = await pool.connect();
      try {
        await client.query("BEGIN");

        const cardCustomerCache = new Map();
        const taxRuleCache = new Map();

        const getCardCustomer = async (cardNumber) => {
          if (cardCustomerCache.has(cardNumber)) {
            return cardCustomerCache.get(cardNumber);
          }

          const result = await client.query(
            `SELECT c.id AS customer_id, c.rate_group_id, COALESCE(rg.markup_per_liter, 0) AS markup_per_liter
             FROM cards cd
             JOIN customers c ON c.id = cd.customer_id
             LEFT JOIN rate_groups rg ON rg.id = c.rate_group_id
             WHERE cd.card_number = $1
             LIMIT 1`,
            [cardNumber]
          );
          const row = result.rows[0] || null;
          cardCustomerCache.set(cardNumber, row);
          return row;
        };

        const getTaxRates = async (province, purchaseDateTime) => {
          const key = `${province}__${new Date(purchaseDateTime).toISOString().slice(0, 10)}`;
          if (taxRuleCache.has(key)) {
            return taxRuleCache.get(key);
          }
          const taxRateResult = await client.query(
            `SELECT gst_rate, pst_rate, qst_rate
             FROM tax_rules
             WHERE province = $1 AND effective_from <= $2
             ORDER BY effective_from DESC
             LIMIT 1`,
            [province, purchaseDateTime]
          );
          const rates = taxRateResult.rows[0] || { gst_rate: 0, pst_rate: 0, qst_rate: 0 };
          taxRuleCache.set(key, rates);
          return rates;
        };

        for (let idx = 0; idx < rows.length; idx += 1) {
          const row = rows[idx];
          if (!row) continue;

          try {
            const card_number = String(row[headerMap.card_number] || "").trim();
            if (!card_number) throw new Error("Missing card_number");

            let purchaseDateTime = null;
            if (hasDateTime) {
              purchaseDateTime = parseDateTime(row[headerMap.purchase_datetime]);
            } else {
              const pd = row[headerMap.purchase_date];
              const pt = row[headerMap.purchase_time];
              const combined = `${pd || ""} ${pt || ""}`.trim();
              purchaseDateTime = parseDateTime(combined);
            }

            if (!purchaseDateTime) throw new Error("Invalid purchase_datetime");

            const rawLocation = String(row[headerMap.location] || "").trim();
            const location = normalizeLocationForMatch(rawLocation);
            if (!location) throw new Error("Missing location");

            const city = headerMap.city ? String(row[headerMap.city] || "").trim() : null;
            const providedProvince = headerMap.province
              ? String(row[headerMap.province] || "").trim()
              : null;
            const province = providedProvince || extractProvince(location);
            if (!province) throw new Error("Missing province");

            const product = String(row[headerMap.product] || "").trim();
            if (!product) throw new Error("Missing product");

            const volume = parseNumber(row[headerMap.volume_liters]);
            if (!Number.isFinite(volume)) throw new Error("Invalid volume_liters");

            const total_amount = parseNumber(row[headerMap.total_amount]);
            if (!Number.isFinite(total_amount)) throw new Error("Invalid total_amount");

            const customerData = await getCardCustomer(card_number);
            if (!customerData?.customer_id) {
              unmatchedCount += 1;
              unmatchedCards.add(card_number);
              if (unmatchedRows.length < 50) {
                unmatchedRows.push({ row: idx + 2, card_number, reason: "card not mapped to customer" });
              }
              continue;
            }

            const customerIdNum = customerData.customer_id;
            const markupPerLiter = parseNumber(customerData.markup_per_liter) || 0;

            const driver_name = headerMap.driver_name
              ? String(row[headerMap.driver_name] || "").trim()
              : null;
            const driver_name_1 = headerMap.driver_name_1
              ? String(row[headerMap.driver_name_1] || "").trim()
              : null;
            const driver_name_2 = headerMap.driver_name_2
              ? String(row[headerMap.driver_name_2] || "").trim()
              : null;
            const combined_driver_name =
              driver_name ||
              [driver_name_1, driver_name_2].filter(Boolean).join(" ") ||
              null;

            const document_number = headerMap.document_number
              ? String(row[headerMap.document_number] || "").trim()
              : null;

            const locationWithCity = city ? `${location} ${city}`.trim() : location;

            let computed_rate_per_liter = null;
            let computed_in_tax = null;
            let computed_ex_tax = null;
            let source_in_tax = null;
            let source_fet = null;
            let source_pft = null;
            let source_fct_pct = null;
            let source_urban = null;
            let source_gst = null;
            let source_pst = null;
            let source_qst = null;
            let source_ex_tax = null;

            if (isSuperPass) {
              const ex_tax = parseNumber(row[headerMap.ex_tax]);
              if (!Number.isFinite(ex_tax)) throw new Error("Invalid ex_tax");
              source_ex_tax = ex_tax;
              source_in_tax = headerMap.in_tax ? parseNumber(row[headerMap.in_tax]) : null;
              source_fet = headerMap.fet ? parseNumber(row[headerMap.fet]) : null;
              source_pft = headerMap.pft ? parseNumber(row[headerMap.pft]) : null;
              source_fct_pct = headerMap.fct_pct ? parseNumber(row[headerMap.fct_pct]) : null;
              source_urban = headerMap.urban ? parseNumber(row[headerMap.urban]) : null;
              source_gst = headerMap.gst ? parseNumber(row[headerMap.gst]) : null;
              source_pst = headerMap.pst ? parseNumber(row[headerMap.pst]) : null;
              source_qst = headerMap.qst ? parseNumber(row[headerMap.qst]) : null;

              computed_ex_tax = round4(ex_tax + markupPerLiter);
              const computed_in_tax_base =
                computed_ex_tax +
                (source_fet || 0) +
                (source_pft || 0) +
                (source_fct_pct || 0) +
                (source_urban || 0);
              computed_in_tax = round4(computed_in_tax_base);
              computed_rate_per_liter = computed_in_tax;
            }

            const rateResult = await client.query(
              "SELECT price FROM rate_match($1,$2,$3,$4)",
              [customerIdNum, province, location, purchaseDateTime]
            );
            if (rateResult.rows.length > 0 && rateResult.rows[0].price != null) {
              computed_rate_per_liter = round4(parseFloat(rateResult.rows[0].price) + markupPerLiter);
              computed_in_tax = computed_rate_per_liter;
              updatedCount += 1;
            } else {
              missingRateCount += 1;
            }

            if (!computed_rate_per_liter) {
              computed_rate_per_liter = round4(total_amount / volume);
              computed_in_tax = computed_rate_per_liter;
            }

            const taxRates = await getTaxRates(province, purchaseDateTime);
            const gst_rate = parseNumber(taxRates.gst_rate) || 0;
            const pst_rate = parseNumber(taxRates.pst_rate) || 0;
            const qst_rate = parseNumber(taxRates.qst_rate) || 0;

            const subtotal = round4((computed_in_tax || 0) * volume);
            const gst = round4(subtotal * gst_rate);
            const pst = round4(subtotal * pst_rate);
            const qst = round4(subtotal * qst_rate);
            const total = round4(subtotal + (gst || 0) + (pst || 0) + (qst || 0));

            await client.query(
              `INSERT INTO transactions
              (customer_id, card_number, driver_name, purchase_datetime, product, volume_liters, total_amount, document_number, location, province,
               computed_rate_per_liter, subtotal, gst, pst, qst, total, computed_ex_tax, computed_in_tax,
               source_ex_tax, source_in_tax, source_fet, source_pft, source_fct_pct, source_urban, source_gst, source_pst, source_qst, source_amount)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
                      $11,$12,$13,$14,$15,$16,$17,$18,
                      $19,$20,$21,$22,$23,$24,$25,$26,$27,$28)`,
              [
                customerIdNum,
                card_number,
                combined_driver_name,
                purchaseDateTime,
                product,
                volume,
                total_amount,
                document_number,
                locationWithCity,
                province,
                computed_rate_per_liter,
                subtotal,
                gst,
                pst,
                qst,
                total,
                computed_ex_tax,
                computed_in_tax,
                source_ex_tax,
                source_in_tax,
                source_fet,
                source_pft,
                source_fct_pct,
                source_urban,
                source_gst,
                source_pst,
                source_qst,
                total_amount,
              ]
            );

            insertedCount += 1;
            if (!minDate || purchaseDateTime < minDate) minDate = purchaseDateTime;
            if (!maxDate || purchaseDateTime > maxDate) maxDate = purchaseDateTime;
          } catch (err) {
            if (errors.length < 50) {
              errors.push({ row: idx + 2, error: err.message });
            }
          }
        }

        await client.query("COMMIT");
      } catch (err) {
        await client.query("ROLLBACK");
        throw err;
      } finally {
        client.release();
      }

      if (insertedCount === 0) {
        safeUnlink(req.file?.path);
      }

      let recalcResult = null;
      if (autoCalculate && minDate && maxDate && insertedCount > 0) {
        try {
          recalcResult = await recalculateTransactions({
            date_from: minDate.toISOString(),
            date_to: maxDate.toISOString(),
            customer_id: null,
          });
        } catch (err) {
          console.error("Recalculate error:", err);
        }
      }

      return res.json({
        inserted_count: insertedCount,
        updated_count: updatedCount,
        missing_rate_count: missingRateCount,
        unmatched_count: unmatchedCount,
        unmatched_cards: Array.from(unmatchedCards),
        unmatched_rows: unmatchedRows,
        errors,
        auto_calculated: autoCalculate,
        recalculation: recalcResult,
      });
    } catch (err) {
      safeUnlink(req.file?.path);
      console.error(err);
      res.status(500).json({ message: "Server error" });
    }
  }
);

const handleCardsImport = async (req, res) => {
  try {
    if (!requireAdmin(req, res)) {
      safeUnlink(req.file?.path);
      return;
    }

    if (!req.file) {
      return res.status(400).json({ message: "No file received. Expected multipart field name: file" });
    }

    const ext = path.extname(req.file.originalname || "").toLowerCase();
    if (ext !== ".csv" && ext !== ".xlsx") {
      return res.status(400).json({ message: "Invalid file type. Use .csv or .xlsx" });
    }

    let rows = [];
    try {
      rows = parseSheetRows(req.file.path);
    } catch (err) {
      return res.status(400).json({ message: `Failed to parse file: ${err.message}` });
    }

    if (!Array.isArray(rows) || rows.length === 0) {
      return res.status(400).json({ message: "No data rows found" });
    }

    const headerMap = {};
    Object.keys(rows[0] || {}).forEach((key) => {
      const normalized = normalizeHeader(key);
      if (normalized === "cardnumber" || normalized === "card") headerMap.card_number = key;
      if (normalized === "customernumber" || normalized === "customer") headerMap.customer_number = key;
      if (normalized === "companyname") headerMap.company_name = key;
      if (normalized === "changedate") headerMap.change_date = key;
      if ((normalized === "drivername1" || normalized === "drivername") && !headerMap.driver_name) {
        headerMap.driver_name = key;
      }
      if (normalized === "status") headerMap.status = key;
      if (normalized === "pin" || normalized === "cardpin") headerMap.pin = key;
    });

    if (!headerMap.card_number) {
      return res.status(400).json({ message: "Missing required column: card_number" });
    }

    let defaultCustomerId = null;
    if (req.body?.customer_number || req.body?.customer_id) {
      try {
        defaultCustomerId = await resolveCustomerId(req.body.customer_id, req.body.customer_number);
      } catch (err) {
        return res.status(400).json({ message: `Invalid default customer: ${err.message}` });
      }
    }

    const statusNorm = (raw) => {
      const value = String(raw || "").trim().toLowerCase();
      if (value === "blocked" || value === "inactive" || value === "disabled") return "blocked";
      return "active";
    };

    let insertedCount = 0;
    let updatedCount = 0;
    const errors = [];
    const customerByNumber = new Map();
    const customerByCompany = new Map();

    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await ensureCardsHistorySchemaCompat(client);

      const cardsHistoryExistsResult = await client.query(
        `SELECT to_regclass('public.cards_history') IS NOT NULL AS exists`
      );
      const hasCardsHistory = Boolean(cardsHistoryExistsResult.rows[0]?.exists);

      const columnsResult = await client.query(
        `SELECT column_name
         FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name = 'cards'`
      );
      const columns = new Set(columnsResult.rows.map((row) => row.column_name));
      const canWriteChangeDate = columns.has("change_date") && Boolean(headerMap.change_date);

      for (let idx = 0; idx < rows.length; idx += 1) {
        const row = rows[idx] || {};
        try {
          const cardNumber = String(row[headerMap.card_number] || "").trim();
          if (!cardNumber) throw new Error("Missing card_number");

          const rowCustomerNumber = headerMap.customer_number
            ? String(row[headerMap.customer_number] || "").trim()
            : "";
          const rowCompanyName = headerMap.company_name
            ? String(row[headerMap.company_name] || "").trim()
            : "";
          const driverName = headerMap.driver_name
            ? String(row[headerMap.driver_name] || "").trim() || null
            : null;
          const status = statusNorm(headerMap.status ? row[headerMap.status] : null);
          const changeDate = canWriteChangeDate ? parseDateTime(row[headerMap.change_date]) : null;

          let customerId = defaultCustomerId;
          if (rowCustomerNumber) {
            if (!customerByNumber.has(rowCustomerNumber)) {
              const byNumber = await client.query(
                "SELECT id FROM customers WHERE customer_number = $1 LIMIT 1",
                [rowCustomerNumber]
              );
              customerByNumber.set(rowCustomerNumber, byNumber.rows[0]?.id || null);
            }
            customerId = customerByNumber.get(rowCustomerNumber);
          } else if (rowCompanyName) {
            const companyKey = rowCompanyName.toLowerCase();
            if (!customerByCompany.has(companyKey)) {
              const byCompany = await client.query(
                "SELECT id FROM customers WHERE lower(company_name) = lower($1) ORDER BY id ASC LIMIT 1",
                [rowCompanyName]
              );
              customerByCompany.set(companyKey, byCompany.rows[0]?.id || null);
            }
            customerId = customerByCompany.get(companyKey);
          }

          // Keep card row even if customer cannot be resolved yet.
          // This preserves latest uploaded card master and allows later mapping.
          if (!customerId) {
            customerId = null;
          }

          const upsert = canWriteChangeDate
            ? await client.query(
                `INSERT INTO cards (customer_id, card_number, driver_name, status, change_date, company_name, customer_number)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (card_number) DO UPDATE SET
                   customer_id = EXCLUDED.customer_id,
                   driver_name = COALESCE(EXCLUDED.driver_name, cards.driver_name),
                   status = EXCLUDED.status,
                   change_date = COALESCE(EXCLUDED.change_date, cards.change_date),
                   company_name = COALESCE(EXCLUDED.company_name, cards.company_name),
                   customer_number = COALESCE(EXCLUDED.customer_number, cards.customer_number)
                 RETURNING id, customer_id, card_number, driver_name, status, company_name, customer_number, (xmax = 0) AS inserted`,
                [customerId, cardNumber, driverName, status, changeDate, rowCompanyName || null, rowCustomerNumber || null]
              )
            : await client.query(
                `INSERT INTO cards (customer_id, card_number, driver_name, status, company_name, customer_number)
                 VALUES ($1, $2, $3, $4, $5, $6)
                 ON CONFLICT (card_number) DO UPDATE SET
                   customer_id = EXCLUDED.customer_id,
                   driver_name = COALESCE(EXCLUDED.driver_name, cards.driver_name),
                   status = EXCLUDED.status,
                   company_name = COALESCE(EXCLUDED.company_name, cards.company_name),
                   customer_number = COALESCE(EXCLUDED.customer_number, cards.customer_number)
                 RETURNING id, customer_id, card_number, driver_name, status, company_name, customer_number, (xmax = 0) AS inserted`,
                [customerId, cardNumber, driverName, status, rowCompanyName || null, rowCustomerNumber || null]
              );

          const card = upsert.rows[0];
          if (hasCardsHistory) {
            await client.query(
              `INSERT INTO cards_history
               (card_id, customer_id, card_number, driver_name, status, changed_by_user_id)
               VALUES ($1, $2, $3, $4, $5, $6)`,
              [card.id, card.customer_id, card.card_number, card.driver_name, card.status, req.user.id || null]
            );
          }

          if (card.inserted) insertedCount += 1;
          else updatedCount += 1;
        } catch (err) {
          if (errors.length < 50) {
            errors.push({ row: idx + 2, error: err.message });
          }
        }
      }

      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }

    if (insertedCount + updatedCount === 0) {
      return res.status(400).json({
        message: errors[0]?.error || "No cards were imported from the file",
        inserted_count: insertedCount,
        updated_count: updatedCount,
        errors,
        note: "PIN fields are ignored and not stored",
      });
    }

    return res.json({
      inserted_count: insertedCount,
      updated_count: updatedCount,
      errors,
      note: "PIN fields are ignored and not stored",
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ message: err?.message || "Server error" });
  } finally {
    safeUnlink(req.file?.path);
  }
};

router.post("/cards/import", authMiddleware, upload.single("file"), handleCardsImport);
router.post("/cards/upload", authMiddleware, upload.single("file"), handleCardsImport);

router.post("/pricing/import", authMiddleware, upload.single("file"), async (req, res) => {
  try {
    if (!requireAdmin(req, res)) {
      safeUnlink(req.file?.path);
      return;
    }

    if (!req.file) {
      return res.status(400).json({ message: "No file received. Expected multipart field name: file" });
    }

    const ext = path.extname(req.file.originalname || "").toLowerCase();
    if (ext !== ".csv" && ext !== ".xlsx") {
      return res.status(400).json({ message: "Invalid file type. Use .csv or .xlsx" });
    }

    let rows = [];
    try {
      rows = parseSheetRows(req.file.path);
    } catch (err) {
      return res.status(400).json({ message: `Failed to parse file: ${err.message}` });
    }

    if (!Array.isArray(rows) || rows.length === 0) {
      return res.status(400).json({ message: "No data rows found" });
    }

    const headerMap = {};
    Object.keys(rows[0] || {}).forEach((key) => {
      const normalized = normalizeHeader(key);
      if (normalized === "cardnumber" || normalized === "card") headerMap.card_number = key;
      if (normalized === "purchasedatetime") headerMap.purchase_datetime = key;
      if (normalized === "purchasedate") headerMap.purchase_date = key;
      if (normalized === "purchasetime") headerMap.purchase_time = key;
      if (normalized === "location" || normalized === "sitename") headerMap.site_name = key;
      if (normalized === "documentnumber" || normalized === "document") headerMap.document_number = key;
      if (normalized === "volumeliters" || normalized === "volume") headerMap.volume_liters = key;
      if (normalized === "totalamount" || normalized === "amount") headerMap.total_amount = key;
      if (normalized === "extax") headerMap.ex_tax = key;
      if (normalized === "intax") headerMap.in_tax = key;
      if (normalized === "fetmemo" || normalized === "fet") headerMap.fet = key;
      if (normalized === "pftmemo" || normalized === "pft") headerMap.pft = key;
      if (normalized === "fctpctmemo" || normalized === "fctpct") headerMap.fct_pct = key;
      if (normalized === "urbanmemo" || normalized === "urban") headerMap.urban = key;
      if (normalized === "gsthst" || normalized === "gst") headerMap.gst = key;
      if (normalized === "pst") headerMap.pst = key;
      if (normalized === "qst") headerMap.qst = key;
    });

    const hasDateTime = !!headerMap.purchase_datetime;
    const hasDateAndTime = !!headerMap.purchase_date && !!headerMap.purchase_time;
    if (!headerMap.card_number || !headerMap.site_name || !headerMap.volume_liters || (!hasDateTime && !hasDateAndTime)) {
      return res.status(400).json({
        message: "Missing required columns. Required: card_number, purchase date/time, site/location, volume",
      });
    }

    const client = await pool.connect();
    let insertedCount = 0;
    let linkedCount = 0;
    const errors = [];
    const unmatchedCards = new Set();
    const unmatchedRows = [];
    const cardCustomerCache = new Map();

    try {
      await client.query("BEGIN");

      const getCardCustomer = async (cardNumber) => {
        if (cardCustomerCache.has(cardNumber)) return cardCustomerCache.get(cardNumber);
        const result = await client.query(
          `SELECT c.id AS customer_id, COALESCE(rg.markup_per_liter, 0) AS markup_per_liter
           FROM cards cd
           JOIN customers c ON c.id = cd.customer_id
           LEFT JOIN rate_groups rg ON rg.id = c.rate_group_id
           WHERE cd.card_number = $1
           LIMIT 1`,
          [cardNumber]
        );
        const row = result.rows[0] || null;
        cardCustomerCache.set(cardNumber, row);
        return row;
      };

      for (let idx = 0; idx < rows.length; idx += 1) {
        const row = rows[idx] || {};
        try {
          const cardNumber = String(row[headerMap.card_number] || "").trim();
          if (!cardNumber) throw new Error("Missing card_number");

          const cardCustomer = await getCardCustomer(cardNumber);
          if (!cardCustomer?.customer_id) {
            unmatchedCards.add(cardNumber);
            if (unmatchedRows.length < 50) unmatchedRows.push({ row: idx + 2, card_number: cardNumber });
            continue;
          }

          const customerId = cardCustomer.customer_id;
          const markupPerLiter = parseNumber(cardCustomer.markup_per_liter) || 0;

          let purchaseDateTime = null;
          if (hasDateTime) {
            purchaseDateTime = parseDateTime(row[headerMap.purchase_datetime]);
          } else {
            purchaseDateTime = parseDateTime(`${row[headerMap.purchase_date] || ""} ${row[headerMap.purchase_time] || ""}`.trim());
          }
          if (!purchaseDateTime) throw new Error("Invalid purchase_datetime");

          const siteName = String(row[headerMap.site_name] || "").trim();
          if (!siteName) throw new Error("Missing site/location");

          const documentNumber = headerMap.document_number ? String(row[headerMap.document_number] || "").trim() : null;
          const volumeLiters = parseNumber(row[headerMap.volume_liters]);
          if (!Number.isFinite(volumeLiters)) throw new Error("Invalid volume_liters");

          const sourceExTax = headerMap.ex_tax ? parseNumber(row[headerMap.ex_tax]) : null;
          const sourceInTax = headerMap.in_tax ? parseNumber(row[headerMap.in_tax]) : null;
          const sourceFet = headerMap.fet ? parseNumber(row[headerMap.fet]) : null;
          const sourcePft = headerMap.pft ? parseNumber(row[headerMap.pft]) : null;
          const sourceFctPct = headerMap.fct_pct ? parseNumber(row[headerMap.fct_pct]) : null;
          const sourceUrban = headerMap.urban ? parseNumber(row[headerMap.urban]) : null;
          const sourceGst = headerMap.gst ? parseNumber(row[headerMap.gst]) : null;
          const sourcePst = headerMap.pst ? parseNumber(row[headerMap.pst]) : null;
          const sourceQst = headerMap.qst ? parseNumber(row[headerMap.qst]) : null;
          const sourceAmount = headerMap.total_amount ? parseNumber(row[headerMap.total_amount]) : null;

          let computedExTax = null;
          let computedInTax = null;
          if (sourceExTax != null) {
            computedExTax = round4(sourceExTax + markupPerLiter);
            computedInTax = round4(
              (computedExTax || 0) +
              (sourceFet || 0) +
              (sourcePft || 0) +
              (sourceFctPct || 0) +
              (sourceUrban || 0)
            );
          } else if (sourceInTax != null) {
            computedInTax = round4(sourceInTax + markupPerLiter);
          }

          let matchedTransactionId = null;
          if (documentNumber) {
            const byDoc = await client.query(
              `SELECT id
               FROM transactions
               WHERE customer_id = $1
                 AND card_number = $2
                 AND document_number = $3
               ORDER BY ABS(EXTRACT(EPOCH FROM (purchase_datetime - $4::timestamp))) ASC
               LIMIT 1`,
              [customerId, cardNumber, documentNumber, purchaseDateTime]
            );
            matchedTransactionId = byDoc.rows[0]?.id || null;
          }

          if (!matchedTransactionId) {
            const byComposite = await client.query(
              `SELECT id
               FROM transactions
               WHERE customer_id = $1
                 AND card_number = $2
                 AND DATE(purchase_datetime) = DATE($3::timestamp)
                 AND ABS(COALESCE(volume_liters, 0) - $4) <= 0.02
                 AND lower(COALESCE(location, '')) LIKE lower($5)
               ORDER BY ABS(EXTRACT(EPOCH FROM (purchase_datetime - $3::timestamp))) ASC
               LIMIT 1`,
              [customerId, cardNumber, purchaseDateTime, volumeLiters, `%${siteName}%`]
            );
            matchedTransactionId = byComposite.rows[0]?.id || null;
          }

          const inserted = await client.query(
            `INSERT INTO pricing_import_lines
             (customer_id, card_number, document_number, purchase_datetime, site_name, volume_liters,
              source_amount, source_ex_tax, source_in_tax, source_fet, source_pft, source_fct_pct,
              source_urban, source_gst, source_pst, source_qst, markup_per_liter, computed_ex_tax,
              computed_in_tax, transaction_id)
             VALUES
             ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20)
             RETURNING id`,
            [
              customerId,
              cardNumber,
              documentNumber || null,
              purchaseDateTime,
              siteName,
              volumeLiters,
              sourceAmount,
              sourceExTax,
              sourceInTax,
              sourceFet,
              sourcePft,
              sourceFctPct,
              sourceUrban,
              sourceGst,
              sourcePst,
              sourceQst,
              markupPerLiter,
              computedExTax,
              computedInTax,
              matchedTransactionId,
            ]
          );
          insertedCount += inserted.rows.length;

          if (matchedTransactionId) {
            linkedCount += 1;
            await client.query(
              `UPDATE transactions
               SET source_amount = COALESCE($2, source_amount),
                   source_ex_tax = COALESCE($3, source_ex_tax),
                   source_in_tax = COALESCE($4, source_in_tax),
                   source_fet = COALESCE($5, source_fet),
                   source_pft = COALESCE($6, source_pft),
                   source_fct_pct = COALESCE($7, source_fct_pct),
                   source_urban = COALESCE($8, source_urban),
                   source_gst = COALESCE($9, source_gst),
                   source_pst = COALESCE($10, source_pst),
                   source_qst = COALESCE($11, source_qst),
                   computed_ex_tax = COALESCE($12, computed_ex_tax),
                   computed_in_tax = COALESCE($13, computed_in_tax)
               WHERE id = $1`,
              [
                matchedTransactionId,
                sourceAmount,
                sourceExTax,
                sourceInTax,
                sourceFet,
                sourcePft,
                sourceFctPct,
                sourceUrban,
                sourceGst,
                sourcePst,
                sourceQst,
                computedExTax,
                computedInTax,
              ]
            );
          }
        } catch (err) {
          if (errors.length < 50) {
            errors.push({ row: idx + 2, error: err.message });
          }
        }
      }

      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }

    return res.json({
      inserted_count: insertedCount,
      linked_count: linkedCount,
      unmatched_count: unmatchedCards.size,
      unmatched_cards: Array.from(unmatchedCards),
      unmatched_rows: unmatchedRows,
      errors,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ message: "Server error" });
  } finally {
    safeUnlink(req.file?.path);
  }
});

router.get("/rate-groups", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    await ensureCustomersSchemaCompat();
    await ensureCustomerRateGroupAssignmentsSchemaCompat();

    const [groupsResult, customersResult] = await Promise.all([
      pool.query(
        `SELECT
           id,
           name,
           markup_per_liter,
           markup_per_liter AS markup_per_litre,
           created_at
         FROM rate_groups
         ORDER BY name ASC`
      ),
      pool.query(
        `SELECT
           c.id,
           c.customer_number,
           c.company_name,
           c.rate_group_id,
           COALESCE(active_crga.rate_group_id, c.rate_group_id) AS effective_rate_group_id,
           rg_active.name AS effective_rate_group_name,
           active_crga.start_date AS assigned_from,
           active_crga.end_date AS assigned_to,
           CASE
             WHEN COALESCE(active_crga.rate_group_id, c.rate_group_id) IS NULL THEN 'UNASSIGNED'
             ELSE 'ASSIGNED'
           END AS assignment_status
         FROM customers c
         LEFT JOIN LATERAL (
           SELECT crga.rate_group_id, crga.start_date, crga.end_date
           FROM customer_rate_group_assignments crga
           WHERE crga.customer_id = c.id
             AND crga.start_date <= CURRENT_DATE
             AND (crga.end_date IS NULL OR crga.end_date >= CURRENT_DATE)
           ORDER BY crga.start_date DESC, crga.id DESC
           LIMIT 1
         ) active_crga ON true
         LEFT JOIN rate_groups rg_active
           ON rg_active.id = COALESCE(active_crga.rate_group_id, c.rate_group_id)
         ORDER BY c.company_name ASC NULLS LAST, c.id ASC`
      ),
    ]);

    res.json({
      groups: groupsResult.rows,
      customers: customersResult.rows,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Server error" });
  }
});

router.post("/rate-groups", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;

    const { name, markup_per_liter } = req.body || {};
    const cleanName = String(name || "").trim();
    if (!cleanName) {
      return res.status(400).json({ message: "name is required" });
    }

    const markup = parseNumber(markup_per_liter);
    if (markup == null) {
      return res.status(400).json({ message: "markup_per_liter is required" });
    }

    const result = await pool.query(
      `INSERT INTO rate_groups (name, markup_per_liter)
       VALUES ($1, $2)
       ON CONFLICT (name) DO UPDATE SET markup_per_liter = EXCLUDED.markup_per_liter
       RETURNING id, name, markup_per_liter, created_at`,
      [cleanName, markup]
    );

    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Server error" });
  }
});

router.post("/rate-groups/assign", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    await ensureCustomersSchemaCompat();
    await ensureCustomerRateGroupAssignmentsSchemaCompat();

    const rateGroupId = parseInt(req.body?.rate_group_id, 10);
    const startDateRaw = String(req.body?.start_date || "").trim();
    const endDateRaw = String(req.body?.end_date || "").trim();
    const startDate = startDateRaw || new Date().toISOString().slice(0, 10);
    const endDate = endDateRaw || null;
    const customerIdsRaw = Array.isArray(req.body?.customer_ids) ? req.body.customer_ids : [];
    const customerIds = Array.from(
      new Set(
        customerIdsRaw
          .map((value) => parseInt(value, 10))
          .filter((value) => Number.isInteger(value) && value > 0)
      )
    );

    if (!Number.isInteger(rateGroupId) || rateGroupId < 1) {
      return res.status(400).json({ message: "Invalid rate_group_id" });
    }

    if (customerIds.length === 0) {
      return res.status(400).json({ message: "customer_ids must include at least one customer" });
    }
    if (!/^\d{4}-\d{2}-\d{2}$/.test(startDate)) {
      return res.status(400).json({ message: "Invalid start_date (use YYYY-MM-DD)" });
    }
    if (endDate && !/^\d{4}-\d{2}-\d{2}$/.test(endDate)) {
      return res.status(400).json({ message: "Invalid end_date (use YYYY-MM-DD)" });
    }
    if (endDate && endDate < startDate) {
      return res.status(400).json({ message: "end_date must be >= start_date" });
    }

    const groupResult = await pool.query("SELECT id FROM rate_groups WHERE id = $1", [rateGroupId]);
    if (groupResult.rows.length === 0) {
      return res.status(400).json({ message: "rate_group_id does not exist" });
    }

    const client = await pool.connect();
    let updatedCount = 0;
    try {
      await client.query("BEGIN");

      const updateResult = await client.query(
        "UPDATE customers SET rate_group_id = $1 WHERE id = ANY($2::int[])",
        [rateGroupId, customerIds]
      );
      updatedCount = updateResult.rowCount || 0;

      await client.query(
        `UPDATE customer_rate_group_assignments
         SET end_date = GREATEST(start_date, ($2::date - INTERVAL '1 day')::date)
         WHERE customer_id = ANY($1::int[])
           AND end_date IS NULL`,
        [customerIds, startDate]
      );

      await client.query(
        `INSERT INTO customer_rate_group_assignments (customer_id, rate_group_id, start_date, end_date)
         SELECT id, $1, $3::date, $4::date
         FROM unnest($2::int[]) AS id
         ON CONFLICT DO NOTHING`,
        [rateGroupId, customerIds, startDate, endDate]
      );

      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }

    return res.json({
      message: "Assignments updated",
      updated_count: updatedCount,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ message: "Server error" });
  }
});

router.patch("/customers/:id/rate-group", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;

    const customerId = parseInt(req.params.id, 10);
    if (!Number.isInteger(customerId) || customerId < 1) {
      return res.status(400).json({ message: "Invalid customer id" });
    }

    const { rate_group_id } = req.body || {};
    const rateGroupId = parseInt(rate_group_id, 10);
    if (!Number.isInteger(rateGroupId) || rateGroupId < 1) {
      return res.status(400).json({ message: "Invalid rate_group_id" });
    }

    const groupResult = await pool.query(
      "SELECT id FROM rate_groups WHERE id = $1",
      [rateGroupId]
    );
    if (groupResult.rows.length === 0) {
      return res.status(400).json({ message: "rate_group_id does not exist" });
    }

    const result = await pool.query(
      "UPDATE customers SET rate_group_id = $1 WHERE id = $2 RETURNING id, rate_group_id",
      [rateGroupId, customerId]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "Customer not found" });
    }

    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Server error" });
  }
});

router.get("/rate-groups/assignments", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    await ensureCustomerRateGroupAssignmentsSchemaCompat();

    const values = [];
    const where = [];
    if (req.query.customer_id) {
      const customerId = parseInt(req.query.customer_id, 10);
      if (!Number.isInteger(customerId) || customerId < 1) {
        return res.status(400).json({ message: "Invalid customer_id" });
      }
      values.push(customerId);
      where.push(`crga.customer_id = $${values.length}`);
    }
    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

    const result = await pool.query(
      `SELECT
         crga.id,
         crga.customer_id,
         c.customer_number,
         c.company_name,
         crga.rate_group_id,
         rg.name AS rate_group_name,
         crga.start_date,
         crga.end_date,
         crga.created_at
       FROM customer_rate_group_assignments crga
       JOIN customers c ON c.id = crga.customer_id
       JOIN rate_groups rg ON rg.id = crga.rate_group_id
       ${whereSql}
       ORDER BY crga.created_at DESC, crga.id DESC
       LIMIT 5000`,
      values
    );

    return res.json(result.rows);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ message: "Server error" });
  }
});

router.get("/customers/lookup", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    await ensureCustomersSchemaCompat();

    const customerNumber = String(req.query.customer_number || "").trim();
    if (!customerNumber) {
      return res.status(400).json({ message: "customer_number is required" });
    }

    const result = await pool.query(
      `SELECT ${customerColumns}
       FROM customers
       WHERE customer_number = $1`,
      [customerNumber]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "Customer not found" });
    }

    return res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ message: "Server error" });
  }
});

router.get("/customers", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    await ensureCustomersSchemaCompat();

    const q = String(req.query.q || "").trim();
    const values = [];
    let where = "";

    if (q) {
      values.push(`%${q}%`);
      where = `
        WHERE
          customer_number ILIKE $1 OR
          company_name ILIKE $1 OR
          owner_name ILIKE $1 OR
          city ILIKE $1 OR
          email ILIKE $1
      `;
    }

    const result = await pool.query(
      `SELECT ${customerColumns}
       FROM customers
       ${where}
       ORDER BY company_name ASC NULLS LAST, id ASC`,
      values
    );

    return res.json(result.rows);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ message: "Server error" });
  }
});

router.post("/customers/import", authMiddleware, upload.single("file"), async (req, res) => {
  try {
    if (!requireAdmin(req, res)) {
      safeUnlink(req.file?.path);
      return;
    }
    await ensureCustomersSchemaCompat();

    if (!req.file) {
      return res.status(400).json({ message: "No file received" });
    }

    const ext = path.extname(req.file.originalname || "").toLowerCase();
    if (ext !== ".csv" && ext !== ".xlsx") {
      return res.status(400).json({ message: "Invalid file type. Use .csv or .xlsx" });
    }

    let rows = [];
    try {
      const workbook = XLSX.readFile(req.file.path, { cellDates: true, raw: true });
      const sheetName = workbook.SheetNames[0];
      if (!sheetName) {
        return res.status(400).json({ message: "No worksheet found in file" });
      }
      const sheet = workbook.Sheets[sheetName];
      rows = XLSX.utils.sheet_to_json(sheet, { defval: null });
    } catch (_err) {
      return res.status(400).json({ message: "Failed to parse file" });
    }

    if (!Array.isArray(rows) || rows.length === 0) {
      return res.status(400).json({ message: "No rows found in file" });
    }

    const headerMap = {};
    Object.keys(rows[0] || {}).forEach((key) => {
      const normalized = normalizeHeader(key);
      if (normalized === "customernumber") headerMap.customer_number = key;
      if (normalized === "companyname") headerMap.company_name = key;
      if (normalized === "ownername") headerMap.owner_name = key;
      if (normalized === "city") headerMap.city = key;
      if (normalized === "email") headerMap.email = key;
      if (normalized === "contactemail") headerMap.contact_email = key;
      if (normalized === "phone" || normalized === "phonenumber" || normalized === "mobile") headerMap.phone = key;
      if (normalized === "fuellinkcard") headerMap.fuellink_card = key;
      if (normalized === "otpsetup") headerMap.otp_setup = key;
      if (normalized === "deposit") headerMap.deposit = key;
      if (normalized === "address") headerMap.address = key;
      if (normalized === "securitydepositinvoice") headerMap.security_deposit_invoice = key;
      if (normalized === "customerstatus") headerMap.customer_status = key;
      if (normalized === "referencename") headerMap.reference_name = key;
      if (normalized === "comment") headerMap.comment = key;
    });

    if (!headerMap.customer_number) {
      return res.status(400).json({ message: "Missing required column: customer_number" });
    }

    let insertedCount = 0;
    let updatedCount = 0;
    const errors = [];

    for (let idx = 0; idx < rows.length; idx += 1) {
      const row = rows[idx] || {};
      try {
        const customerNumber = String(row[headerMap.customer_number] || "").trim();
        const companyName = String(row[headerMap.company_name] || "").trim();

        if (!customerNumber) {
          throw new Error("Missing customer_number");
        }
        if (!companyName) {
          throw new Error("Missing company_name");
        }

        const ownerName = headerMap.owner_name ? String(row[headerMap.owner_name] || "").trim() : null;
        const city = headerMap.city ? String(row[headerMap.city] || "").trim() : null;
        const email = headerMap.email ? String(row[headerMap.email] || "").trim() : null;
        const contactEmail = headerMap.contact_email ? String(row[headerMap.contact_email] || "").trim() : null;
        const phone = headerMap.phone ? String(row[headerMap.phone] || "").trim() : null;
        const otpSetup = headerMap.otp_setup ? String(row[headerMap.otp_setup] || "").trim() : null;
        const address = headerMap.address ? String(row[headerMap.address] || "").trim() : null;
        const secInv = headerMap.security_deposit_invoice
          ? String(row[headerMap.security_deposit_invoice] || "").trim()
          : null;
        const refName = headerMap.reference_name ? String(row[headerMap.reference_name] || "").trim() : null;
        const comment = headerMap.comment ? String(row[headerMap.comment] || "").trim() : null;

        let fuelLinkCard = null;
        if (headerMap.fuellink_card && row[headerMap.fuellink_card] != null && row[headerMap.fuellink_card] !== "") {
          fuelLinkCard = parseInt(row[headerMap.fuellink_card], 10);
          if (!Number.isInteger(fuelLinkCard) || fuelLinkCard < 0) {
            throw new Error("Invalid fuellink_card");
          }
        }

        let deposit = null;
        if (headerMap.deposit && row[headerMap.deposit] != null && row[headerMap.deposit] !== "") {
          deposit = parseNumber(row[headerMap.deposit]);
          if (deposit == null) {
            throw new Error("Invalid deposit");
          }
        }

        const customerStatus = headerMap.customer_status
          ? parseBooleanish(row[headerMap.customer_status], true)
          : true;

        const upsertResult = await pool.query(
          `INSERT INTO customers (
            customer_number, company_name, owner_name, city, email, contact_email, phone, fuellink_card, otp_setup,
            deposit, address, security_deposit_invoice, customer_status, reference_name, comment
          )
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
          ON CONFLICT (customer_number) DO UPDATE SET
            company_name = EXCLUDED.company_name,
            owner_name = EXCLUDED.owner_name,
            city = EXCLUDED.city,
            email = EXCLUDED.email,
            contact_email = EXCLUDED.contact_email,
            phone = EXCLUDED.phone,
            fuellink_card = EXCLUDED.fuellink_card,
            otp_setup = EXCLUDED.otp_setup,
            deposit = EXCLUDED.deposit,
            address = EXCLUDED.address,
            security_deposit_invoice = EXCLUDED.security_deposit_invoice,
            customer_status = EXCLUDED.customer_status,
            reference_name = EXCLUDED.reference_name,
            comment = EXCLUDED.comment
          RETURNING (xmax = 0) AS inserted`,
          [
            customerNumber,
            companyName,
            ownerName || null,
            city || null,
            email || null,
            contactEmail || null,
            phone || null,
            fuelLinkCard,
            otpSetup || null,
            deposit,
            address || null,
            secInv || null,
            customerStatus,
            refName || null,
            comment || null,
          ]
        );

        if (upsertResult.rows[0]?.inserted) {
          insertedCount += 1;
        } else {
          updatedCount += 1;
        }
      } catch (err) {
        if (errors.length < 50) {
          errors.push({ row: idx + 2, error: err.message });
        }
      }
    }

    return res.json({
      inserted_count: insertedCount,
      updated_count: updatedCount,
      errors,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ message: "Server error" });
  } finally {
    safeUnlink(req.file?.path);
  }
});

router.get("/customers/:id", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    await ensureCustomersSchemaCompat();

    const id = parseInt(req.params.id, 10);
    if (!Number.isInteger(id) || id < 1) {
      return res.status(400).json({ message: "Invalid customer id" });
    }

    const result = await pool.query(
      `SELECT ${customerColumns}
       FROM customers
       WHERE id = $1`,
      [id]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "Customer not found" });
    }

    return res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ message: "Server error" });
  }
});

router.get("/customers/:id/portal-login", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    await ensureCustomersSchemaCompat();

    const customerId = parseInt(req.params.id, 10);
    if (!Number.isInteger(customerId) || customerId < 1) {
      return res.status(400).json({ message: "Invalid customer id" });
    }

    const result = await pool.query(
      `SELECT email, must_change_password, last_login_at
       FROM users
       WHERE customer_id = $1 AND role = 'customer'
       LIMIT 1`,
      [customerId]
    );

    if (result.rows.length === 0) {
      return res.json({
        exists: false,
        email: null,
        must_change_password: null,
        last_login_at: null,
      });
    }

    const row = result.rows[0];
    return res.json({
      exists: true,
      email: row.email || null,
      must_change_password: row.must_change_password ?? null,
      last_login_at: row.last_login_at || null,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ message: "Server error" });
  }
});

router.post("/customers/:id/portal-login", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    await ensureCustomersSchemaCompat();

    const customerId = parseInt(req.params.id, 10);
    if (!Number.isInteger(customerId) || customerId < 1) {
      return res.status(400).json({ message: "Invalid customer id" });
    }

    let email = String(req.body?.email || "").trim();

    if (!email) {
      const customerResult = await pool.query(
        "SELECT contact_email, email FROM customers WHERE id = $1",
        [customerId]
      );
      if (customerResult.rows.length === 0) {
        return res.status(404).json({ message: "Customer not found" });
      }
      const customerRow = customerResult.rows[0];
      email = String(customerRow.contact_email || customerRow.email || "").trim();
    }

    if (!email) {
      return res.status(400).json({ message: "email is required" });
    }

    const existingUser = await pool.query(
      "SELECT id, email FROM users WHERE customer_id = $1 AND role = 'customer' LIMIT 1",
      [customerId]
    );

    const tempPassword = generateTempPassword();
    const hashedPassword = await bcrypt.hash(tempPassword, 10);

    if (existingUser.rows.length > 0) {
      const user = existingUser.rows[0];
      await pool.query(
        "UPDATE users SET password = $1, must_change_password = true WHERE id = $2",
        [hashedPassword, user.id]
      );

      return res.json({
        exists: true,
        email: user.email,
        temp_password: tempPassword,
      });
    }

    await pool.query(
      `INSERT INTO users (email, password, role, customer_id, must_change_password)
       VALUES ($1, $2, 'customer', $3, true)
       ON CONFLICT (email) DO UPDATE SET
         customer_id = EXCLUDED.customer_id,
         role = 'customer',
         password = EXCLUDED.password,
         must_change_password = true`,
      [email, hashedPassword, customerId]
    );

    return res.json({
      exists: true,
      email,
      temp_password: tempPassword,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ message: "Server error" });
  }
});

router.post("/customers/:id/portal-login/reset", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;

    const customerId = parseInt(req.params.id, 10);
    if (!Number.isInteger(customerId) || customerId < 1) {
      return res.status(400).json({ message: "Invalid customer id" });
    }

    const existingUser = await pool.query(
      "SELECT id, email FROM users WHERE customer_id = $1 AND role = 'customer' LIMIT 1",
      [customerId]
    );

    if (existingUser.rows.length === 0) {
      return res.status(404).json({ message: "Customer portal user not found" });
    }

    const user = existingUser.rows[0];
    const tempPassword = generateTempPassword();
    const hashedPassword = await bcrypt.hash(tempPassword, 10);

    await pool.query(
      "UPDATE users SET password = $1, must_change_password = true WHERE id = $2",
      [hashedPassword, user.id]
    );

    return res.json({
      email: user.email,
      temp_password: tempPassword,
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ message: "Server error" });
  }
});

router.post("/customers", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    await ensureCustomersSchemaCompat();

    const {
      customer_number,
      company_name,
      owner_name,
      city,
      email,
      contact_email,
      phone,
      fuellink_card,
      otp_setup,
      deposit,
      address,
      security_deposit_invoice,
      customer_status,
      reference_name,
      comment,
    } = req.body || {};

    const customerNumber = String(customer_number || "").trim();
    if (!customerNumber) {
      return res.status(400).json({ message: "customer_number is required" });
    }

    const companyName = String(company_name || "").trim();
    if (!companyName) {
      return res.status(400).json({ message: "company_name is required" });
    }

    const fuellinkCard = fuellink_card == null || fuellink_card === ""
      ? null
      : parseInt(fuellink_card, 10);
    if (fuellinkCard != null && (!Number.isInteger(fuellinkCard) || fuellinkCard < 0)) {
      return res.status(400).json({ message: "Invalid fuellink_card" });
    }

    const depositNum = deposit == null || deposit === ""
      ? null
      : parseNumber(deposit);
    if (depositNum == null && deposit != null && deposit !== "") {
      return res.status(400).json({ message: "Invalid deposit" });
    }

    const status = parseBooleanish(customer_status, true);

    const result = await pool.query(
      `INSERT INTO customers (
        customer_number, company_name, owner_name, city, email, contact_email, phone, fuellink_card, otp_setup,
        deposit, address, security_deposit_invoice, customer_status, reference_name, comment
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
      RETURNING ${customerColumns}`,
      [
        customerNumber,
        companyName,
        owner_name || null,
        city || null,
        email || null,
        contact_email || null,
        phone || null,
        fuellinkCard,
        otp_setup || null,
        depositNum,
        address || null,
        security_deposit_invoice || null,
        status,
        reference_name || null,
        comment || null,
      ]
    );

    return res.status(201).json(result.rows[0]);
  } catch (err) {
    if (err?.code === "23505") {
      return res.status(409).json({ message: "customer_number already exists" });
    }
    console.error(err);
    return res.status(500).json({ message: "Server error" });
  }
});

router.patch("/customers/:id", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    await ensureCustomersSchemaCompat();

    const id = parseInt(req.params.id, 10);
    if (!Number.isInteger(id) || id < 1) {
      return res.status(400).json({ message: "Invalid customer id" });
    }

    const fieldMap = {
      customer_number: "customer_number",
      company_name: "company_name",
      owner_name: "owner_name",
      city: "city",
      email: "email",
      contact_email: "contact_email",
      phone: "phone",
      fuellink_card: "fuellink_card",
      otp_setup: "otp_setup",
      deposit: "deposit",
      address: "address",
      security_deposit_invoice: "security_deposit_invoice",
      customer_status: "customer_status",
      reference_name: "reference_name",
      comment: "comment",
      rate_group_id: "rate_group_id",
    };

    const updates = [];
    const values = [];
    Object.entries(fieldMap).forEach(([bodyKey, column]) => {
      if (Object.prototype.hasOwnProperty.call(req.body || {}, bodyKey)) {
        let value = req.body[bodyKey];

        if (bodyKey === "fuellink_card" || bodyKey === "rate_group_id") {
          value = value == null || value === "" ? null : parseInt(value, 10);
          if (value != null && (!Number.isInteger(value) || value < 0)) {
            throw new Error(`Invalid ${bodyKey}`);
          }
          if (bodyKey === "rate_group_id" && value != null && value < 1) {
            throw new Error("Invalid rate_group_id");
          }
        }
        if (bodyKey === "deposit") {
          value = value == null || value === "" ? null : parseNumber(value);
          if (value == null && req.body[bodyKey] != null && req.body[bodyKey] !== "") {
            throw new Error("Invalid deposit");
          }
        }
        if (bodyKey === "customer_status") {
          value = parseBooleanish(value, true);
        }
        if (typeof value === "string") {
          value = value.trim();
        }

        values.push(value === "" ? null : value);
        updates.push(`${column} = $${values.length}`);
      }
    });

    if (updates.length === 0) {
      return res.status(400).json({ message: "No fields provided to update" });
    }

    values.push(id);
    const result = await pool.query(
      `UPDATE customers
       SET ${updates.join(", ")}
       WHERE id = $${values.length}
       RETURNING ${customerColumns}`,
      values
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "Customer not found" });
    }

    return res.json(result.rows[0]);
  } catch (err) {
    if (err?.message?.startsWith("Invalid ")) {
      return res.status(400).json({ message: err.message });
    }
    if (err?.code === "23505") {
      return res.status(409).json({ message: "customer_number already exists" });
    }
    console.error(err);
    return res.status(500).json({ message: "Server error" });
  }
});

// =======================
// CARDS (Admin)
// =======================

router.get("/cards", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;

    const columnsResult = await pool.query(
      `SELECT column_name
       FROM information_schema.columns
       WHERE table_schema = 'public'
         AND table_name = 'cards'`
    );

    const columns = new Set(columnsResult.rows.map((row) => row.column_name));
    const hasColumn = (name) => columns.has(name);
    const selectExpr = (name, exprIfExists, alias) =>
      (hasColumn(name) ? exprIfExists : "''") + ` AS "${alias}"`;

    const driverNameExpr = hasColumn("driver_name_1")
      ? hasColumn("driver_name")
        ? "COALESCE(cd.driver_name_1, cd.driver_name, '')"
        : "COALESCE(cd.driver_name_1, '')"
      : hasColumn("driver_name")
      ? "COALESCE(cd.driver_name, '')"
      : "''";

    const changeDateExpr = hasColumn("change_date")
      ? "COALESCE(to_char(change_date, 'YYYY-MM-DD'), '')"
      : "''";

    const selectClauses = [
      selectExpr("card_number", "COALESCE(card_number, '')", "Card #"),
      `${changeDateExpr} AS "Change date"`,
      selectExpr("status", "COALESCE(status, '')", "Status"),
      `${driverNameExpr} AS "Driver Name 1"`,
      selectExpr("pin", "COALESCE(pin, '')", "PIN #"),
      `COALESCE(c.company_name, cd.company_name, '') AS "Company Name"`,
      `COALESCE(c.customer_number, cd.customer_number, '') AS "Customer Number"`,
      selectExpr("customer_id", "customer_id", "customer_id"),
    ];

    const orderBy = hasColumn("updated_at")
      ? "cd.updated_at DESC NULLS LAST, cd.id DESC"
      : "cd.id DESC";

    const result = await pool.query(
      `SELECT
         ${selectClauses.join(",\n         ")}
       FROM cards cd
       LEFT JOIN customers c ON c.id = cd.customer_id
       ORDER BY ${orderBy}`
    );

    return res.json(result.rows);
  } catch (err) {
    console.error("Admin cards list error:", err);
    return res.status(500).json({ message: "Server error" });
  }
});

module.exports = router;
