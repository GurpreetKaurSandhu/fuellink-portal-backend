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
  if (match) return match[1].toUpperCase();
  const tail = String(location).trim().match(/(AB|BC|MB|NB|NL|NS|NT|NU|ON|PE|QC|SK|YT)$/i);
  return tail ? tail[1].toUpperCase() : null;
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

  const raw = String(value).trim();
  if (!raw) return null;

  const direct = new Date(raw);
  if (!Number.isNaN(direct.getTime())) return direct;

  const normalized = raw.replace(/\./g, "/").replace(/\s+/g, " ");
  const match = normalized.match(
    /^(\d{1,4})[/-](\d{1,2})[/-](\d{1,4})(?:\s+(\d{1,2}:\d{2}(?::\d{2})?)(?:\s*([AP]M))?)?$/i
  );
  if (!match) return null;

  let a = parseInt(match[1], 10);
  let b = parseInt(match[2], 10);
  let c = parseInt(match[3], 10);
  const timePart = match[4] || "00:00:00";
  const ampm = String(match[5] || "").toUpperCase();

  let year;
  let month;
  let day;
  if (String(match[1]).length === 4) {
    year = a;
    month = b;
    day = c;
  } else if (String(match[3]).length === 4) {
    year = c;
    if (a > 12 && b <= 12) {
      day = a;
      month = b;
    } else if (b > 12 && a <= 12) {
      month = a;
      day = b;
    } else {
      month = a;
      day = b;
    }
  } else {
    return null;
  }

  const timeMatch = timePart.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!timeMatch) return null;
  let hours = parseInt(timeMatch[1], 10);
  const minutes = parseInt(timeMatch[2], 10);
  const seconds = parseInt(timeMatch[3] || "0", 10);
  if (ampm === "PM" && hours < 12) hours += 12;
  if (ampm === "AM" && hours === 12) hours = 0;

  const parsed = new Date(year, month - 1, day, hours, minutes, seconds);
  return Number.isNaN(parsed.getTime()) ? null : parsed;
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

const ensureGeneratedInvoicesSchemaCompat = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS customer_invoices (
      id bigserial PRIMARY KEY,
      customer_id int NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
      invoice_no text NOT NULL,
      invoice_date date NOT NULL DEFAULT CURRENT_DATE,
      period_start date,
      period_end date,
      subtotal numeric(12,2) NOT NULL DEFAULT 0,
      gst numeric(12,2) NOT NULL DEFAULT 0,
      hst numeric(12,2) NOT NULL DEFAULT 0,
      pst numeric(12,2) NOT NULL DEFAULT 0,
      qst numeric(12,2) NOT NULL DEFAULT 0,
      total numeric(12,2) NOT NULL DEFAULT 0,
      totals_provided boolean NOT NULL DEFAULT false,
      status text NOT NULL DEFAULT 'issued',
      created_at timestamptz NOT NULL DEFAULT now()
    )
  `);
  await pool.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_customer_invoices_customer_invoice_no'
      ) THEN
        ALTER TABLE customer_invoices
          ADD CONSTRAINT uq_customer_invoices_customer_invoice_no UNIQUE (customer_id, invoice_no);
      END IF;
    END $$;
  `);
};

const parseDateOnly = (value) => {
  if (!value) return null;
  const parsed = new Date(String(value).trim());
  if (Number.isNaN(parsed.getTime())) return null;
  return parsed.toISOString().slice(0, 10);
};

const ensureInvoiceBatchPhase1Schema = async () => {
  await pool.query(`
    ALTER TABLE transactions
      ADD COLUMN IF NOT EXISTS is_invoiced BOOLEAN NOT NULL DEFAULT false,
      ADD COLUMN IF NOT EXISTS invoice_batch_id BIGINT NULL,
      ADD COLUMN IF NOT EXISTS invoice_id BIGINT NULL
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS invoice_batches (
      id BIGSERIAL PRIMARY KEY,
      batch_code TEXT UNIQUE NOT NULL,
      status TEXT NOT NULL DEFAULT 'DRAFT',
      date_from DATE,
      date_to DATE,
      created_by_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS invoice_batch_transactions (
      id BIGSERIAL PRIMARY KEY,
      invoice_batch_id BIGINT NOT NULL REFERENCES invoice_batches(id) ON DELETE CASCADE,
      transaction_id BIGINT NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
      customer_id INTEGER REFERENCES customers(id) ON DELETE SET NULL,
      line_status TEXT NOT NULL DEFAULT 'PENDING',
      issue_note TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      UNIQUE (invoice_batch_id, transaction_id)
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_invoice_batches_status
      ON invoice_batches (status, created_at DESC)
  `);
  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_invoice_batch_transactions_batch
      ON invoice_batch_transactions (invoice_batch_id, line_status)
  `);
};

const makeBatchCode = () => {
  const d = new Date();
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  const seq = String(Math.floor(Math.random() * 10000)).padStart(4, "0");
  return `BATCH-${y}${m}${day}-${seq}`;
};

const findCustomersMissingRateGroupForRange = async ({ customerId, dateFrom, dateTo }) => {
  const values = [dateFrom, dateTo];
  let customerFilter = "";
  if (customerId) {
    values.push(customerId);
    customerFilter = `AND t.customer_id = $${values.length}`;
  }

  const assignmentsExistsResult = await pool.query(
    `SELECT to_regclass('public.customer_rate_group_assignments') IS NOT NULL AS exists`
  );
  const hasAssignmentsTable = !!assignmentsExistsResult.rows[0]?.exists;

  const query = hasAssignmentsTable
    ? `
      SELECT DISTINCT
        c.id AS customer_id,
        c.customer_number,
        c.company_name
      FROM transactions t
      JOIN customers c ON c.id = t.customer_id
      LEFT JOIN LATERAL (
        SELECT crga.rate_group_id
        FROM customer_rate_group_assignments crga
        WHERE crga.customer_id = t.customer_id
          AND crga.start_date <= COALESCE(t.purchase_datetime::date, CURRENT_DATE)
          AND (crga.end_date IS NULL OR crga.end_date >= COALESCE(t.purchase_datetime::date, CURRENT_DATE))
        ORDER BY crga.start_date DESC, crga.id DESC
        LIMIT 1
      ) cra ON TRUE
      WHERE t.purchase_datetime >= $1
        AND t.purchase_datetime <= $2
        ${customerFilter}
        AND COALESCE(cra.rate_group_id, c.rate_group_id) IS NULL
      ORDER BY c.company_name ASC NULLS LAST, c.id ASC
    `
    : `
      SELECT DISTINCT
        c.id AS customer_id,
        c.customer_number,
        c.company_name
      FROM transactions t
      JOIN customers c ON c.id = t.customer_id
      WHERE t.purchase_datetime >= $1
        AND t.purchase_datetime <= $2
        ${customerFilter}
        AND c.rate_group_id IS NULL
      ORDER BY c.company_name ASC NULLS LAST, c.id ASC
    `;

  const result = await pool.query(query, values);
  return result.rows;
};

const normalizeKey = (value) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");

const ensureInvoiceBatchPhase2Schema = async () => {
  await ensureInvoiceBatchPhase1Schema();
  await pool.query(`
    CREATE TABLE IF NOT EXISTS customer_markup_rules (
      id BIGSERIAL PRIMARY KEY,
      customer_id INTEGER NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
      product TEXT,
      province TEXT,
      site TEXT,
      markup_type TEXT NOT NULL CHECK (markup_type IN ('per_liter', 'percent')),
      markup_value NUMERIC(12,6) NOT NULL,
      priority INTEGER NOT NULL DEFAULT 100,
      is_active BOOLEAN NOT NULL DEFAULT true,
      effective_from DATE,
      effective_to DATE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);

  await pool.query(`
    ALTER TABLE invoice_batches
      ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ,
      ADD COLUMN IF NOT EXISTS reviewed_by_user_id INTEGER REFERENCES users(id) ON DELETE SET NULL
  `);

  await pool.query(`
    ALTER TABLE invoice_batch_transactions
      ADD COLUMN IF NOT EXISTS rate_group_id INTEGER,
      ADD COLUMN IF NOT EXISTS rate_group_name TEXT,
      ADD COLUMN IF NOT EXISTS rate_source_effective_date DATE,
      ADD COLUMN IF NOT EXISTS base_rate NUMERIC(12,6),
      ADD COLUMN IF NOT EXISTS markup_rule_id BIGINT,
      ADD COLUMN IF NOT EXISTS markup_rule_used TEXT,
      ADD COLUMN IF NOT EXISTS markup_type TEXT,
      ADD COLUMN IF NOT EXISTS markup_value NUMERIC(12,6),
      ADD COLUMN IF NOT EXISTS rate_per_ltr NUMERIC(12,6),
      ADD COLUMN IF NOT EXISTS subtotal NUMERIC(14,4),
      ADD COLUMN IF NOT EXISTS gst NUMERIC(14,4),
      ADD COLUMN IF NOT EXISTS pst NUMERIC(14,4),
      ADD COLUMN IF NOT EXISTS qst NUMERIC(14,4),
      ADD COLUMN IF NOT EXISTS amount_total NUMERIC(14,4),
      ADD COLUMN IF NOT EXISTS markup_checked BOOLEAN NOT NULL DEFAULT false,
      ADD COLUMN IF NOT EXISTS flags TEXT[] DEFAULT ARRAY[]::TEXT[]
  `);
};

const ensureInvoiceNumberSequenceSchema = async () => {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS invoice_number_sequences (
      month_key CHAR(6) PRIMARY KEY,
      last_seq INTEGER NOT NULL DEFAULT 0,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )
  `);
  await pool.query(`ALTER TABLE customer_invoices ADD COLUMN IF NOT EXISTS due_date DATE`);
};

const generateInvoiceNo = async (client, invoiceDate) => {
  const d = invoiceDate ? new Date(invoiceDate) : new Date();
  if (Number.isNaN(d.getTime())) {
    throw new Error("Invalid invoice date");
  }
  const monthKey = `${d.getFullYear()}${String(d.getMonth() + 1).padStart(2, "0")}`;

  const result = await client.query(
    `INSERT INTO invoice_number_sequences (month_key, last_seq)
     VALUES ($1, 1)
     ON CONFLICT (month_key)
     DO UPDATE SET last_seq = invoice_number_sequences.last_seq + 1, updated_at = now()
     RETURNING last_seq`,
    [monthKey]
  );

  const seq = result.rows[0]?.last_seq || 1;
  return `FL-${monthKey}-${String(seq).padStart(4, "0")}`;
};

const getEffectiveRateGroupForTx = async (client, customerId, txDate) => {
  const rateGroupColumns = await client.query(
    `SELECT column_name
     FROM information_schema.columns
     WHERE table_schema='public' AND table_name='rate_groups'`
  );
  const rateGroupColumnSet = new Set(rateGroupColumns.rows.map((r) => r.column_name));
  const readyExpr = rateGroupColumnSet.has("is_ready")
    ? "COALESCE(rg.is_ready, true)"
    : "true";

  const hasAssignmentsResult = await client.query(
    `SELECT to_regclass('public.customer_rate_group_assignments') IS NOT NULL AS exists`
  );
  const hasAssignments = !!hasAssignmentsResult.rows[0]?.exists;

  if (hasAssignments) {
    const result = await client.query(
      `SELECT
         COALESCE(cra.rate_group_id, c.rate_group_id) AS rate_group_id,
         rg.name AS rate_group_name,
         rg.markup_per_liter,
         ${readyExpr} AS is_ready
       FROM customers c
       LEFT JOIN LATERAL (
         SELECT crga.rate_group_id
         FROM customer_rate_group_assignments crga
         WHERE crga.customer_id = c.id
           AND crga.start_date <= $2::date
           AND (crga.end_date IS NULL OR crga.end_date >= $2::date)
         ORDER BY crga.start_date DESC, crga.id DESC
         LIMIT 1
       ) cra ON TRUE
       LEFT JOIN rate_groups rg ON rg.id = COALESCE(cra.rate_group_id, c.rate_group_id)
       WHERE c.id = $1
       LIMIT 1`,
      [customerId, txDate]
    );
    return result.rows[0] || null;
  }

  const fallback = await client.query(
    `SELECT c.rate_group_id, rg.name AS rate_group_name, rg.markup_per_liter, true AS is_ready
     FROM customers c
     LEFT JOIN rate_groups rg ON rg.id = c.rate_group_id
     WHERE c.id = $1
     LIMIT 1`,
    [customerId]
  );
  return fallback.rows[0] || null;
};

const getTaxRatesForTx = async (client, province, txDateTime) => {
  const hasTaxTable = await client.query(
    `SELECT to_regclass('public.tax_rules') IS NOT NULL AS exists`
  );
  if (!hasTaxTable.rows[0]?.exists) {
    return { gst_rate: 0, pst_rate: 0, qst_rate: 0 };
  }
  const result = await client.query(
    `SELECT COALESCE(gst_rate,0) AS gst_rate, COALESCE(pst_rate,0) AS pst_rate, COALESCE(qst_rate,0) AS qst_rate
     FROM tax_rules
     WHERE province = $1
       AND effective_from <= $2
     ORDER BY effective_from DESC
     LIMIT 1`,
    [String(province || "").toUpperCase(), txDateTime]
  );
  return result.rows[0] || { gst_rate: 0, pst_rate: 0, qst_rate: 0 };
};

const findBestMarkupRule = async (client, { customerId, product, province, location, txDate, fallbackMarkup }) => {
  const rulesResult = await client.query(
    `SELECT id, customer_id, product, province, site, markup_type, markup_value, priority
     FROM customer_markup_rules
     WHERE customer_id = $1
       AND is_active = true
       AND (effective_from IS NULL OR effective_from <= $2::date)
       AND (effective_to IS NULL OR effective_to >= $2::date)
     ORDER BY priority ASC, id DESC`,
    [customerId, txDate]
  );

  const txProduct = normalizeKey(product);
  const txProvince = normalizeKey(province);
  const txSite = normalizeKey(location);

  const scored = rulesResult.rows
    .map((rule) => {
      const ruleSite = normalizeKey(rule.site);
      const ruleProduct = normalizeKey(rule.product);
      const ruleProvince = normalizeKey(rule.province);
      const siteMatch = !ruleSite || ruleSite === txSite;
      const productMatch = !ruleProduct || ruleProduct === txProduct;
      const provinceMatch = !ruleProvince || ruleProvince === txProvince;
      if (!siteMatch || !productMatch || !provinceMatch) return null;

      let specificity = 0;
      if (ruleSite && ruleProduct) specificity = 600;
      else if (ruleSite) specificity = 500;
      else if (ruleProvince && ruleProduct) specificity = 400;
      else if (ruleProvince) specificity = 300;
      else if (ruleProduct) specificity = 200;
      else specificity = 100;

      return { ...rule, specificity };
    })
    .filter(Boolean)
    .sort((a, b) => {
      if (b.specificity !== a.specificity) return b.specificity - a.specificity;
      if ((a.priority || 0) !== (b.priority || 0)) return (a.priority || 0) - (b.priority || 0);
      return (b.id || 0) - (a.id || 0);
    });

  if (scored.length > 0) {
    return {
      markup_rule_id: scored[0].id,
      markup_rule_used: "CUSTOMER_MARKUP_RULE",
      markup_type: scored[0].markup_type,
      markup_value: Number(scored[0].markup_value) || 0,
    };
  }

  if (Number.isFinite(Number(fallbackMarkup))) {
    return {
      markup_rule_id: null,
      markup_rule_used: "RATE_GROUP_FALLBACK",
      markup_type: "per_liter",
      markup_value: Number(fallbackMarkup) || 0,
    };
  }

  return null;
};

const getBaseRateForTx = async (client, { txDate, location, province }) => {
  const result = await client.query(
    `SELECT rl.base_price, rf.effective_date
     FROM rates_lines rl
     JOIN rates_files rf ON rf.id = rl.rates_file_id
     WHERE COALESCE(rf.customer_id, 0) = 0
       AND rf.effective_date <= $1::date
       AND COALESCE(rl.base_price, 0) > 0
       AND (
         regexp_replace(lower(COALESCE(rl.site_name,'')), '[^a-z0-9]+', '', 'g') =
         regexp_replace(lower(COALESCE($2,'')), '[^a-z0-9]+', '', 'g')
         OR (
           COALESCE($3,'') <> '' AND upper(COALESCE(rl.province,'')) = upper($3)
         )
       )
     ORDER BY rf.effective_date DESC, rl.id DESC
     LIMIT 1`,
    [txDate, location, province]
  );
  return result.rows[0] || null;
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
  const lines = String(text || "")
    .split(/\r?\n/)
    .map((line) => String(line || "").replace(/\s+/g, " ").trim())
    .filter(Boolean);
  const parsed = [];
  const dateTimeRegex = /(20\d{2}[/-]\d{2}[/-]\d{2})(?:\s+(\d{2}:\d{2}))?/;
  const cleanDriver = (value) =>
    String(value || "")
      .replace(/\s+/g, " ")
      .replace(/([A-Za-z])(\d{3,})/g, "$1 $2")
      .trim();
  const tryParseCompactLine = (rawLine) => {
    const line = String(rawLine || "").replace(/\s+/g, " ").trim();
    if (!line) return null;
    if (!dateTimeRegex.test(line)) return null;
    if (/selection\s*:|product category|page\s*:/i.test(line)) return null;

    const dt = line.match(dateTimeRegex);
    if (!dt) return null;
    const dtStart = dt.index || 0;
    const dtToken = `${dt[1]}${dt[2] ? ` ${dt[2]}` : ""}`;
    const purchase_datetime = parseDateTime(dtToken);
    if (!purchase_datetime) return null;

    const left = line.slice(0, dtStart).trim();
    const right = line.slice(dtStart + dt[0].length).trim();

    const cardMatch = left.match(/^(\d{4})/);
    if (!cardMatch) return null;
    const card_number = cardMatch[1];
    const driver_name = cleanDriver(left.slice(cardMatch[0].length));

    const compact = right.replace(/\s+/g, " ");
    const productMatch = compact.match(/\b(DSL-LS|DEF BULK|DEF|DSL|ULS|GAS|REG|PREM)\b/i);
    if (!productMatch) return null;
    const product = String(productMatch[1] || "").trim().toUpperCase();

    const afterProduct = compact.slice((productMatch.index || 0) + productMatch[0].length);
    const numbersAfterProduct = afterProduct.match(/\d+\.\d{2}/g) || [];
    const volume_liters = parseNumber(numbersAfterProduct[0]);
    const amount = parseNumber(numbersAfterProduct[1] || null);
    if (!Number.isFinite(volume_liters) || volume_liters <= 0 || volume_liters > 2000) return null;

    const beforeProduct = compact.slice(0, productMatch.index || 0).trim();
    const docCandidateBefore = beforeProduct.match(/([A-Z0-9]{6,})\s*$/i);
    const docCandidateAfter = afterProduct.match(/\b([A-Z0-9]{6,})\b/i);
    const document_number = String(
      (docCandidateBefore && docCandidateBefore[1]) || (docCandidateAfter && docCandidateAfter[1]) || ""
    ).trim() || null;
    const locationRaw = beforeProduct.replace(/([A-Z0-9]{6,})\s*$/i, "").trim();
    const location = locationRaw || null;

    if (!product || /^(to|from|category|all)$/i.test(product)) return null;
    if (!Number.isFinite(volume_liters) || volume_liters <= 0 || volume_liters > 2000) return null;

    return {
      card_number,
      driver_name: driver_name || null,
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
        raw_line: line,
        parser_mode: "compact-line",
      },
    };
  };

  const isDateToken = (token) =>
    /^\d{4}[/-]\d{1,2}[/-]\d{1,2}$/.test(token) ||
    /^\d{1,2}[/-]\d{1,2}[/-]\d{4}$/.test(token) ||
    /^\d{1,2}-[A-Za-z]{3}-\d{4}$/.test(token);

  const isTimeToken = (token) =>
    /^\d{1,2}:\d{2}(?::\d{2})?$/.test(token) ||
    /^\d{1,2}:\d{2}(?::\d{2})?[AP]M$/i.test(token);

  // Dedicated parser for Petro-Pass "Fuel Pricing Report by Card" PDFs.
  const tryParseFuelPricingReport = () => {
    if (!/Fuel Pricing Report by Card/i.test(String(text || ""))) return [];

    const productRegex = /\b(DSL-LS|DEF BULK|DEF|DSL|ULS|GAS|REG|PREM|OCT)\b/i;
    const provinceDocProductRegex =
      /^(?:[PR]\s*)?(.*?)(AB|BC|MB|NB|NL|NS|NT|NU|ON|PE|QC|SK|YT)(\d{7,}[A-Z]?)(DSL-LS|DEF BULK|DEF|DSL|ULS|GAS|REG|PREM|OCT)(.*)$/i;
    const amountOnlyRegex = /^\d{1,3}(?:,\d{3})*(?:\.\d{2})$/;

    const parsedFuel = [];
    let carryCard = "";
    let carryDriverLines = [];

    for (let i = 0; i < lines.length; i += 1) {
      const line = String(lines[i] || "").trim();
      if (!line) continue;

      if (
        /Fuel Pricing Report by Card|Selection:|Account:|Page:|Card #Driver Name 1Driver Name 2Date|IN Tax|^\(Memo\)$|^PFT$|^FCT\/PCT$|^Urban$/i.test(
          line
        )
      ) {
        continue;
      }
      if (/^Total for Card\b/i.test(line)) {
        carryCard = "";
        carryDriverLines = [];
        continue;
      }

      const hasDate = /20\d{2}\/\d{2}\/\d{2}/.test(line);
      const startsWithCard = /^\d{4,}/.test(line);

      if (startsWithCard && !hasDate) {
        const m = line.match(/^(\d{4})\d*(.*)$/);
        carryCard = m?.[1] || "";
        const rest = String(m?.[2] || "").trim();
        carryDriverLines = rest ? [rest] : [];
        continue;
      }

      if (!hasDate) {
        // likely continuation driver/location line
        if (carryCard && line.length <= 40 && !/\d+\.\d{2}/.test(line)) {
          carryDriverLines.push(line);
        }
        continue;
      }

      let rowLine = line;
      if (/^20\d{2}\/\d{2}\/\d{2}/.test(line) && carryCard) {
        rowLine = `${carryCard} ${carryDriverLines.join(" ")} ${line}`.replace(/\s+/g, " ").trim();
      }

      // Merge wrapped location lines until product appears.
      let look = i + 1;
      while (look < lines.length && look <= i + 4 && !productRegex.test(rowLine)) {
        const next = String(lines[look] || "").trim();
        if (!next) break;
        if (/^Total for Card\b|Fuel Pricing Report by Card|Selection:|Account:|Page:/i.test(next)) break;
        if (/^\d{4,}/.test(next) && /20\d{2}\/\d{2}\/\d{2}/.test(next)) break;
        if (/^20\d{2}\/\d{2}\/\d{2}/.test(next)) break;
        rowLine = `${rowLine} ${next}`.replace(/\s+/g, " ").trim();
        look += 1;
      }

      const cardMatch = rowLine.match(/^(\d{4})\d*/);
      const dateMatch = rowLine.match(/(20\d{2}\/\d{2}\/\d{2})/);
      if (!cardMatch || !dateMatch) continue;

      const card_number = cardMatch[1];
      const purchase_datetime = parseDateTime(dateMatch[1]);
      if (!purchase_datetime) continue;

      const left = rowLine.slice(cardMatch[0].length, dateMatch.index || 0).trim();
      const right = rowLine.slice((dateMatch.index || 0) + dateMatch[0].length).trim();
      const driver_name = cleanDriver(left).replace(/^\((.*)\)$/, "$1") || null;

      const coreMatch = right.match(provinceDocProductRegex);
      if (!coreMatch) continue;

      const locationRaw = normalizeLocationForMatch(coreMatch[1] || "");
      const province = String(coreMatch[2] || "").toUpperCase();
      const document_number = String(coreMatch[3] || "").trim() || null;
      const product = String(coreMatch[4] || "").toUpperCase();
      const numbersRaw = String(coreMatch[5] || "");
      const numericValues = (numbersRaw.match(/\d+\.\d{2}/g) || []).map((v) => parseNumber(v)).filter((v) => Number.isFinite(v));
      const volume_liters = numericValues[0];
      if (!Number.isFinite(volume_liters) || volume_liters <= 0 || volume_liters > 3000) continue;

      // In this report, final Amount is usually on the next line by itself.
      let amount = null;
      const nextLine = String(lines[look] || "").trim();
      if (amountOnlyRegex.test(nextLine)) {
        amount = parseNumber(nextLine);
        i = look; // consume the trailing amount line
      } else {
        amount = parseNumber(numericValues[numericValues.length - 1] || null);
      }

      const ex_tax = numericValues[1] ?? null;
      const fet = numericValues[2] ?? null;
      const pft = numericValues[3] ?? null;
      const fct_pct = numericValues[4] ?? null;
      const urban = numericValues[5] ?? null;
      const in_tax = numericValues[6] ?? null;
      const gst_hst = numericValues[7] ?? null;
      const pst = numericValues[8] ?? null;
      const amountCandidate = numericValues[9] ?? null;

      parsedFuel.push({
        card_number,
        driver_name,
        purchase_datetime,
        location: locationRaw || null,
        city: null,
        province,
        document_number,
        product,
        volume_liters,
        amount: Number.isFinite(amount) ? amount : null,
        source_raw_json: {
          amount: Number.isFinite(amount) ? amount : (Number.isFinite(amountCandidate) ? amountCandidate : null),
          ex_tax,
          fet,
          pft,
          fct_pct,
          urban,
          in_tax,
          gst_hst,
          pst,
          raw_line: rowLine,
          parser_mode: "fuel-pricing-report",
        },
      });
    }

    return parsedFuel;
  };

  const fuelPricingRows = tryParseFuelPricingReport();
  if (fuelPricingRows.length > 0) return fuelPricingRows;

  // Dedicated parser for invoice-style exports where each transaction is spread over multiple lines:
  // Card # + Company + Date + Location + City/Prov/Doc/Product/Numbers + Amount + Driver.
  const tryParseInvoiceStyleReport = () => {
    const textBlob = String(text || "");
    if (
      !/Card\s*#\s*Company\s*Name\s*Date\s*Location\s*City\s*Prov\s*Document\s*#/i.test(textBlob) ||
      !/Product\s*Volume\s*\(L\)/i.test(textBlob)
    ) {
      return [];
    }

    const productRegex = /\b(DSL-LS|DEF BULK|DEFBULK|DEF|DSL|ULS|GAS|REG|PREM|OCT)\b/i;
    const provinceDocProductRegex =
      /^(.*?)(AB|BC|MB|NB|NL|NS|NT|NU|ON|PE|QC|SK|YT)\s*(\d{7,}[A-Z]?)(DSL-LS|DEF BULK|DEFBULK|DEF|DSL|ULS|GAS|REG|PREM|OCT)(.*)$/i;
    const amountOnlyRegex = /^\d{1,3}(?:,\d{3})*(?:\.\d{2})$/;
    const rows = [];

    let i = 0;
    while (i < lines.length) {
      const start = String(lines[i] || "").trim();
      if (!/^\d{4}/.test(start) || /^20\d{2}[/-]\d{2}[/-]\d{2}\b/.test(start)) {
        i += 1;
        continue;
      }

      const cardMatch = start.match(/^(\d{4})/);
      if (!cardMatch) {
        i += 1;
        continue;
      }
      const card_number = cardMatch[1];

      const companyParts = [];
      let cursor = i;
      let dateLine = "";

      const remainder = start.slice(cardMatch[0].length).trim();
      if (remainder && /20\d{2}\/\d{2}\/\d{2}/.test(start)) {
        dateLine = start;
      } else if (remainder) {
        companyParts.push(remainder);
      }

      if (!dateLine) {
        cursor += 1;
        while (cursor < lines.length) {
          const part = String(lines[cursor] || "").trim();
          if (!part) {
            cursor += 1;
            continue;
          }
          if (/Card #Company NameDateLocationCityProvDocument #ProductVolume/i.test(part)) {
            cursor += 1;
            continue;
          }
          if (/^\d{4}/.test(part) && !/20\d{2}\/\d{2}\/\d{2}/.test(part)) {
            break;
          }
          if (/20\d{2}\/\d{2}\/\d{2}/.test(part)) {
            dateLine = part;
            break;
          }
          companyParts.push(part);
          cursor += 1;
        }
      }

      if (!dateLine) {
        i = Math.max(i + 1, cursor);
        continue;
      }

      const dateMatch = dateLine.match(/20\d{2}\/\d{2}\/\d{2}/);
      const purchase_datetime = parseDateTime(dateMatch ? dateMatch[0] : null);
      if (!dateMatch || !purchase_datetime) {
        i = Math.max(i + 1, cursor + 1);
        continue;
      }

      const afterDate = dateLine.slice((dateMatch.index || 0) + dateMatch[0].length).trim();
      const locationParts = [];
      if (afterDate) locationParts.push(afterDate);

      let dataLine = "";
      let dataIndex = cursor + 1;
      while (dataIndex < lines.length) {
        const part = String(lines[dataIndex] || "").trim();
        if (!part) {
          dataIndex += 1;
          continue;
        }
        if (/Card #Company NameDateLocationCityProvDocument #ProductVolume/i.test(part)) {
          dataIndex += 1;
          continue;
        }
        if (/^\d{4}/.test(part)) {
          break;
        }
        if (provinceDocProductRegex.test(part) && productRegex.test(part)) {
          dataLine = part;
          break;
        }
        locationParts.push(part);
        dataIndex += 1;
      }

      if (!dataLine) {
        i = Math.max(i + 1, dataIndex);
        continue;
      }

      const core = dataLine.match(provinceDocProductRegex);
      if (!core) {
        i = Math.max(i + 1, dataIndex + 1);
        continue;
      }

      const city = String(core[1] || "").replace(/^P\s+/i, "").trim() || null;
      const province = String(core[2] || "").toUpperCase();
      const document_number = String(core[3] || "").trim() || null;
      let product = String(core[4] || "").toUpperCase();
      if (product === "DEFBULK") product = "DEF BULK";
      const numericValues = (
        String(core[5] || "").match(/-?\$?\d+(?:,\d{3})*(?:\.\d{1,4})?/g) || []
      )
        .map((v) => parseNumber(v))
        .filter((v) => Number.isFinite(v));

      const volume_liters = numericValues[0];
      if (!Number.isFinite(volume_liters) || volume_liters <= 0 || volume_liters > 10000) {
        i = Math.max(i + 1, dataIndex + 1);
        continue;
      }

      const base_rate = numericValues[1] ?? null;
      const fet = numericValues[2] ?? null;
      const pft = numericValues[3] ?? null;
      const rate_per_ltr = numericValues[4] ?? null;
      const subtotal = numericValues[5] ?? null;
      const gst_hst = numericValues[6] ?? null;
      const pst = numericValues[7] ?? null;
      const qst = numericValues[8] ?? null;

      let amount = Number.isFinite(numericValues[9]) ? numericValues[9] : null;
      let nextIndex = dataIndex + 1;
      const nextLine = String(lines[nextIndex] || "").trim();
      if (amountOnlyRegex.test(nextLine)) {
        amount = parseNumber(nextLine);
        nextIndex += 1;
      }

      const driverParts = [];
      while (nextIndex < lines.length && driverParts.length < 3) {
        const part = String(lines[nextIndex] || "").trim();
        if (!part) {
          nextIndex += 1;
          continue;
        }
        if (/Card #Company NameDateLocationCityProvDocument #ProductVolume/i.test(part)) break;
        if (/^\d{4}/.test(part) || /20\d{2}\/\d{2}\/\d{2}/.test(part)) break;
        if (amountOnlyRegex.test(part)) {
          nextIndex += 1;
          continue;
        }
        driverParts.push(part);
        nextIndex += 1;
      }

      const location = normalizeLocationForMatch(locationParts.join(" ").trim()) || null;
      const company_name = companyParts.join(" ").replace(/\s+/g, " ").trim() || null;
      const driver_name = cleanDriver(driverParts.join(" ")) || null;

      rows.push({
        card_number,
        driver_name,
        purchase_datetime,
        location,
        city,
        province,
        document_number,
        product,
        volume_liters,
        amount: Number.isFinite(amount) ? amount : null,
        source_raw_json: {
          parser_mode: "invoice-style-report",
          company_name,
          city,
          base_rate,
          fet,
          pft,
          rate_per_ltr,
          subtotal,
          gst_hst,
          pst,
          qst,
          amount: Number.isFinite(amount) ? amount : null,
          raw_line: [start, ...companyParts, dateLine, ...locationParts, dataLine, ...driverParts].join(" | "),
        },
      });

      i = Math.max(nextIndex, i + 1);
    }

    return rows;
  };

  const invoiceStyleRows = tryParseInvoiceStyleReport();
  if (invoiceStyleRows.length > 0) return invoiceStyleRows;

  // Fallback parser for tightly wrapped invoice PDFs where rows are split across
  // many lines and company/location tokens are broken aggressively.
  const tryParseInvoiceBlockFallback = () => {
    const rows = [];
    const productRegex = /\b(DSL-LS|DEF BULK|DEFBULK|DEF|DSL|ULS|GAS|REG|PREM|OCT)\b/i;
    const provinceDocProductRegex =
      /^(.*?)(AB|BC|MB|NB|NL|NS|NT|NU|ON|PE|QC|SK|YT)\s*([0-9]{6,}[A-Z]?)(DSL-LS|DEF BULK|DEFBULK|DEF|DSL|ULS|GAS|REG|PREM|OCT)(.*)$/i;
    const amountOnlyRegex = /^\d{1,3}(?:,\d{3})*(?:\.\d{2})$/;
    const isHeaderLine = (line) =>
      /Card\s*#.*Company\s*Name.*Date.*Location.*Prov.*Document\s*#.*Product/i.test(line) ||
      /^Fuel Pricing Report by Card$/i.test(line) ||
      /^Purchase Details by Card$/i.test(line) ||
      /^Selection:/i.test(line) ||
      /^Account:/i.test(line) ||
      /^Page:/i.test(line);

    let i = 0;
    while (i < lines.length) {
      const start = String(lines[i] || "").trim();
      if (
        !start ||
        isHeaderLine(start) ||
        !/^\d{4}/.test(start) ||
        /^20\d{2}[/-]\d{2}[/-]\d{2}\b/.test(start)
      ) {
        i += 1;
        continue;
      }

      const cardMatch = start.match(/^(\d{4})/);
      if (!cardMatch) {
        i += 1;
        continue;
      }
      const card_number = cardMatch[1];
      const companyParts = [];
      const startRemainder = start.slice(cardMatch[0].length).trim();
      if (startRemainder) companyParts.push(startRemainder);

      let cursor = i + 1;
      let dateLine = "";
      while (cursor < lines.length) {
        const part = String(lines[cursor] || "").trim();
        if (!part) {
          cursor += 1;
          continue;
        }
        if (isHeaderLine(part)) {
          cursor += 1;
          continue;
        }
        if (/^\d{4}/.test(part) && !/20\d{2}\/\d{2}\/\d{2}/.test(part)) break;
        if (/20\d{2}\/\d{2}\/\d{2}/.test(part)) {
          dateLine = part;
          break;
        }
        companyParts.push(part);
        cursor += 1;
      }
      if (!dateLine) {
        i += 1;
        continue;
      }

      const dateMatch = dateLine.match(/20\d{2}\/\d{2}\/\d{2}/);
      const purchase_datetime = parseDateTime(dateMatch ? dateMatch[0] : null);
      if (!dateMatch || !purchase_datetime) {
        i = Math.max(i + 1, cursor + 1);
        continue;
      }

      const locationParts = [];
      const afterDate = dateLine.slice((dateMatch.index || 0) + dateMatch[0].length).trim();
      if (afterDate) locationParts.push(afterDate);

      let dataLine = "";
      let dataIndex = cursor + 1;
      while (dataIndex < lines.length) {
        const part = String(lines[dataIndex] || "").trim();
        if (!part) {
          dataIndex += 1;
          continue;
        }
        if (isHeaderLine(part)) {
          dataIndex += 1;
          continue;
        }
        if (/^\d{4}/.test(part) && !/20\d{2}\/\d{2}\/\d{2}/.test(part)) break;
        if (provinceDocProductRegex.test(part) && productRegex.test(part)) {
          dataLine = part;
          break;
        }
        locationParts.push(part);
        dataIndex += 1;
      }
      if (!dataLine) {
        i = Math.max(i + 1, dataIndex);
        continue;
      }

      const core = dataLine.match(provinceDocProductRegex);
      if (!core) {
        i = Math.max(i + 1, dataIndex + 1);
        continue;
      }

      const city = String(core[1] || "").replace(/^P\s+/i, "").trim() || null;
      const province = String(core[2] || "").toUpperCase();
      const document_number = String(core[3] || "").trim() || null;
      let product = String(core[4] || "").toUpperCase();
      if (product === "DEFBULK") product = "DEF BULK";

      const numericValues = (String(core[5] || "").match(/\d+\.\d{2,4}/g) || [])
        .map((v) => parseNumber(v))
        .filter((v) => Number.isFinite(v));

      const volume_liters = numericValues[0];
      if (!Number.isFinite(volume_liters) || volume_liters <= 0 || volume_liters > 20000) {
        i = Math.max(i + 1, dataIndex + 1);
        continue;
      }

      const base_rate = numericValues[1] ?? null;
      const fet = numericValues[2] ?? null;
      const pft = numericValues[3] ?? null;
      const rate_per_ltr = numericValues[4] ?? null;
      const subtotal = numericValues[5] ?? null;
      const gst_hst = numericValues[6] ?? null;
      const pst = numericValues[7] ?? null;
      const qst = numericValues[8] ?? null;

      let amount = Number.isFinite(numericValues[numericValues.length - 1]) ? numericValues[numericValues.length - 1] : null;
      let nextIndex = dataIndex + 1;
      const nextLine = String(lines[nextIndex] || "").trim();
      if (amountOnlyRegex.test(nextLine)) {
        amount = parseNumber(nextLine);
        nextIndex += 1;
      }

      const driverParts = [];
      while (nextIndex < lines.length && driverParts.length < 3) {
        const part = String(lines[nextIndex] || "").trim();
        if (!part) {
          nextIndex += 1;
          continue;
        }
        if (isHeaderLine(part)) break;
        if (/^\d{4}/.test(part) || /20\d{2}\/\d{2}\/\d{2}/.test(part)) break;
        if (amountOnlyRegex.test(part)) {
          nextIndex += 1;
          continue;
        }
        driverParts.push(part);
        nextIndex += 1;
      }

      const location = normalizeLocationForMatch(locationParts.join(" ").trim()) || null;
      const company_name = companyParts.join(" ").replace(/\s+/g, " ").trim() || null;
      const driver_name = cleanDriver(driverParts.join(" ")) || null;

      rows.push({
        card_number,
        driver_name,
        purchase_datetime,
        location,
        city,
        province,
        document_number,
        product,
        volume_liters,
        amount: Number.isFinite(amount) ? amount : null,
        source_raw_json: {
          parser_mode: "invoice-style-fallback",
          company_name,
          city,
          base_rate,
          fet,
          pft,
          rate_per_ltr,
          subtotal,
          gst_hst,
          pst,
          qst,
          amount: Number.isFinite(amount) ? amount : null,
          raw_line: [start, ...companyParts, dateLine, ...locationParts, dataLine, ...driverParts].join(" | "),
        },
      });

      i = Math.max(nextIndex, i + 1);
    }

    return rows;
  };

  const invoiceFallbackRows = tryParseInvoiceBlockFallback();
  if (invoiceFallbackRows.length > 0) return invoiceFallbackRows;

  // Final fallback: date-anchored invoice parser for heavily wrapped exports.
  const tryParseDateAnchoredInvoiceBlocks = () => {
    const rows = [];
    const dateRegex = /20\d{2}\/\d{2}\/\d{2}/;
    const provinceDocProductRegex =
      /^(.*?)(AB|BC|MB|NB|NL|NS|NT|NU|ON|PE|QC|SK|YT)\s*([0-9]{6,}[A-Z]?)(DSL-LS|DEF BULK|DEFBULK|DEF|DSL|ULS|GAS|REG|PREM|OCT)(.*)$/i;
    const amountOnlyRegex = /^\d{1,3}(?:,\d{3})*(?:\.\d{2})$/;

    const isNoise = (line) =>
      !line ||
      /Card\s*#.*Company\s*Name.*Date.*Location.*Prov.*Document\s*#.*Product/i.test(line) ||
      /^Fuel Pricing Report by Card$/i.test(line) ||
      /^Purchase Details by Card$/i.test(line) ||
      /^Selection:/i.test(line) ||
      /^Account:/i.test(line) ||
      /^Page:/i.test(line) ||
      /^\(Memo\)$/i.test(line);

    for (let i = 0; i < lines.length; i += 1) {
      const dateLineRaw = String(lines[i] || "").trim();
      if (!dateRegex.test(dateLineRaw)) continue;

      const dateMatch = dateLineRaw.match(dateRegex);
      const purchase_datetime = parseDateTime(dateMatch ? dateMatch[0] : null);
      if (!purchase_datetime) continue;

      let cardLineIdx = -1;
      for (let b = i - 1; b >= Math.max(0, i - 6); b -= 1) {
        const prev = String(lines[b] || "").trim();
        if (/^\d{4}/.test(prev) && !/^20\d{2}[/-]\d{2}[/-]\d{2}\b/.test(prev)) {
          cardLineIdx = b;
          break;
        }
      }
      if (cardLineIdx < 0) continue;

      const cardLine = String(lines[cardLineIdx] || "").trim();
      const cardMatch = cardLine.match(/^(\d{4})/);
      if (!cardMatch) continue;
      const card_number = cardMatch[1];

      const companyParts = [];
      const firstCompany = cardLine.slice(cardMatch[0].length).trim();
      if (firstCompany) companyParts.push(firstCompany);
      for (let c = cardLineIdx + 1; c < i; c += 1) {
        const part = String(lines[c] || "").trim();
        if (!part || isNoise(part)) continue;
        if (dateRegex.test(part)) continue;
        if (/^\d{4}/.test(part)) break;
        companyParts.push(part);
      }

      const afterDate = dateLineRaw
        .slice((dateMatch.index || 0) + dateMatch[0].length)
        .trim();
      const locationParts = [];
      if (afterDate) locationParts.push(afterDate);

      let dataLine = "";
      let dataIndex = -1;
      for (let d = i + 1; d <= Math.min(lines.length - 1, i + 6); d += 1) {
        const part = String(lines[d] || "").trim();
        if (!part || isNoise(part)) continue;
        if (dateRegex.test(part) || /^\d{4}/.test(part)) break;
        if (provinceDocProductRegex.test(part)) {
          dataLine = part;
          dataIndex = d;
          break;
        }
        locationParts.push(part);
      }
      if (!dataLine) continue;

      const core = dataLine.match(provinceDocProductRegex);
      if (!core) continue;

      const city = String(core[1] || "").replace(/^P\s+/i, "").trim() || null;
      const province = String(core[2] || "").toUpperCase();
      const document_number = String(core[3] || "").trim() || null;
      let product = String(core[4] || "").toUpperCase();
      if (product === "DEFBULK") product = "DEF BULK";

      const numericValues = (String(core[5] || "").match(/\d+\.\d{2,4}/g) || [])
        .map((v) => parseNumber(v))
        .filter((v) => Number.isFinite(v));

      const volume_liters = numericValues[0];
      if (!Number.isFinite(volume_liters) || volume_liters <= 0 || volume_liters > 20000) continue;

      const base_rate = numericValues[1] ?? null;
      const fet = numericValues[2] ?? null;
      const pft = numericValues[3] ?? null;
      const rate_per_ltr = numericValues[4] ?? null;
      const subtotal = numericValues[5] ?? null;
      const gst_hst = numericValues[6] ?? null;
      const pst = numericValues[7] ?? null;
      const qst = numericValues[8] ?? null;

      let amount = Number.isFinite(numericValues[numericValues.length - 1])
        ? numericValues[numericValues.length - 1]
        : null;
      let nextIndex = dataIndex + 1;
      if (nextIndex >= 0) {
        const maybeAmount = String(lines[nextIndex] || "").trim();
        if (amountOnlyRegex.test(maybeAmount)) {
          amount = parseNumber(maybeAmount);
          nextIndex += 1;
        }
      }

      const driverParts = [];
      while (nextIndex >= 0 && nextIndex < lines.length && driverParts.length < 3) {
        const part = String(lines[nextIndex] || "").trim();
        if (!part || isNoise(part)) {
          nextIndex += 1;
          continue;
        }
        if (dateRegex.test(part) || /^\d{4}/.test(part)) break;
        if (amountOnlyRegex.test(part)) {
          nextIndex += 1;
          continue;
        }
        driverParts.push(part);
        nextIndex += 1;
      }

      const location = normalizeLocationForMatch(locationParts.join(" ").trim()) || null;
      const company_name = companyParts.join(" ").replace(/\s+/g, " ").trim() || null;
      const driver_name = cleanDriver(driverParts.join(" ")) || null;

      rows.push({
        card_number,
        driver_name,
        purchase_datetime,
        location,
        city,
        province,
        document_number,
        product,
        volume_liters,
        amount: Number.isFinite(amount) ? amount : null,
        source_raw_json: {
          parser_mode: "invoice-date-anchored-fallback",
          company_name,
          city,
          base_rate,
          fet,
          pft,
          rate_per_ltr,
          subtotal,
          gst_hst,
          pst,
          qst,
          amount: Number.isFinite(amount) ? amount : null,
          raw_line: [cardLine, ...companyParts, dateLineRaw, ...locationParts, dataLine, ...driverParts].join(" | "),
        },
      });
    }

    return rows;
  };

  const invoiceDateFallbackRows = tryParseDateAnchoredInvoiceBlocks();
  if (invoiceDateFallbackRows.length > 0) return invoiceDateFallbackRows;

  for (const line of lines) {
    if (/category\s*:/i.test(line) || /all\s+transactions/i.test(line) || /purchase\s+date/i.test(line)) {
      continue;
    }
    const tokens = line.split(/\s+/).filter(Boolean);
    if (tokens.length < 7) continue;

    let dateIdx = -1;
    for (let i = 0; i < tokens.length; i += 1) {
      if (isDateToken(tokens[i])) {
        dateIdx = i;
        break;
      }
    }
    if (dateIdx < 0) continue;

    let cardIdx = -1;
    for (let i = dateIdx - 1; i >= 0; i -= 1) {
      const digits = String(tokens[i] || "").replace(/\D/g, "");
      if (digits.length >= 4) {
        cardIdx = i;
        break;
      }
    }
    if (cardIdx < 0) continue;

    const cardDigits = String(tokens[cardIdx] || "").replace(/\D/g, "");
    if (cardDigits.length < 4) continue;
    const card_number = cardDigits;

    const rawDate = tokens[dateIdx];
    const rawTime = tokens[dateIdx + 1] || "";
    const rawAmPm = /^[AP]M$/i.test(tokens[dateIdx + 2] || "") ? tokens[dateIdx + 2] : "";
    const hasTime = isTimeToken(rawTime);
    const afterTimeOffset = hasTime ? (rawAmPm ? 3 : 2) : 1;

    const purchase_datetime = parseDateTime(
      `${rawDate}${hasTime ? ` ${rawTime}` : ""}${rawAmPm ? ` ${rawAmPm}` : ""}`.trim()
    );
    if (!purchase_datetime) continue;

    const dataStart = dateIdx + afterTimeOffset;
    if (dataStart >= tokens.length) continue;

    const product = tokens[dataStart] || null;
    if (!product || /^(to|from|category|all)$/i.test(product)) continue;

    let volumeIdx = -1;
    for (let i = dataStart + 1; i < Math.min(tokens.length, dataStart + 8); i += 1) {
      const n = parseNumber(tokens[i]);
      if (Number.isFinite(n) && n > 0) {
        volumeIdx = i;
        break;
      }
    }
    if (volumeIdx < 0) continue;
    const volume_liters = parseNumber(tokens[volumeIdx]);
    if (!Number.isFinite(volume_liters) || volume_liters <= 0 || volume_liters > 1000) continue;

    let amountIdx = -1;
    for (let i = volumeIdx + 1; i < Math.min(tokens.length, volumeIdx + 8); i += 1) {
      const n = parseNumber(tokens[i]);
      if (Number.isFinite(n) && n >= 0) {
        amountIdx = i;
        break;
      }
    }
    const amount = amountIdx >= 0 ? parseNumber(tokens[amountIdx]) : null;

    const docIdx = amountIdx >= 0 ? amountIdx + 1 : volumeIdx + 1;
    const document_number = tokens[docIdx] || null;
    const location = tokens.slice(docIdx + 1).join(" ").trim() || null;
    const driver_name = tokens.slice(cardIdx + 1, dateIdx).join(" ").trim() || null;
    if (/purchase\s*date|all\s*transactions|category/i.test(`${driver_name} ${location}`)) continue;

    const numericExtras = tokens.map((token) => parseNumber(token)).filter((value) => Number.isFinite(value));

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

  if (parsed.length > 0) return parsed;

  // Third attempt for reports where one transaction is wrapped across multiple lines.
  const blocks = [];
  let currentBlock = null;
  for (const line of lines) {
    if (/^\d{4}\b/.test(line)) {
      if (currentBlock) blocks.push(currentBlock);
      currentBlock = line;
      continue;
    }
    if (!currentBlock) continue;
    if (/^0\d{3}\b/.test(line)) {
      blocks.push(currentBlock);
      currentBlock = line;
      continue;
    }
    if (/selection\s*:|product category|page\s*:/i.test(line)) continue;
    if (/^\(memo\)$/i.test(line)) continue;
    currentBlock = `${currentBlock} ${line}`.trim();
  }
  if (currentBlock) blocks.push(currentBlock);

  const blockDateRegex = /(20\d{2}[/-]\d{2}[/-]\d{2})(?:\s+(\d{2}:\d{2}))?/;
  const parseBlock = (block) => {
    const row = String(block || "").replace(/\s+/g, " ").trim();
    if (!row) return null;
    const card = row.match(/^(\d{4})\b/);
    if (!card) return null;
    const card_number = card[1];

    const dt = row.match(blockDateRegex);
    if (!dt) return null;
    const purchase_datetime = parseDateTime(`${dt[1]}${dt[2] ? ` ${dt[2]}` : ""}`);
    if (!purchase_datetime) return null;

    const left = row.slice(card[0].length, dt.index || 0).trim();
    const right = row.slice((dt.index || 0) + dt[0].length).trim();
    const driver_name = cleanDriver(left) || null;

    const productMatch = right.match(/\b(DSL-LS|DEF BULK|DEF|DSL|ULS|GAS|REG|PREM)\b/i);
    if (!productMatch) return null;
    const product = String(productMatch[1] || "").trim().toUpperCase();

    const afterProduct = right.slice((productMatch.index || 0) + productMatch[0].length);
    const nums = afterProduct.match(/\d+\.\d{2}/g) || [];
    const volume_liters = parseNumber(nums[0]);
    if (!Number.isFinite(volume_liters) || volume_liters <= 0 || volume_liters > 2000) return null;

    const amount = parseNumber(nums[1] || null);

    // Prefer document number closest to location start (often ending with P)
    const doc = afterProduct.match(/\b([A-Z0-9]{7,}P?)\b/i);
    const document_number = doc ? doc[1] : null;

    const locationRaw = String(afterProduct || "")
      .replace(/\d+\.\d{2}/g, " ")
      .replace(/\b([A-Z0-9]{7,}P?)\b/i, " ")
      .replace(/\s+/g, " ")
      .trim();
    const location = locationRaw || null;

    return {
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
        raw_line: row,
        parser_mode: "multiline-block",
      },
    };
  };

  for (const block of blocks) {
    const row = parseBlock(block);
    if (row) parsed.push(row);
  }
  if (parsed.length > 0) return parsed;

  // Secondary attempt for compact line formats from Petro-Pass exports.
  for (const line of lines) {
    const row = tryParseCompactLine(line);
    if (row) parsed.push(row);
  }
  if (parsed.length > 0) return parsed;

  // Fallback for PDFs where row text is not split line-by-line predictably.
  const compactText = lines.join("\n");
  const rowRegex =
    /(\d{4,})\s+([A-Za-z][A-Za-z .'-]{0,50})?\s*(\d{4}[/-]\d{1,2}[/-]\d{1,2}|\d{1,2}[/-]\d{1,2}[/-]\d{4}|\d{1,2}-[A-Za-z]{3}-\d{4})\s+(\d{1,2}:\d{2}(?::\d{2})?(?:\s*[AP]M)?)\s+([A-Za-z0-9-]{1,20})\s+([0-9]+(?:\.[0-9]+)?)\s+([0-9]+(?:\.[0-9]+)?)\s+([A-Za-z0-9-]{2,40})\s+([^\n]+)/gi;
  let match = rowRegex.exec(compactText);
  while (match) {
    const card_number = String(match[1] || "").replace(/\D/g, "");
    const driver_name = String(match[2] || "").trim() || null;
    const purchase_datetime = parseDateTime(`${match[3]} ${match[4]}`);
    const product = match[5] || null;
    const volume_liters = parseNumber(match[6]);
    const amount = parseNumber(match[7]);
    const document_number = match[8] || null;
    const location = String(match[9] || "").trim() || null;

    if (card_number.length >= 4 && purchase_datetime && Number.isFinite(volume_liters)) {
      if (volume_liters <= 0 || volume_liters > 1000) {
        match = rowRegex.exec(compactText);
        continue;
      }
      if (!product || /^(to|from|category|all)$/i.test(product)) {
        match = rowRegex.exec(compactText);
        continue;
      }
      if (/purchase\s*date|all\s*transactions|category/i.test(`${driver_name || ""} ${location || ""}`)) {
        match = rowRegex.exec(compactText);
        continue;
      }
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
          raw_line: match[0],
          parser_mode: "regex-fallback",
        },
      });
    }
    match = rowRegex.exec(compactText);
  }

  return parsed;
};

const resolveCustomerIdByCard = async (client, rawCardNumber) => {
  const raw = String(rawCardNumber || "").trim();
  if (!raw) return null;
  const digits = raw.replace(/\D/g, "");
  const last4 = digits.length >= 4 ? digits.slice(-4) : "";

  const result = await client.query(
    `WITH matches AS (
       SELECT
         COALESCE(cd.customer_id, cnum.id, cname.id) AS customer_id,
         CASE
           WHEN cd.card_number = $1 THEN 0
           WHEN regexp_replace(cd.card_number, '\\D', '', 'g') = $2 THEN 1
           ELSE 2
         END AS match_rank,
         cd.id
       FROM cards cd
       LEFT JOIN customers cnum
         ON cnum.customer_number IS NOT NULL
        AND cd.customer_number IS NOT NULL
        AND lower(trim(cnum.customer_number)) = lower(trim(cd.customer_number))
       LEFT JOIN customers cname
         ON cname.company_name IS NOT NULL
        AND cd.company_name IS NOT NULL
        AND lower(trim(cname.company_name)) = lower(trim(cd.company_name))
       WHERE COALESCE(cd.customer_id, cnum.id, cname.id) IS NOT NULL
         AND (
           cd.card_number = $1
           OR regexp_replace(cd.card_number, '\\D', '', 'g') = $2
           OR (
             $3 <> ''
             AND lpad(right(regexp_replace(cd.card_number, '\\D', '', 'g'), 4), 4, '0') = lpad($3, 4, '0')
           )
         )
     )
     SELECT customer_id
     FROM (
       SELECT
         customer_id,
         MIN(match_rank) AS best_rank,
         MAX(id) AS latest_card_id
       FROM matches
       GROUP BY customer_id
     ) ranked
     ORDER BY best_rank ASC, latest_card_id DESC
     LIMIT 10`,
    [raw, digits, last4]
  );

  if (!result.rows.length) return null;
  const uniqueCustomers = [...new Set(result.rows.map((r) => r.customer_id).filter(Boolean))];
  if (uniqueCustomers.length !== 1) return null;
  return uniqueCustomers[0];
};

const resolveCustomerIdByCompanyName = async (client, rawCompanyName) => {
  const company = String(rawCompanyName || "").trim();
  if (!company) return null;

  const normalized = company
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!normalized) return null;

  // If uploaded company starts with customer number (e.g. "5312 (...)"), prefer direct customer_number match.
  const numberPrefixMatch = company.match(/^\s*(\d{3,12})\b/);
  if (numberPrefixMatch) {
    const customerNum = String(numberPrefixMatch[1] || "").trim();
    if (customerNum) {
      const byCustomerNumber = await client.query(
        `SELECT id
         FROM customers
         WHERE customer_number IS NOT NULL
           AND regexp_replace(customer_number, '\\D', '', 'g') = regexp_replace($1, '\\D', '', 'g')
         ORDER BY id DESC
         LIMIT 5`,
        [customerNum]
      );
      const uniqueByNumber = [...new Set(byCustomerNumber.rows.map((r) => r.id).filter(Boolean))];
      if (uniqueByNumber.length === 1) return uniqueByNumber[0];
    }
  }

  // Handle "O/A" aliases by comparing both full legal name and operating-as part.
  const aliasParts = company.split(/\bO\/?A\b/i).map((p) => String(p || "").trim()).filter(Boolean);
  const aliasRaw = aliasParts.length > 1 ? aliasParts[aliasParts.length - 1] : company;
  const aliasNormalized = aliasRaw
    .toLowerCase()
    .replace(/\s+/g, " ")
    .replace(/[^\w\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  const result = await client.query(
    `SELECT id
     FROM customers
     WHERE company_name IS NOT NULL
       AND (
         lower(trim(company_name)) = lower(trim($1))
         OR regexp_replace(lower(company_name), '[^a-z0-9]+', ' ', 'g') = $2
         OR regexp_replace(lower(company_name), '[^a-z0-9]+', ' ', 'g') = $3
         OR regexp_replace(lower(company_name), '[^a-z0-9]+', ' ', 'g') LIKE '%' || $3 || '%'
         OR $3 LIKE '%' || regexp_replace(lower(company_name), '[^a-z0-9]+', ' ', 'g') || '%'
         OR regexp_replace(lower(company_name), '[^a-z0-9]+', ' ', 'g') LIKE '%' || $2 || '%'
         OR $2 LIKE '%' || regexp_replace(lower(company_name), '[^a-z0-9]+', ' ', 'g') || '%'
       )
     ORDER BY id DESC
     LIMIT 10`,
    [company, normalized, aliasNormalized]
  );

  if (!result.rows.length) return null;
  const uniqueCustomers = [...new Set(result.rows.map((r) => r.id).filter(Boolean))];
  if (uniqueCustomers.length !== 1) return null;
  return uniqueCustomers[0];
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
        const previewLines = String(parsedPdf.text || "")
          .split(/\r?\n/)
          .map((line) => String(line || "").trim())
          .filter(Boolean)
          .slice(0, 12);

        if (rows.length === 0) {
          const parseError = `No transaction rows matched parser. text_length=${String(parsedPdf.text || "").length}`;
          await client.query(
            `UPDATE transaction_uploads
             SET parse_status = 'failed',
                 parse_error = $2,
                 rows_inserted = 0,
                 rows_skipped = 0,
                 rows_unmatched = 0
             WHERE id = $1`,
            [uploadId, parseError]
          );

          summary.push({
            upload_id: uploadId,
            original_filename: file.originalname,
            parsed_rows: 0,
            rows_inserted: 0,
            rows_skipped: 0,
            rows_unmatched: 0,
            parse_status: "failed",
            parse_error: parseError,
            preview_lines: previewLines,
          });
          continue;
        }

        let inserted = 0;
        let skipped = 0;
        let unmatched = 0;
        const unmatchedCards = new Set();
        const rowErrors = [];
        const txColumnsResult = await client.query(
          `SELECT column_name
           FROM information_schema.columns
           WHERE table_schema = 'public'
             AND table_name = 'transactions'`
        );
        const txColumns = new Set((txColumnsResult.rows || []).map((r) => String(r.column_name || "").trim()));
        const hasExtendedPdfColumns = [
          "base_rate",
          "fet",
          "pft",
          "computed_rate_per_liter",
          "subtotal",
          "gst",
          "pst",
          "qst",
          "total",
        ].every((column) => txColumns.has(column));

        await client.query("BEGIN");

        for (const row of rows) {
          try {
            const parsedDate = row?.purchase_datetime ? new Date(row.purchase_datetime) : null;
            const purchaseYear =
              parsedDate && !Number.isNaN(parsedDate.getTime())
                ? String(parsedDate.getFullYear())
                : null;
            if (purchaseYear && String(row?.card_number || "").trim() === purchaseYear) {
              skipped += 1;
              continue;
            }

            const raw = row && row.source_raw_json && typeof row.source_raw_json === "object"
              ? row.source_raw_json
              : {};
            const sourceCompanyName = raw.company_name || raw["Company Name"] || null;

            let customerId = await resolveCustomerIdByCard(client, row.card_number);
            if (!customerId && sourceCompanyName) {
              customerId = await resolveCustomerIdByCompanyName(client, sourceCompanyName);
            }
            if (!customerId) {
              unmatched += 1;
              if (row.card_number) unmatchedCards.add(String(row.card_number));
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

            const getNumeric = (v) => {
              const n = parseNumber(v);
              return Number.isFinite(n) ? n : null;
            };
            const baseRate = getNumeric(raw.base_rate) ?? getNumeric(raw.ex_tax);
            const fet = getNumeric(raw.fet) ?? getNumeric(raw.fet_per_liter);
            const pft = getNumeric(raw.pft) ?? getNumeric(raw.pft_per_liter);
            const ratePerLtr = getNumeric(raw.rate_per_ltr) ?? getNumeric(raw.rate_per_liter);
            const subtotal = getNumeric(raw.subtotal);
            const gst = getNumeric(raw.gst_hst) ?? getNumeric(raw.gst);
            const pst = getNumeric(raw.pst);
            const qst = getNumeric(raw.qst);
            const total = Number.isFinite(row.amount) ? row.amount : getNumeric(raw.amount);
            const sourceRawJson = { ...raw };
            const duplicateRawKeys = [
              "fet",
              "pft",
              "pst",
              "qst",
              "city",
              "amount",
              "gst_hst",
              "subtotal",
              "base_rate",
              "company_name",
              "Company Name",
              "rate_per_ltr",
              "rate_per_liter",
            ];
            duplicateRawKeys.forEach((key) => {
              delete sourceRawJson[key];
            });
            if (!hasExtendedPdfColumns) {
              // Keep payload consistent on older schemas where extended columns do not exist.
              sourceRawJson.company_name = raw.company_name ?? raw["Company Name"] ?? null;
              sourceRawJson.city = raw.city ?? null;
              sourceRawJson.rate_per_ltr = ratePerLtr;
              sourceRawJson.base_rate = baseRate;
              sourceRawJson.subtotal = subtotal;
              sourceRawJson.gst_hst = gst;
              sourceRawJson.pst = pst;
              sourceRawJson.qst = qst;
              sourceRawJson.amount = total;
            }

            if (hasExtendedPdfColumns) {
              await client.query(
                `INSERT INTO transactions
                 (customer_id, card_number, purchase_datetime, location, city, province, document_number, product,
                  volume_liters, total_amount, driver_name, source_upload_id, source_type, source_raw_json,
                  base_rate, fet, pft, computed_rate_per_liter, subtotal, gst, pst, qst, total)
                 VALUES
                 ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)`,
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
                  sourceRawJson,
                  baseRate,
                  fet,
                  pft,
                  ratePerLtr,
                  subtotal,
                  gst,
                  pst,
                  qst,
                  total,
                ]
              );
            } else {
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
                  sourceRawJson,
                ]
              );
            }
            inserted += 1;
          } catch (rowErr) {
            skipped += 1;
            if (rowErrors.length < 25) {
              rowErrors.push({
                card_number: row?.card_number || null,
                document_number: row?.document_number || null,
                purchase_datetime: row?.purchase_datetime || null,
                error: String(rowErr?.message || "row_insert_failed"),
              });
            }
          }
        }

        await client.query("COMMIT");

        const parsedStatus = inserted > 0 ? "done" : "failed";
        const parseError =
          inserted > 0
            ? null
            : `No rows inserted. Sample row errors: ${rowErrors
                .map((e) => e.error)
                .filter(Boolean)
                .slice(0, 3)
                .join(" | ") || "Unknown row insert error"}`;

        await client.query(
          `UPDATE transaction_uploads
           SET parse_status = $2,
               parse_error = $3,
               rows_inserted = $4,
               rows_skipped = $5,
               rows_unmatched = $6
           WHERE id = $1`,
          [uploadId, parsedStatus, parseError, inserted, skipped, unmatched]
        );

        summary.push({
          upload_id: uploadId,
          original_filename: file.originalname,
          parsed_rows: rows.length,
          rows_inserted: inserted,
          rows_skipped: skipped,
          rows_unmatched: unmatched,
          unmatched_card_sample: Array.from(unmatchedCards).slice(0, 25),
          parse_status: parsedStatus,
          ...(parseError ? { parse_error: parseError } : {}),
          ...(rowErrors.length ? { row_error_sample: rowErrors } : {}),
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
          parsed_rows: 0,
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

// Admin utility: reset all PDF-imported transaction data so files can be reloaded cleanly.
router.post("/transactions/reset-pdf-imports", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      const deleteTransactions = await client.query(
        `DELETE FROM transactions
         WHERE source_type = 'pdf'
            OR source_upload_id IN (
              SELECT id FROM transaction_uploads WHERE source_type = 'pdf'
            )`
      );

      const deleteUploads = await client.query(
        `DELETE FROM transaction_uploads
         WHERE source_type = 'pdf'`
      );

      await client.query("COMMIT");

      return res.json({
        message: "PDF-imported transactions reset successfully",
        deleted_transactions: deleteTransactions.rowCount || 0,
        deleted_uploads: deleteUploads.rowCount || 0,
      });
    } catch (err) {
      try {
        await client.query("ROLLBACK");
      } catch (_rollbackErr) {}
      throw err;
    } finally {
      client.release();
    }
  } catch (err) {
    console.error("Reset PDF imports error:", err);
    return res.status(500).json({ message: "Server error" });
  }
});

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
        if (normalized === "companyname") headerMap.company_name = key;
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
        if (normalized === "baserate") headerMap.base_rate = key;
        if (normalized === "rateperltr" || normalized === "rateperlitr" || normalized === "rateperliter") headerMap.rate_per_ltr = key;
        if (normalized === "subtotal" || normalized === "subtoatl") headerMap.subtotal = key;
      }

      const hasDateTime = !!headerMap.purchase_datetime;
      const hasDateAndTime = !!headerMap.purchase_date && !!headerMap.purchase_time;
      const isSuperPass = !!headerMap.ex_tax;
      const isInvoiceStyle = !!(
        headerMap.base_rate &&
        headerMap.rate_per_ltr &&
        headerMap.subtotal &&
        headerMap.gst &&
        headerMap.total_amount
      );

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
            const customerIdNum = customerData?.customer_id || null;
            if (!customerIdNum) {
              unmatchedCount += 1;
              unmatchedCards.add(card_number);
              if (unmatchedRows.length < 50) {
                unmatchedRows.push({ row: idx + 2, card_number, reason: "card not mapped to customer" });
              }
            }
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
            const sourceRawJson = {};
            for (const [rawKey, rawValue] of Object.entries(row || {})) {
              sourceRawJson[String(rawKey || "").trim()] = rawValue;
            }

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

            if (isInvoiceStyle) {
              const sheetBaseRate = parseNumber(row[headerMap.base_rate]);
              const sheetRatePerLtr = parseNumber(row[headerMap.rate_per_ltr]);
              const sheetSubtotal = parseNumber(row[headerMap.subtotal]);
              const sheetGst = parseNumber(row[headerMap.gst]);
              const sheetPst = parseNumber(row[headerMap.pst]);
              const sheetQst = parseNumber(row[headerMap.qst]);
              source_ex_tax = Number.isFinite(sheetBaseRate) ? sheetBaseRate : source_ex_tax;
              source_fet = headerMap.fet ? parseNumber(row[headerMap.fet]) : source_fet;
              source_pft = headerMap.pft ? parseNumber(row[headerMap.pft]) : source_pft;
              computed_ex_tax = Number.isFinite(sheetBaseRate) ? round4(sheetBaseRate + markupPerLiter) : computed_ex_tax;
              computed_rate_per_liter = Number.isFinite(sheetRatePerLtr) ? round4(sheetRatePerLtr) : computed_rate_per_liter;
              computed_in_tax = computed_rate_per_liter;

              const subtotalFromSheet = Number.isFinite(sheetSubtotal)
                ? round4(sheetSubtotal)
                : round4((computed_in_tax || 0) * volume);
              const gstFromSheet = Number.isFinite(sheetGst) ? round4(sheetGst) : 0;
              const pstFromSheet = Number.isFinite(sheetPst) ? round4(sheetPst) : 0;
              const qstFromSheet = Number.isFinite(sheetQst) ? round4(sheetQst) : 0;

              const subtotal = subtotalFromSheet;
              const gst = gstFromSheet;
              const pst = pstFromSheet;
              const qst = qstFromSheet;
              const total = Number.isFinite(total_amount)
                ? round4(total_amount)
                : round4(subtotal + gst + pst + qst);

              await client.query(
                `INSERT INTO transactions
                (customer_id, card_number, driver_name, purchase_datetime, product, volume_liters, total_amount, document_number, location, province,
                 computed_rate_per_liter, subtotal, gst, pst, qst, total, computed_ex_tax, computed_in_tax,
                 source_ex_tax, source_in_tax, source_fet, source_pft, source_fct_pct, source_urban, source_gst, source_pst, source_qst, source_amount,
                 source_type, source_raw_json)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
                        $11,$12,$13,$14,$15,$16,$17,$18,
                        $19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30)`,
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
                  "sheet",
                  sourceRawJson,
                ]
              );
              insertedCount += 1;
              if (!minDate || purchaseDateTime < minDate) minDate = purchaseDateTime;
              if (!maxDate || purchaseDateTime > maxDate) maxDate = purchaseDateTime;
              continue;
            }

            if (customerIdNum) {
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
               source_ex_tax, source_in_tax, source_fet, source_pft, source_fct_pct, source_urban, source_gst, source_pst, source_qst, source_amount,
               source_type, source_raw_json)
              VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
                      $11,$12,$13,$14,$15,$16,$17,$18,
                      $19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30)`,
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
                "sheet",
                sourceRawJson,
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

router.get("/transactions/raw", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;

    const {
      date_from,
      date_to,
      customer_id,
      card_number,
      page = "1",
      pageSize = "50",
    } = req.query || {};

    const pageNum = parseInt(page, 10);
    const pageSizeNum = parseInt(pageSize, 10);
    if (!Number.isInteger(pageNum) || pageNum < 1) {
      return res.status(400).json({ message: "Invalid page" });
    }
    if (!Number.isInteger(pageSizeNum) || pageSizeNum < 1 || pageSizeNum > 500) {
      return res.status(400).json({ message: "Invalid pageSize" });
    }

    const values = [];
    const where = [];
    const resolvedCustomerExpr = `COALESCE(t.customer_id, card_map.mapped_customer_id)`;

    if (date_from) {
      values.push(date_from);
      where.push(`t.purchase_datetime >= $${values.length}`);
    }
    if (date_to) {
      values.push(date_to);
      where.push(`t.purchase_datetime < ($${values.length}::date + INTERVAL '1 day')`);
    }
    if (customer_id) {
      const customerIdNum = parseInt(customer_id, 10);
      if (!Number.isInteger(customerIdNum) || customerIdNum < 1) {
        return res.status(400).json({ message: "Invalid customer_id" });
      }
      values.push(customerIdNum);
      where.push(`${resolvedCustomerExpr} = $${values.length}`);
    }
    if (card_number) {
      values.push(String(card_number).trim());
      where.push(`t.card_number = $${values.length}`);
    }

    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

    const totalsResult = await pool.query(
      `WITH deduped AS (
         SELECT DISTINCT ON (
           COALESCE(${resolvedCustomerExpr}, -1),
           COALESCE(t.card_number, ''),
           COALESCE(t.document_number, ''),
           COALESCE(t.purchase_datetime, 'epoch'::timestamp),
           COALESCE(t.volume_liters, 0),
           COALESCE(t.total_amount, 0)
         ) t.id, t.volume_liters,
           COALESCE(
             NULLIF(t.total_amount, 0),
             CASE
               WHEN COALESCE(t.source_raw_json->>'amount', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (t.source_raw_json->>'amount')::numeric
               ELSE 0
             END
           ) AS amount_value
         FROM transactions t
         LEFT JOIN LATERAL (
           SELECT
             cd.customer_id AS mapped_customer_id,
             cd.customer_number,
             cd.company_name
           FROM cards cd
           WHERE cd.card_number = t.card_number
              OR regexp_replace(cd.card_number, '\\D', '', 'g') = regexp_replace(COALESCE(t.card_number, ''), '\\D', '', 'g')
              OR (
                right(regexp_replace(cd.card_number, '\\D', '', 'g'), 4) <> ''
                AND right(regexp_replace(cd.card_number, '\\D', '', 'g'), 4) = right(regexp_replace(COALESCE(t.card_number, ''), '\\D', '', 'g'), 4)
              )
           ORDER BY cd.id DESC
           LIMIT 1
         ) card_map ON TRUE
         ${whereSql}
         ORDER BY
           COALESCE(${resolvedCustomerExpr}, -1),
           COALESCE(t.card_number, ''),
           COALESCE(t.document_number, ''),
           COALESCE(t.purchase_datetime, 'epoch'::timestamp),
           COALESCE(t.volume_liters, 0),
           COALESCE(t.total_amount, 0),
           t.id DESC
       )
       SELECT
         COUNT(*)::int AS count,
         COALESCE(SUM(volume_liters), 0) AS total_litres,
         COALESCE(SUM(amount_value), 0) AS total_amount
       FROM deduped`,
      values
    );

    const offset = (pageNum - 1) * pageSizeNum;
    const dataResult = await pool.query(
      `WITH deduped AS (
         SELECT DISTINCT ON (
           COALESCE(${resolvedCustomerExpr}, -1),
           COALESCE(t.card_number, ''),
           COALESCE(t.document_number, ''),
           COALESCE(t.purchase_datetime, 'epoch'::timestamp),
           COALESCE(t.volume_liters, 0),
           COALESCE(t.total_amount, 0)
         )
           t.*,
           COALESCE(c.customer_number, c_map.customer_number, card_map.customer_number) AS customer_number,
           COALESCE(c.company_name, c_map.company_name, card_map.company_name, t.source_raw_json->>'Company Name', t.source_raw_json->>'company_name') AS company_name
         FROM transactions t
         LEFT JOIN customers c ON c.id = t.customer_id
         LEFT JOIN LATERAL (
           SELECT
             cd.customer_id AS mapped_customer_id,
             cd.customer_number,
             cd.company_name
           FROM cards cd
           WHERE cd.card_number = t.card_number
              OR regexp_replace(cd.card_number, '\\D', '', 'g') = regexp_replace(COALESCE(t.card_number, ''), '\\D', '', 'g')
              OR (
                right(regexp_replace(cd.card_number, '\\D', '', 'g'), 4) <> ''
                AND right(regexp_replace(cd.card_number, '\\D', '', 'g'), 4) = right(regexp_replace(COALESCE(t.card_number, ''), '\\D', '', 'g'), 4)
              )
           ORDER BY cd.id DESC
           LIMIT 1
         ) card_map ON TRUE
         LEFT JOIN customers c_map ON c_map.id = card_map.mapped_customer_id
         ${whereSql}
         ORDER BY
           COALESCE(${resolvedCustomerExpr}, -1),
           COALESCE(t.card_number, ''),
           COALESCE(t.document_number, ''),
           COALESCE(t.purchase_datetime, 'epoch'::timestamp),
           COALESCE(t.volume_liters, 0),
           COALESCE(t.total_amount, 0),
           t.id DESC
       )
       SELECT *
       FROM deduped
       ORDER BY purchase_datetime DESC NULLS LAST, id DESC
       LIMIT $${values.length + 1} OFFSET $${values.length + 2}`,
      values.concat([pageSizeNum, offset])
    );

    const rawColumns = new Set();
    const blockedRawColumns = new Set([
      "raw_line",
      "parser_mode",
      "numeric_tokens",
      "fet",
      "pft",
      "pst",
      "qst",
      "city",
      "amount",
      "gst_hst",
      "subtotal",
      "base_rate",
      "company_name",
      "company name",
      "rate_per_ltr",
      "rate_per_liter",
    ]);
    const rows = dataResult.rows.map((row) => {
      const raw = row?.source_raw_json && typeof row.source_raw_json === "object" ? row.source_raw_json : {};
      Object.entries(raw).forEach(([key, value]) => {
        const keyName = String(key || "").trim();
        if (!keyName || blockedRawColumns.has(keyName.toLowerCase())) return;
        rawColumns.add(keyName);
      });
      return { ...row, source_raw_json: raw };
    });

    return res.json({
      data: rows,
      raw_columns: Array.from(rawColumns),
      totals: totalsResult.rows[0] || { count: 0, total_litres: 0, total_amount: 0 },
      page: pageNum,
      pageSize: pageSizeNum,
    });
  } catch (err) {
    console.error("Admin raw transactions error:", err);
    return res.status(500).json({ message: "Server error" });
  }
});

router.get("/customer-transactions-view", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    await ensureInvoiceBatchPhase2Schema();

    const {
      date_from,
      date_to,
      customer_id,
      page = "1",
      pageSize = "50",
    } = req.query || {};

    const pageNum = parseInt(page, 10);
    const pageSizeNum = parseInt(pageSize, 10);
    if (!Number.isInteger(pageNum) || pageNum < 1) {
      return res.status(400).json({ message: "Invalid page" });
    }
    if (!Number.isInteger(pageSizeNum) || pageSizeNum < 1 || pageSizeNum > 500) {
      return res.status(400).json({ message: "Invalid pageSize" });
    }

    const values = [];
    const where = [];
    const resolvedCustomerExpr = `COALESCE(t.customer_id, cm.customer_id)`;
    if (date_from) {
      values.push(date_from);
      where.push(`t.purchase_datetime >= $${values.length}`);
    }
    if (date_to) {
      values.push(date_to);
      where.push(`t.purchase_datetime < ($${values.length}::date + INTERVAL '1 day')`);
    }
    if (customer_id) {
      const customerIdNum = parseInt(customer_id, 10);
      if (!Number.isInteger(customerIdNum) || customerIdNum < 1) {
        return res.status(400).json({ message: "Invalid customer_id" });
      }
      values.push(customerIdNum);
      where.push(`${resolvedCustomerExpr} = $${values.length}`);
    }

    if (date_from && date_to) {
      const blockedCustomers = await findCustomersMissingRateGroupForRange({
        customerId: customer_id ? parseInt(customer_id, 10) : null,
        dateFrom: date_from,
        dateTo: date_to,
      });
      if (blockedCustomers.length > 0) {
        return res.status(409).json({
          message: "Rate group assignment is required for all customers before processing.",
          blocked_customers: blockedCustomers,
        });
      }
    }

    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
    const txColumnsResult = await pool.query(
      `SELECT column_name
       FROM information_schema.columns
       WHERE table_schema = 'public'
         AND table_name = 'transactions'`
    );
    const txColumns = new Set((txColumnsResult.rows || []).map((r) => String(r.column_name || "").trim()));
    const txCol = (name) => (txColumns.has(name) ? name : "NULL");
    const assignmentsExistsResult = await pool.query(
      `SELECT to_regclass('public.customer_rate_group_assignments') IS NOT NULL AS exists`
    );
    const hasAssignmentsTable = !!assignmentsExistsResult.rows[0]?.exists;
    const rateJoinSql = hasAssignmentsTable
      ? `LEFT JOIN LATERAL (
           SELECT crga.rate_group_id
           FROM customer_rate_group_assignments crga
           WHERE crga.customer_id = ${resolvedCustomerExpr}
             AND crga.start_date <= COALESCE(t.purchase_datetime::date, CURRENT_DATE)
             AND (crga.end_date IS NULL OR crga.end_date >= COALESCE(t.purchase_datetime::date, CURRENT_DATE))
           ORDER BY crga.start_date DESC, crga.id DESC
           LIMIT 1
         ) cra ON TRUE
         LEFT JOIN rate_groups rg ON rg.id = COALESCE(cra.rate_group_id, c.rate_group_id)`
      : `LEFT JOIN rate_groups rg ON rg.id = c.rate_group_id`;

    const totalsResult = await pool.query(
      `WITH deduped AS (
         SELECT DISTINCT ON (
           COALESCE(${resolvedCustomerExpr}, -1),
           COALESCE(t.card_number, ''),
           COALESCE(t.document_number, ''),
           COALESCE(t.purchase_datetime, 'epoch'::timestamp),
           COALESCE(t.volume_liters, 0),
           COALESCE(t.total_amount, 0)
         )
           t.*,
           ${resolvedCustomerExpr} AS resolved_customer_id,
           c.customer_number,
           COALESCE(c.company_name, t.source_raw_json->>'Company Name', t.source_raw_json->>'company_name') AS company_name,
           COALESCE(rg.markup_per_liter, 0) AS effective_markup_per_liter
         FROM transactions t
         LEFT JOIN LATERAL (
           SELECT cd.customer_id
           FROM cards cd
           WHERE cd.customer_id IS NOT NULL
             AND (
               regexp_replace(COALESCE(cd.card_number, ''), '\\D', '', 'g') =
               regexp_replace(COALESCE(t.card_number, ''), '\\D', '', 'g')
               OR (
                 right(regexp_replace(COALESCE(cd.card_number, ''), '\\D', '', 'g'), 4) <>
                 '' AND
                 right(regexp_replace(COALESCE(cd.card_number, ''), '\\D', '', 'g'), 4) =
                 right(regexp_replace(COALESCE(t.card_number, ''), '\\D', '', 'g'), 4)
               )
             )
           ORDER BY cd.id DESC
           LIMIT 1
         ) cm ON TRUE
         LEFT JOIN customers c ON c.id = ${resolvedCustomerExpr}
         ${rateJoinSql}
         ${whereSql}
         ORDER BY
           COALESCE(${resolvedCustomerExpr}, -1),
           COALESCE(t.card_number, ''),
           COALESCE(t.document_number, ''),
           COALESCE(t.purchase_datetime, 'epoch'::timestamp),
           COALESCE(t.volume_liters, 0),
           COALESCE(t.total_amount, 0),
           t.id DESC
       )
       SELECT
         COUNT(*)::int AS count,
         COALESCE(SUM(volume_liters), 0) AS total_litres,
         COALESCE(SUM(
           COALESCE(
             ${txCol("total")},
             CASE
               WHEN COALESCE(source_raw_json->>'amount', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                 THEN (source_raw_json->>'amount')::numeric
               ELSE NULL
             END,
             total_amount,
             COALESCE(
               ${txCol("subtotal")},
               CASE
                 WHEN COALESCE(source_raw_json->>'subtotal', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                   THEN (source_raw_json->>'subtotal')::numeric
                 ELSE NULL
               END,
               COALESCE(${txCol("total")}, total_amount, 0) - COALESCE(${txCol("gst")}, 0) - COALESCE(${txCol("pst")}, 0) - COALESCE(${txCol("qst")}, 0)
             ) + COALESCE(${txCol("gst")}, 0) + COALESCE(${txCol("pst")}, 0) + COALESCE(${txCol("qst")}, 0)
           )
         ), 0) AS total_amount
       FROM deduped`,
      values
    );

    const offset = (pageNum - 1) * pageSizeNum;
    const dataResult = await pool.query(
      `WITH deduped AS (
         SELECT DISTINCT ON (
           COALESCE(${resolvedCustomerExpr}, -1),
           COALESCE(t.card_number, ''),
           COALESCE(t.document_number, ''),
           COALESCE(t.purchase_datetime, 'epoch'::timestamp),
           COALESCE(t.volume_liters, 0),
           COALESCE(t.total_amount, 0)
         )
           t.*,
           ${resolvedCustomerExpr} AS resolved_customer_id,
           c.customer_number,
           COALESCE(c.company_name, t.source_raw_json->>'Company Name', t.source_raw_json->>'company_name') AS company_name,
           COALESCE(rg.markup_per_liter, 0) AS effective_markup_per_liter
         FROM transactions t
         LEFT JOIN LATERAL (
           SELECT cd.customer_id
           FROM cards cd
           WHERE cd.customer_id IS NOT NULL
             AND (
               regexp_replace(COALESCE(cd.card_number, ''), '\\D', '', 'g') =
               regexp_replace(COALESCE(t.card_number, ''), '\\D', '', 'g')
               OR (
                 right(regexp_replace(COALESCE(cd.card_number, ''), '\\D', '', 'g'), 4) <>
                 '' AND
                 right(regexp_replace(COALESCE(cd.card_number, ''), '\\D', '', 'g'), 4) =
                 right(regexp_replace(COALESCE(t.card_number, ''), '\\D', '', 'g'), 4)
               )
             )
           ORDER BY cd.id DESC
           LIMIT 1
         ) cm ON TRUE
         LEFT JOIN customers c ON c.id = ${resolvedCustomerExpr}
         ${rateJoinSql}
         ${whereSql}
         ORDER BY
           COALESCE(${resolvedCustomerExpr}, -1),
           COALESCE(t.card_number, ''),
           COALESCE(t.document_number, ''),
           COALESCE(t.purchase_datetime, 'epoch'::timestamp),
           COALESCE(t.volume_liters, 0),
           COALESCE(t.total_amount, 0),
           t.id DESC
       )
       SELECT
         id,
         resolved_customer_id AS customer_id,
         customer_number,
         company_name,
         purchase_datetime,
         card_number,
         document_number,
         location,
         COALESCE(city, source_raw_json->>'city') AS city,
         COALESCE(province, '') AS province,
         product,
         volume_liters,
         COALESCE(
           ${txCol("base_rate")},
           NULLIF(source_raw_json->>'base_rate', '')::numeric,
           NULLIF(source_raw_json->>'ex_tax', '')::numeric
         )::numeric AS base_rate,
         COALESCE(${txCol("fet")}, NULLIF(source_raw_json->>'fet', '')::numeric, NULLIF(source_raw_json->>'fet_per_liter', '')::numeric)::numeric AS fet,
         COALESCE(${txCol("pft")}, NULLIF(source_raw_json->>'pft', '')::numeric, NULLIF(source_raw_json->>'pft_per_liter', '')::numeric)::numeric AS pft,
        COALESCE(
          ${txCol("computed_rate_per_liter")},
          CASE
            WHEN COALESCE(source_raw_json->>'rate_per_ltr', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
              THEN (source_raw_json->>'rate_per_ltr')::numeric
            WHEN COALESCE(source_raw_json->>'rate_per_liter', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
              THEN (source_raw_json->>'rate_per_liter')::numeric
            ELSE NULL
          END,
          CASE
            WHEN COALESCE(volume_liters, 0) > 0 THEN ROUND(
              (COALESCE(${txCol("subtotal")}, ${txCol("total")}, total_amount, 0) / volume_liters) + COALESCE(effective_markup_per_liter, 0),
               4
             )
             ELSE NULL
           END
         ) AS computed_rate_per_liter,
         COALESCE(
           ${txCol("subtotal")},
           CASE
             WHEN COALESCE(source_raw_json->>'subtotal', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (source_raw_json->>'subtotal')::numeric
             ELSE NULL
           END,
           COALESCE(${txCol("total")}, total_amount, 0) - COALESCE(${txCol("gst")}, 0) - COALESCE(${txCol("pst")}, 0) - COALESCE(${txCol("qst")}, 0)
         ) AS subtotal,
         COALESCE(
           ${txCol("gst")},
           CASE
             WHEN COALESCE(source_raw_json->>'gst_hst', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (source_raw_json->>'gst_hst')::numeric
             WHEN COALESCE(source_raw_json->>'gst', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (source_raw_json->>'gst')::numeric
             ELSE NULL
           END,
           0
         ) AS gst,
         COALESCE(
           ${txCol("pst")},
           CASE
             WHEN COALESCE(source_raw_json->>'pst', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (source_raw_json->>'pst')::numeric
             ELSE NULL
           END,
           0
         ) AS pst,
         COALESCE(
           ${txCol("qst")},
           CASE
             WHEN COALESCE(source_raw_json->>'qst', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (source_raw_json->>'qst')::numeric
             ELSE NULL
           END,
           0
         ) AS qst,
         COALESCE(driver_name, source_raw_json->>'driver_name') AS driver_name,
         COALESCE(
           ${txCol("total")},
           CASE
             WHEN COALESCE(source_raw_json->>'amount', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (source_raw_json->>'amount')::numeric
             ELSE NULL
           END,
           total_amount,
           COALESCE(
             ${txCol("subtotal")},
             COALESCE(${txCol("total")}, total_amount, 0) - COALESCE(${txCol("gst")}, 0) - COALESCE(${txCol("pst")}, 0) - COALESCE(${txCol("qst")}, 0)
           ) + COALESCE(${txCol("gst")}, 0) + COALESCE(${txCol("pst")}, 0) + COALESCE(${txCol("qst")}, 0)
         ) AS total
       FROM deduped
       ORDER BY purchase_datetime DESC NULLS LAST, id DESC
       LIMIT $${values.length + 1} OFFSET $${values.length + 2}`,
      values.concat([pageSizeNum, offset])
    );

    return res.json({
      data: dataResult.rows,
      totals: totalsResult.rows[0] || { count: 0, total_litres: 0, total_amount: 0 },
      page: pageNum,
      pageSize: pageSizeNum,
    });
  } catch (err) {
    const errorId = `customer-tx-view-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
    console.error(`[${errorId}] Admin customer-transactions-view error:`, err);
    return res.status(500).json({ message: "Server error", errorId });
  }
});

router.post("/customer-transactions-view/invoice", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    await ensureGeneratedInvoicesSchemaCompat();

    const customerIdNum = parseInt(req.body?.customer_id, 10);
    if (!Number.isInteger(customerIdNum) || customerIdNum < 1) {
      return res.status(400).json({ message: "Invalid customer_id" });
    }

    const dateFrom = parseDateOnly(req.body?.date_from);
    const dateTo = parseDateOnly(req.body?.date_to);
    if (!dateFrom || !dateTo) {
      return res.status(400).json({ message: "date_from and date_to are required" });
    }

    const blockedCustomers = await findCustomersMissingRateGroupForRange({
      customerId: customerIdNum,
      dateFrom,
      dateTo,
    });
    if (blockedCustomers.length > 0) {
      return res.status(409).json({
        message: "Cannot generate invoice until rate group is assigned to customer.",
        blocked_customers: blockedCustomers,
      });
    }

    const assignmentsExistsResult = await pool.query(
      `SELECT to_regclass('public.customer_rate_group_assignments') IS NOT NULL AS exists`
    );
    const hasAssignmentsTable = !!assignmentsExistsResult.rows[0]?.exists;
    const rateJoinSql = hasAssignmentsTable
      ? `LEFT JOIN LATERAL (
           SELECT crga.rate_group_id
           FROM customer_rate_group_assignments crga
           WHERE crga.customer_id = t.customer_id
             AND crga.start_date <= COALESCE(t.purchase_datetime::date, CURRENT_DATE)
             AND (crga.end_date IS NULL OR crga.end_date >= COALESCE(t.purchase_datetime::date, CURRENT_DATE))
           ORDER BY crga.start_date DESC, crga.id DESC
           LIMIT 1
         ) cra ON TRUE
         LEFT JOIN rate_groups rg ON rg.id = COALESCE(cra.rate_group_id, c.rate_group_id)`
      : `LEFT JOIN rate_groups rg ON rg.id = c.rate_group_id`;

    const summaryResult = await pool.query(
      `WITH deduped AS (
         SELECT DISTINCT ON (
           COALESCE(t.customer_id, -1),
           COALESCE(t.card_number, ''),
           COALESCE(t.document_number, ''),
           COALESCE(t.purchase_datetime, 'epoch'::timestamp),
           COALESCE(t.volume_liters, 0),
           COALESCE(t.total_amount, 0)
         )
           t.*,
           COALESCE(rg.markup_per_liter, 0) AS effective_markup_per_liter
         FROM transactions t
         JOIN customers c ON c.id = t.customer_id
         ${rateJoinSql}
         WHERE t.customer_id = $1
           AND t.purchase_datetime >= $2
           AND t.purchase_datetime <= $3
         ORDER BY
           COALESCE(t.customer_id, -1),
           COALESCE(t.card_number, ''),
           COALESCE(t.document_number, ''),
           COALESCE(t.purchase_datetime, 'epoch'::timestamp),
           COALESCE(t.volume_liters, 0),
           COALESCE(t.total_amount, 0),
           t.id DESC
       )
       SELECT
         COUNT(*)::int AS row_count,
         COALESCE(SUM(volume_liters), 0) AS total_litres,
         COALESCE(SUM(
           COALESCE(
             total,
             total_amount,
             COALESCE(
               subtotal,
               COALESCE(total, total_amount, 0) - COALESCE(gst, 0) - COALESCE(pst, 0) - COALESCE(qst, 0)
             ) + COALESCE(gst, 0) + COALESCE(pst, 0) + COALESCE(qst, 0)
           )
         ), 0) AS invoice_total
       FROM deduped`,
      [customerIdNum, dateFrom, dateTo]
    );

    const stats = summaryResult.rows[0] || { row_count: 0, total_litres: 0, invoice_total: 0 };
    if (Number(stats.row_count) === 0) {
      return res.status(400).json({ message: "No transactions found in selected date range for this customer" });
    }

    const generatedNo = `AUTO-${new Date().toISOString().slice(0, 10).replace(/-/g, "")}-${Math.floor(Math.random() * 1e6)
      .toString()
      .padStart(6, "0")}`;
    const invoiceNo = String(req.body?.invoice_no || "").trim() || generatedNo;
    const invoiceDate = parseDateOnly(req.body?.invoice_date) || new Date().toISOString().slice(0, 10);
    const dueDate = parseDateOnly(req.body?.due_date);

    const insertResult = await pool.query(
      `INSERT INTO customer_invoices
         (customer_id, invoice_no, invoice_date, period_start, period_end, total, totals_provided, status)
       VALUES
         ($1, $2, $3, $4, $5, $6, true, 'issued')
       ON CONFLICT (customer_id, invoice_no)
       DO UPDATE SET
         invoice_date = EXCLUDED.invoice_date,
         period_start = EXCLUDED.period_start,
         period_end = EXCLUDED.period_end,
         total = EXCLUDED.total,
         totals_provided = true,
         status = 'issued'
       RETURNING id, customer_id, invoice_no, invoice_date, period_start, period_end, total, status`,
      [customerIdNum, invoiceNo, invoiceDate, dateFrom, dateTo, stats.invoice_total]
    );

    const invoiceRow = insertResult.rows[0];
    if (dueDate) {
      try {
        await pool.query(`ALTER TABLE customer_invoices ADD COLUMN IF NOT EXISTS due_date date`);
        await pool.query(`UPDATE customer_invoices SET due_date = $1 WHERE id = $2`, [dueDate, invoiceRow.id]);
      } catch (_err) {}
    }

    return res.json({
      message: "Invoice created from customer transaction view",
      invoice: invoiceRow,
      stats: {
        row_count: Number(stats.row_count) || 0,
        total_litres: Number(stats.total_litres) || 0,
        invoice_total: Number(stats.invoice_total) || 0,
      },
    });
  } catch (err) {
    console.error("Create invoice from customer transaction view error:", err);
    return res.status(500).json({ message: "Server error" });
  }
});

router.get("/transactions/uninvoiced", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    await ensureInvoiceBatchPhase1Schema();

    const { date_from, date_to, customer_id, page = "1", pageSize = "100" } = req.query || {};
    const pageNum = parseInt(page, 10);
    const pageSizeNum = parseInt(pageSize, 10);
    if (!Number.isInteger(pageNum) || pageNum < 1) {
      return res.status(400).json({ message: "Invalid page" });
    }
    if (!Number.isInteger(pageSizeNum) || pageSizeNum < 1 || pageSizeNum > 500) {
      return res.status(400).json({ message: "Invalid pageSize" });
    }

    const values = [];
    const where = ["COALESCE(t.is_invoiced, false) = false"];

    if (date_from) {
      values.push(date_from);
      where.push(`t.purchase_datetime >= $${values.length}`);
    }
    if (date_to) {
      values.push(date_to);
      where.push(`t.purchase_datetime < ($${values.length}::date + INTERVAL '1 day')`);
    }
    if (customer_id) {
      const customerIdNum = parseInt(customer_id, 10);
      if (!Number.isInteger(customerIdNum) || customerIdNum < 1) {
        return res.status(400).json({ message: "Invalid customer_id" });
      }
      values.push(customerIdNum);
      where.push(`t.customer_id = $${values.length}`);
    }

    const whereSql = `WHERE ${where.join(" AND ")}`;
    const offset = (pageNum - 1) * pageSizeNum;

    const totalsResult = await pool.query(
      `SELECT
         COUNT(*)::int AS count,
         COALESCE(SUM(t.volume_liters), 0) AS total_litres,
         COALESCE(SUM(COALESCE(t.total, t.total_amount, 0)), 0) AS total_amount
       FROM transactions t
       ${whereSql}`,
      values
    );

    const rowsResult = await pool.query(
      `SELECT
         t.id,
         t.customer_id,
         c.customer_number,
         c.company_name,
         t.card_number,
         t.purchase_datetime,
         t.location,
         t.province,
         t.product,
         t.volume_liters,
         COALESCE(t.total, t.total_amount, 0) AS total_amount,
         t.document_number,
         COALESCE(t.is_invoiced, false) AS is_invoiced,
         t.invoice_batch_id,
         t.invoice_id
       FROM transactions t
       LEFT JOIN customers c ON c.id = t.customer_id
       ${whereSql}
       ORDER BY t.purchase_datetime DESC NULLS LAST, t.id DESC
       LIMIT $${values.length + 1}
       OFFSET $${values.length + 2}`,
      values.concat([pageSizeNum, offset])
    );

    return res.json({
      data: rowsResult.rows,
      totals: totalsResult.rows[0] || { count: 0, total_litres: 0, total_amount: 0 },
      page: pageNum,
      pageSize: pageSizeNum,
    });
  } catch (err) {
    console.error("Admin uninvoiced transactions error:", err);
    return res.status(500).json({ message: "Server error" });
  }
});

router.post("/invoice-batches", authMiddleware, async (req, res) => {
  const client = await pool.connect();
  try {
    if (!requireAdmin(req, res)) return;
    await ensureInvoiceBatchPhase1Schema();

    const inputIds = Array.isArray(req.body?.transaction_ids) ? req.body.transaction_ids : [];
    const txIds = [...new Set(inputIds.map((id) => parseInt(id, 10)).filter((id) => Number.isInteger(id) && id > 0))];
    if (txIds.length === 0) {
      return res.status(400).json({ message: "transaction_ids is required" });
    }

    await client.query("BEGIN");
    const txResult = await client.query(
      `SELECT id, customer_id, purchase_datetime, COALESCE(is_invoiced, false) AS is_invoiced, invoice_batch_id
       FROM transactions
       WHERE id = ANY($1::bigint[])
       ORDER BY id ASC`,
      [txIds]
    );

    if (txResult.rows.length !== txIds.length) {
      await client.query("ROLLBACK");
      return res.status(400).json({ message: "One or more transaction_ids do not exist" });
    }

    const alreadyInvoiced = txResult.rows.filter((row) => row.is_invoiced);
    if (alreadyInvoiced.length > 0) {
      await client.query("ROLLBACK");
      return res.status(409).json({
        message: "Some transactions are already invoiced",
        transaction_ids: alreadyInvoiced.map((row) => row.id),
      });
    }

    const alreadyBatched = txResult.rows.filter((row) => row.invoice_batch_id);
    if (alreadyBatched.length > 0) {
      await client.query("ROLLBACK");
      return res.status(409).json({
        message: "Some transactions already belong to another batch",
        transaction_ids: alreadyBatched.map((row) => row.id),
      });
    }

    const minDate = txResult.rows.reduce((acc, row) => {
      const d = row.purchase_datetime ? new Date(row.purchase_datetime) : null;
      return !d || Number.isNaN(d.getTime()) ? acc : (!acc || d < acc ? d : acc);
    }, null);
    const maxDate = txResult.rows.reduce((acc, row) => {
      const d = row.purchase_datetime ? new Date(row.purchase_datetime) : null;
      return !d || Number.isNaN(d.getTime()) ? acc : (!acc || d > acc ? d : acc);
    }, null);
    const dateFrom = minDate ? minDate.toISOString().slice(0, 10) : null;
    const dateTo = maxDate ? maxDate.toISOString().slice(0, 10) : null;

    const customerIds = [...new Set(txResult.rows.map((row) => row.customer_id).filter((v) => Number.isInteger(v)))];
    if (customerIds.length === 0) {
      await client.query("ROLLBACK");
      return res.status(400).json({ message: "Selected transactions must have customer_id" });
    }

    const blocked = await findCustomersMissingRateGroupForRange({
      customerId: null,
      dateFrom: dateFrom || new Date().toISOString().slice(0, 10),
      dateTo: dateTo || new Date().toISOString().slice(0, 10),
    });
    const blockedInSelection = blocked.filter((row) => customerIds.includes(row.customer_id));
    if (blockedInSelection.length > 0) {
      await client.query("ROLLBACK");
      return res.status(409).json({
        message: "Rate group assignment is required for all customers in selected transactions.",
        blocked_customers: blockedInSelection,
      });
    }

    const batchCode = makeBatchCode();
    const batchInsert = await client.query(
      `INSERT INTO invoice_batches (batch_code, status, date_from, date_to, created_by_user_id)
       VALUES ($1, 'PROCESSING', $2, $3, $4)
       RETURNING id, batch_code, status, date_from, date_to, created_at`,
      [batchCode, dateFrom, dateTo, req.user.id || null]
    );
    const batch = batchInsert.rows[0];

    await client.query(
      `INSERT INTO invoice_batch_transactions (invoice_batch_id, transaction_id, customer_id, line_status)
       SELECT $1, t.id, t.customer_id, 'READY'
       FROM transactions t
       WHERE t.id = ANY($2::bigint[])`,
      [batch.id, txIds]
    );

    await client.query(
      `UPDATE transactions
       SET invoice_batch_id = $1
       WHERE id = ANY($2::bigint[])`,
      [batch.id, txIds]
    );

    await client.query("COMMIT");
    return res.json({
      message: "Invoice batch created",
      batch,
      transaction_count: txIds.length,
    });
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch (_rollbackErr) {}
    console.error("Create invoice batch error:", err);
    return res.status(500).json({ message: "Server error" });
  } finally {
    client.release();
  }
});

router.get("/invoice-batches", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    await ensureInvoiceBatchPhase2Schema();

    const result = await pool.query(
      `SELECT
         b.id,
         b.batch_code,
         b.status,
         b.date_from,
         b.date_to,
         b.created_at,
         COUNT(ibt.id)::int AS transaction_count,
         COUNT(DISTINCT ibt.customer_id)::int AS customer_count
       FROM invoice_batches b
       LEFT JOIN invoice_batch_transactions ibt ON ibt.invoice_batch_id = b.id
       GROUP BY b.id
       ORDER BY b.created_at DESC, b.id DESC`
    );
    return res.json(result.rows);
  } catch (err) {
    console.error("List invoice batches error:", err);
    return res.status(500).json({ message: "Server error" });
  }
});

router.get("/invoice-batches/:id", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    await ensureInvoiceBatchPhase2Schema();

    const batchId = parseInt(req.params.id, 10);
    if (!Number.isInteger(batchId) || batchId < 1) {
      return res.status(400).json({ message: "Invalid batch id" });
    }

    const batchResult = await pool.query(
      `SELECT id, batch_code, status, date_from, date_to, created_at
       FROM invoice_batches
       WHERE id = $1
       LIMIT 1`,
      [batchId]
    );
    if (batchResult.rows.length === 0) {
      return res.status(404).json({ message: "Batch not found" });
    }

    const [ibtColsResult, txColsResult] = await Promise.all([
      pool.query(
        `SELECT column_name
         FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name = 'invoice_batch_transactions'`
      ),
      pool.query(
        `SELECT column_name
         FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name = 'transactions'`
      ),
    ]);
    const ibtCols = new Set((ibtColsResult.rows || []).map((r) => String(r.column_name || "").trim()));
    const txCols = new Set((txColsResult.rows || []).map((r) => String(r.column_name || "").trim()));
    const ibtCol = (name) => (ibtCols.has(name) ? `ibt.${name}` : "NULL");
    const txCol = (name) => (txCols.has(name) ? `t.${name}` : "NULL");

    const rowsResult = await pool.query(
      `SELECT
         ibt.id AS batch_line_id,
         ibt.line_status,
         ibt.issue_note,
         ${ibtCol("rate_group_id")} AS rate_group_id,
         ${ibtCol("rate_group_name")} AS rate_group_name,
         ${ibtCol("rate_source_effective_date")} AS rate_source_effective_date,
         ${ibtCol("base_rate")} AS base_rate,
         ${ibtCol("markup_rule_id")} AS markup_rule_id,
         ${ibtCol("markup_rule_used")} AS markup_rule_used,
         ${ibtCol("markup_type")} AS markup_type,
         ${ibtCol("markup_value")} AS markup_value,
         ${ibtCol("rate_per_ltr")} AS rate_per_ltr,
         ${ibtCol("subtotal")} AS subtotal,
         ${ibtCol("gst")} AS gst,
         ${ibtCol("pst")} AS pst,
         ${ibtCol("qst")} AS qst,
         ${ibtCol("amount_total")} AS amount_total,
         COALESCE(${ibtCol("markup_checked")}, false) AS markup_checked,
         COALESCE(${ibtCol("flags")}, ARRAY[]::TEXT[]) AS flags,
         t.id AS transaction_id,
         t.customer_id,
         c.customer_number,
         c.company_name,
         t.card_number,
         t.purchase_datetime,
         t.location,
         t.city,
         t.province,
         t.product,
         t.volume_liters,
         ${txCol("fet")} AS fet,
         ${txCol("pft")} AS pft,
         t.driver_name,
         COALESCE(${txCol("total")}, t.total_amount, 0) AS total_amount,
         t.document_number,
         COALESCE(${txCol("is_invoiced")}, false) AS is_invoiced
       FROM invoice_batch_transactions ibt
       JOIN transactions t ON t.id = ibt.transaction_id
       LEFT JOIN customers c ON c.id = t.customer_id
       WHERE ibt.invoice_batch_id = $1
       ORDER BY t.purchase_datetime DESC NULLS LAST, t.id DESC`,
      [batchId]
    );

    const totals = rowsResult.rows.reduce(
      (acc, row) => {
        acc.count += 1;
        acc.total_litres += Number(row.volume_liters) || 0;
        acc.total_amount += Number(row.total_amount) || 0;
        return acc;
      },
      { count: 0, total_litres: 0, total_amount: 0 }
    );

    return res.json({
      batch: batchResult.rows[0],
      rows: rowsResult.rows,
      totals,
    });
  } catch (err) {
    console.error("Get invoice batch detail error:", err);
    return res.status(500).json({ message: "Server error" });
  }
});

router.post("/invoice-batches/:id/recalculate", authMiddleware, async (req, res) => {
  const client = await pool.connect();
  try {
    if (!requireAdmin(req, res)) return;
    await ensureInvoiceBatchPhase2Schema();

    const batchId = parseInt(req.params.id, 10);
    if (!Number.isInteger(batchId) || batchId < 1) {
      return res.status(400).json({ message: "Invalid batch id" });
    }

    const batchResult = await client.query(
      `SELECT id, status FROM invoice_batches WHERE id = $1 LIMIT 1`,
      [batchId]
    );
    if (batchResult.rows.length === 0) {
      return res.status(404).json({ message: "Batch not found" });
    }

    const rowsResult = await client.query(
      `SELECT
         ibt.id AS batch_line_id,
         t.id AS transaction_id,
         t.customer_id,
         t.purchase_datetime,
         t.location,
         t.province,
         t.product,
         t.volume_liters
       FROM invoice_batch_transactions ibt
       JOIN transactions t ON t.id = ibt.transaction_id
       WHERE ibt.invoice_batch_id = $1
       ORDER BY t.purchase_datetime ASC NULLS LAST, t.id ASC`,
      [batchId]
    );

    if (rowsResult.rows.length === 0) {
      return res.status(400).json({ message: "Batch has no transactions" });
    }

    await client.query("BEGIN");
    let readyCount = 0;
    let rateMissingCount = 0;
    let markupMissingCount = 0;
    let errorCount = 0;

    for (const row of rowsResult.rows) {
      const flags = [];
      let lineStatus = "READY";
      let issueNote = null;

      const txDate = row.purchase_datetime
        ? new Date(row.purchase_datetime).toISOString().slice(0, 10)
        : null;
      const volume = Number(row.volume_liters) || 0;

      let rateGroup = null;
      let baseRate = null;
      let markup = null;
      let finalRate = null;
      let subtotal = null;
      let gst = null;
      let pst = null;
      let qst = null;
      let total = null;
      let effectiveDate = null;

      try {
        if (!row.customer_id || !txDate) {
          flags.push("ERROR");
        } else {
          rateGroup = await getEffectiveRateGroupForTx(client, row.customer_id, txDate);
          if (!rateGroup?.rate_group_id || rateGroup?.is_ready === false) {
            flags.push("RATE_MISSING");
          }

          baseRate = await getBaseRateForTx(client, {
            txDate,
            location: row.location,
            province: row.province,
          });
          if (!baseRate?.base_price) {
            flags.push("RATE_MISSING");
          } else {
            effectiveDate = baseRate.effective_date || null;
          }

          markup = await findBestMarkupRule(client, {
            customerId: row.customer_id,
            product: row.product,
            province: row.province,
            location: row.location,
            txDate,
            fallbackMarkup: rateGroup?.markup_per_liter,
          });
          if (!markup) {
            flags.push("MARKUP_MISSING");
          }

          if (flags.length === 0) {
            const base = Number(baseRate.base_price) || 0;
            if (markup.markup_type === "percent") {
              finalRate = round4(base * (1 + (Number(markup.markup_value) || 0) / 100));
            } else {
              finalRate = round4(base + (Number(markup.markup_value) || 0));
            }

            subtotal = round4(volume * (finalRate || 0));
            const taxes = await getTaxRatesForTx(client, row.province, row.purchase_datetime || txDate);
            gst = round4((subtotal || 0) * (Number(taxes.gst_rate) || 0));
            pst = round4((subtotal || 0) * (Number(taxes.pst_rate) || 0));
            qst = round4((subtotal || 0) * (Number(taxes.qst_rate) || 0));
            total = round4((subtotal || 0) + (gst || 0) + (pst || 0) + (qst || 0));
          }
        }
      } catch (err) {
        flags.push("ERROR");
        issueNote = err.message || "Calculation error";
      }

      if (flags.includes("ERROR")) {
        lineStatus = "ERROR";
        errorCount += 1;
      } else if (flags.includes("RATE_MISSING")) {
        lineStatus = "RATE_MISSING";
        rateMissingCount += 1;
      } else if (flags.includes("MARKUP_MISSING")) {
        lineStatus = "MARKUP_MISSING";
        markupMissingCount += 1;
      } else {
        readyCount += 1;
      }

      await client.query(
        `UPDATE invoice_batch_transactions
         SET
           line_status = $2,
           issue_note = $3,
           rate_group_id = $4,
           rate_group_name = $5,
           rate_source_effective_date = $6,
           base_rate = $7,
           markup_rule_id = $8,
           markup_rule_used = $9,
           markup_type = $10,
           markup_value = $11,
           rate_per_ltr = $12,
           subtotal = $13,
           gst = $14,
           pst = $15,
           qst = $16,
           amount_total = $17,
           flags = $18,
           markup_checked = false
         WHERE id = $1`,
        [
          row.batch_line_id,
          lineStatus,
          issueNote,
          rateGroup?.rate_group_id || null,
          rateGroup?.rate_group_name || null,
          effectiveDate,
          baseRate?.base_price || null,
          markup?.markup_rule_id || null,
          markup?.markup_rule_used || null,
          markup?.markup_type || null,
          markup?.markup_value ?? null,
          finalRate,
          subtotal,
          gst,
          pst,
          qst,
          total,
          flags,
        ]
      );
    }

    await client.query(
      `UPDATE invoice_batches
       SET status = 'PROCESSING', updated_at = now()
       WHERE id = $1`,
      [batchId]
    );

    await client.query("COMMIT");
    return res.json({
      message: "Batch recalculated",
      batch_id: batchId,
      summary: {
        total_rows: rowsResult.rows.length,
        ready: readyCount,
        rate_missing: rateMissingCount,
        markup_missing: markupMissingCount,
        errors: errorCount,
      },
    });
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch (_rollbackErr) {}
    console.error("Recalculate batch error:", err);
    return res.status(500).json({ message: "Server error" });
  } finally {
    client.release();
  }
});

router.post("/invoice-batches/:id/mark-reviewed", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    await ensureInvoiceBatchPhase2Schema();

    const batchId = parseInt(req.params.id, 10);
    if (!Number.isInteger(batchId) || batchId < 1) {
      return res.status(400).json({ message: "Invalid batch id" });
    }

    const invalidRows = await pool.query(
      `SELECT COUNT(*)::int AS count
       FROM invoice_batch_transactions
       WHERE invoice_batch_id = $1
         AND line_status IN ('RATE_MISSING', 'MARKUP_MISSING', 'ERROR')`,
      [batchId]
    );
    if ((invalidRows.rows[0]?.count || 0) > 0) {
      return res.status(409).json({
        message: "Cannot mark reviewed until all rows are READY (no RATE_MISSING/MARKUP_MISSING/ERROR).",
      });
    }

    const uncheckedRows = await pool.query(
      `SELECT COUNT(*)::int AS count
       FROM invoice_batch_transactions
       WHERE invoice_batch_id = $1
         AND line_status = 'READY'
         AND COALESCE(markup_checked, false) = false`,
      [batchId]
    );
    if ((uncheckedRows.rows[0]?.count || 0) > 0) {
      return res.status(409).json({
        message: "Cannot mark reviewed until markup checkbox is verified for all READY rows.",
      });
    }

    const updateResult = await pool.query(
      `UPDATE invoice_batches
       SET status = 'REVIEWED',
           reviewed_at = now(),
           reviewed_by_user_id = $2,
           updated_at = now()
       WHERE id = $1
       RETURNING id, batch_code, status, reviewed_at, reviewed_by_user_id`,
      [batchId, req.user.id || null]
    );
    if (updateResult.rows.length === 0) {
      return res.status(404).json({ message: "Batch not found" });
    }

    const summary = await pool.query(
      `SELECT
         COUNT(*)::int AS row_count,
         COUNT(DISTINCT customer_id)::int AS customer_count
       FROM invoice_batch_transactions
       WHERE invoice_batch_id = $1`,
      [batchId]
    );

    return res.json({
      message: "Batch marked as reviewed",
      batch: updateResult.rows[0],
      prompt: {
        action: "generate_invoices",
        message: "Batch reviewed. Generate invoices now?",
      },
      summary: summary.rows[0] || { row_count: 0, customer_count: 0 },
    });
  } catch (err) {
    console.error("Mark reviewed error:", err);
    return res.status(500).json({ message: "Server error" });
  }
});

router.patch("/invoice-batches/:batchId/rows/:batchLineId", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    await ensureInvoiceBatchPhase2Schema();

    const batchId = parseInt(req.params.batchId, 10);
    const batchLineId = parseInt(req.params.batchLineId, 10);
    if (!Number.isInteger(batchId) || batchId < 1 || !Number.isInteger(batchLineId) || batchLineId < 1) {
      return res.status(400).json({ message: "Invalid batch id or row id" });
    }

    const batchResult = await pool.query(
      `SELECT id, status FROM invoice_batches WHERE id = $1 LIMIT 1`,
      [batchId]
    );
    if (batchResult.rows.length === 0) {
      return res.status(404).json({ message: "Batch not found" });
    }
    if (String(batchResult.rows[0].status || "").toUpperCase() === "INVOICED") {
      return res.status(409).json({ message: "Cannot edit rows after batch is invoiced" });
    }

    const getNumberOrNull = (value) => {
      if (value === undefined || value === null || String(value).trim() === "") return null;
      const parsed = Number(value);
      return Number.isFinite(parsed) ? parsed : null;
    };

    const ratePerLtr = getNumberOrNull(req.body?.rate_per_ltr);
    const subtotal = getNumberOrNull(req.body?.subtotal);
    const gst = getNumberOrNull(req.body?.gst);
    const pst = getNumberOrNull(req.body?.pst);
    const qst = getNumberOrNull(req.body?.qst);
    const amountTotal = getNumberOrNull(req.body?.amount_total);
    const markupChecked = req.body?.markup_checked === true;
    const issueNote = req.body?.issue_note == null ? null : String(req.body.issue_note).trim();

    const update = await pool.query(
      `UPDATE invoice_batch_transactions
       SET
         rate_per_ltr = COALESCE($3, rate_per_ltr),
         subtotal = COALESCE($4, subtotal),
         gst = COALESCE($5, gst),
         pst = COALESCE($6, pst),
         qst = COALESCE($7, qst),
         amount_total = COALESCE($8, amount_total),
         markup_checked = $9,
         issue_note = COALESCE($10, issue_note),
         line_status = CASE
           WHEN line_status IN ('RATE_MISSING', 'MARKUP_MISSING', 'ERROR') AND $9 = true THEN 'READY'
           ELSE line_status
         END
       WHERE invoice_batch_id = $1
         AND id = $2
       RETURNING *`,
      [
        batchId,
        batchLineId,
        ratePerLtr,
        subtotal,
        gst,
        pst,
        qst,
        amountTotal,
        markupChecked,
        issueNote,
      ]
    );

    if (update.rows.length === 0) {
      return res.status(404).json({ message: "Batch row not found" });
    }

    return res.json({
      message: "Batch row updated",
      row: update.rows[0],
    });
  } catch (err) {
    console.error("Update batch row error:", err);
    return res.status(500).json({ message: "Server error" });
  }
});

router.post("/invoice-batches/:id/generate-invoices", authMiddleware, async (req, res) => {
  const client = await pool.connect();
  try {
    if (!requireAdmin(req, res)) return;
    await ensureInvoiceBatchPhase2Schema();
    await ensureGeneratedInvoicesSchemaCompat();
    await ensureInvoiceNumberSequenceSchema();

    const batchId = parseInt(req.params.id, 10);
    if (!Number.isInteger(batchId) || batchId < 1) {
      return res.status(400).json({ message: "Invalid batch id" });
    }

    const batchResult = await client.query(
      `SELECT id, batch_code, status, date_from, date_to
       FROM invoice_batches
       WHERE id = $1
       LIMIT 1`,
      [batchId]
    );
    if (batchResult.rows.length === 0) {
      return res.status(404).json({ message: "Batch not found" });
    }

    const batch = batchResult.rows[0];
    if (batch.status !== "REVIEWED") {
      return res.status(409).json({ message: "Batch must be REVIEWED before invoice generation" });
    }

    const invalidRows = await client.query(
      `SELECT COUNT(*)::int AS count
       FROM invoice_batch_transactions
       WHERE invoice_batch_id = $1
         AND line_status IN ('RATE_MISSING', 'MARKUP_MISSING', 'ERROR')`,
      [batchId]
    );
    if ((invalidRows.rows[0]?.count || 0) > 0) {
      return res.status(409).json({
        message: "Batch still contains invalid rows. Recalculate and fix flags before generating invoices.",
      });
    }

    const alreadyInvoicedRows = await client.query(
      `SELECT COUNT(*)::int AS count
       FROM invoice_batch_transactions ibt
       JOIN transactions t ON t.id = ibt.transaction_id
       WHERE ibt.invoice_batch_id = $1
         AND COALESCE(t.is_invoiced, false) = true`,
      [batchId]
    );
    if ((alreadyInvoicedRows.rows[0]?.count || 0) > 0) {
      return res.status(409).json({
        message: "Batch has transactions already invoiced. Aborting to prevent duplicates.",
      });
    }

    const customerGroups = await client.query(
      `SELECT
         ibt.customer_id,
         MIN(t.purchase_datetime::date) AS period_start,
         MAX(t.purchase_datetime::date) AS period_end,
         COUNT(*)::int AS line_count,
         COALESCE(SUM(COALESCE(ibt.amount_total, t.total, t.total_amount, 0)), 0) AS invoice_total
       FROM invoice_batch_transactions ibt
       JOIN transactions t ON t.id = ibt.transaction_id
       WHERE ibt.invoice_batch_id = $1
         AND ibt.line_status = 'READY'
       GROUP BY ibt.customer_id
       ORDER BY ibt.customer_id ASC`,
      [batchId]
    );

    if (customerGroups.rows.length === 0) {
      return res.status(400).json({ message: "No READY rows in this batch to invoice" });
    }

    const invoiceDate = parseDateOnly(req.body?.invoice_date) || new Date().toISOString().slice(0, 10);
    const dueDate = parseDateOnly(req.body?.due_date);

    await client.query("BEGIN");

    const createdInvoices = [];
    for (const group of customerGroups.rows) {
      const customerId = parseInt(group.customer_id, 10);
      if (!Number.isInteger(customerId) || customerId < 1) {
        continue;
      }

      const forcedInvoiceNo = String(req.body?.invoice_no_prefix || "").trim();
      const generatedNo = await generateInvoiceNo(client, invoiceDate);
      const invoiceNo = forcedInvoiceNo ? `${forcedInvoiceNo}-${generatedNo}` : generatedNo;

      const invoiceInsert = await client.query(
        `INSERT INTO customer_invoices (
           customer_id,
           invoice_no,
           invoice_date,
           period_start,
           period_end,
           due_date,
           total,
           totals_provided,
           status
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, true, 'issued')
         RETURNING id, customer_id, invoice_no, invoice_date, period_start, period_end, due_date, total, status`,
        [
          customerId,
          invoiceNo,
          invoiceDate,
          group.period_start || batch.date_from || null,
          group.period_end || batch.date_to || null,
          dueDate,
          Number(group.invoice_total) || 0,
        ]
      );

      const invoice = invoiceInsert.rows[0];

      await client.query(
        `UPDATE transactions t
         SET
           is_invoiced = true,
           invoice_id = $1
         FROM invoice_batch_transactions ibt
         WHERE ibt.invoice_batch_id = $2
           AND ibt.customer_id = $3
           AND ibt.line_status = 'READY'
           AND ibt.transaction_id = t.id`,
        [invoice.id, batchId, customerId]
      );

      await client.query(
        `UPDATE invoice_batch_transactions
         SET line_status = 'INVOICED', issue_note = NULL
         WHERE invoice_batch_id = $1
           AND customer_id = $2
           AND line_status = 'READY'`,
        [batchId, customerId]
      );

      createdInvoices.push({
        ...invoice,
        line_count: Number(group.line_count) || 0,
      });
    }

    await client.query(
      `UPDATE invoice_batches
       SET status = 'INVOICED', updated_at = now()
       WHERE id = $1`,
      [batchId]
    );

    await client.query("COMMIT");
    return res.json({
      message: "Invoices generated from batch",
      batch_id: batchId,
      invoice_count: createdInvoices.length,
      invoices: createdInvoices,
    });
  } catch (err) {
    try {
      await client.query("ROLLBACK");
    } catch (_rollbackErr) {}
    console.error("Generate invoices from batch error:", err);
    return res.status(500).json({ message: "Server error" });
  } finally {
    client.release();
  }
});

module.exports = router;
