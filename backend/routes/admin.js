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

const round4 = (value) => {
  if (value == null || !Number.isFinite(value)) return null;
  return Math.round(value * 10000) / 10000;
};

const parseBooleanish = (value, fallback = true) => {
  if (value == null) return fallback;
  if (typeof value === "boolean") return value;
  const normalized = String(value).trim().toLowerCase();
  if (normalized === "true" || normalized === "1" || normalized === "yes") return true;
  if (normalized === "false" || normalized === "0" || normalized === "no") return false;
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
  id, customer_number, company_name, owner_name, city, email, bv, fuellink_card, otp_setup,
  deposit, address, security_deposit_invoice, customer_status, reference_name, comment,
  rate_group_id, created_at
`;

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

router.post("/cards/import", authMiddleware, upload.single("file"), async (req, res) => {
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
      if (normalized === "customernumber") headerMap.customer_number = key;
      if (normalized === "companyname") headerMap.company_name = key;
      if (normalized === "drivername") headerMap.driver_name = key;
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

          if (!customerId) {
            throw new Error("Unable to resolve customer (customer_number/company_name/default customer)");
          }

          const upsert = await client.query(
            `INSERT INTO cards (customer_id, card_number, driver_name, status)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (card_number) DO UPDATE SET
               customer_id = EXCLUDED.customer_id,
               driver_name = EXCLUDED.driver_name,
               status = EXCLUDED.status
             RETURNING id, customer_id, card_number, driver_name, status, (xmax = 0) AS inserted`,
            [customerId, cardNumber, driverName, status]
          );

          const card = upsert.rows[0];
          await client.query(
            `INSERT INTO cards_history
             (card_id, customer_id, card_number, driver_name, status, changed_by_user_id)
             VALUES ($1, $2, $3, $4, $5, $6)`,
            [card.id, card.customer_id, card.card_number, card.driver_name, card.status, req.user.id || null]
          );

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

    return res.json({
      inserted_count: insertedCount,
      updated_count: updatedCount,
      errors,
      note: "PIN fields are ignored and not stored",
    });
  } catch (err) {
    console.error(err);
    return res.status(500).json({ message: "Server error" });
  } finally {
    safeUnlink(req.file?.path);
  }
});

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

    const result = await pool.query(
      "SELECT id, name, markup_per_liter, created_at FROM rate_groups ORDER BY name ASC"
    );
    res.json(result.rows);
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

router.get("/customers/lookup", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;

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
      if (normalized === "bv") headerMap.bv = key;
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
        const bv = headerMap.bv ? String(row[headerMap.bv] || "").trim() : null;
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
            customer_number, company_name, owner_name, city, email, bv, fuellink_card, otp_setup,
            deposit, address, security_deposit_invoice, customer_status, reference_name, comment
          )
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
          ON CONFLICT (customer_number) DO UPDATE SET
            company_name = EXCLUDED.company_name,
            owner_name = EXCLUDED.owner_name,
            city = EXCLUDED.city,
            email = EXCLUDED.email,
            bv = EXCLUDED.bv,
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
            bv || null,
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

router.post("/customers", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;

    const {
      customer_number,
      company_name,
      owner_name,
      city,
      email,
      bv,
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
        customer_number, company_name, owner_name, city, email, bv, fuellink_card, otp_setup,
        deposit, address, security_deposit_invoice, customer_status, reference_name, comment
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
      RETURNING ${customerColumns}`,
      [
        customerNumber,
        companyName,
        owner_name || null,
        city || null,
        email || null,
        bv || null,
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
      bv: "bv",
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

module.exports = router;
