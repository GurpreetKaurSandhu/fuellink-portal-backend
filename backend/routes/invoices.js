const express = require("express");
const multer = require("multer");
const fs = require("fs");
const path = require("path");
const pool = require("../db");
const authMiddleware = require("../middleware/authMiddleware");

const router = express.Router();

const upload = multer({ storage: multer.memoryStorage() });

const isAdmin = (user) => user && user.role === "admin";

const requireAdmin = (req, res) => {
  if (!isAdmin(req.user)) {
    res.status(403).json({ message: "Access denied" });
    return false;
  }
  return true;
};

const sanitizeInvoiceNo = (value) => {
  const cleaned = String(value || "")
    .trim()
    .replace(/[^a-zA-Z0-9_-]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return cleaned || "invoice";
};

const generateInvoiceNo = () =>
  `AUTO-${new Date().toISOString().slice(0, 10).replace(/-/g, "")}-${Math.floor(Math.random() * 1e6)
    .toString()
    .padStart(6, "0")}`;

const parseOptionalNumber = (value) => {
  if (value == null) return null;
  const trimmed = String(value).trim();
  if (!trimmed) return null;
  const parsed = Number(trimmed);
  return Number.isFinite(parsed) ? parsed : null;
};

const buildTotalsPayload = (body) => {
  const fields = ["subtotal", "gst", "hst", "pst", "qst", "total"];
  const totalsProvided = fields.some((field) => {
    const value = body?.[field];
    return value != null && String(value).trim() !== "";
  });

  const numbers = {};
  for (const field of fields) {
    const parsed = parseOptionalNumber(body?.[field]);
    if (parsed == null) {
      numbers[field] = 0;
    } else {
      numbers[field] = parsed;
    }
  }

  return { totalsProvided, numbers };
};

const ensureCustomerExists = async (customerId) => {
  const result = await pool.query("SELECT id FROM customers WHERE id = $1", [customerId]);
  return result.rows.length > 0;
};

const ensureSafePath = (storageKey) => {
  const baseDir = path.resolve(__dirname, "..");
  const resolved = path.resolve(baseDir, storageKey);
  if (!resolved.startsWith(baseDir)) {
    return null;
  }
  return resolved;
};

const ensureInvoicesSchemaCompat = async () => {
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
    ALTER TABLE customer_invoices
      ADD COLUMN IF NOT EXISTS invoice_date date,
      ADD COLUMN IF NOT EXISTS period_start date,
      ADD COLUMN IF NOT EXISTS period_end date,
      ADD COLUMN IF NOT EXISTS subtotal numeric(12,2) DEFAULT 0,
      ADD COLUMN IF NOT EXISTS gst numeric(12,2) DEFAULT 0,
      ADD COLUMN IF NOT EXISTS hst numeric(12,2) DEFAULT 0,
      ADD COLUMN IF NOT EXISTS pst numeric(12,2) DEFAULT 0,
      ADD COLUMN IF NOT EXISTS qst numeric(12,2) DEFAULT 0,
      ADD COLUMN IF NOT EXISTS total numeric(12,2) DEFAULT 0,
      ADD COLUMN IF NOT EXISTS totals_provided boolean DEFAULT false,
      ADD COLUMN IF NOT EXISTS status text DEFAULT 'issued'
  `);

  await pool.query(`
    DO $$
    BEGIN
      IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'customer_invoices'
          AND column_name = 'total_amount'
      ) THEN
        UPDATE customer_invoices
        SET total = total_amount
        WHERE total IS NULL AND total_amount IS NOT NULL;
      END IF;
    END $$;
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS customer_invoice_files (
      id bigserial PRIMARY KEY,
      invoice_id bigint NOT NULL REFERENCES customer_invoices(id) ON DELETE CASCADE,
      file_type text NOT NULL,
      original_name text,
      mime_type text,
      size_bytes bigint,
      storage_key text,
      created_at timestamptz NOT NULL DEFAULT now()
    )
  `);

  await pool.query(`
    ALTER TABLE customer_invoice_files
      ADD COLUMN IF NOT EXISTS original_name text,
      ADD COLUMN IF NOT EXISTS mime_type text,
      ADD COLUMN IF NOT EXISTS size_bytes bigint,
      ADD COLUMN IF NOT EXISTS storage_key text,
      ADD COLUMN IF NOT EXISTS original_filename text,
      ADD COLUMN IF NOT EXISTS stored_filename text
  `);

  await pool.query(`
    UPDATE customer_invoice_files
    SET storage_key = COALESCE(storage_key, stored_filename)
    WHERE storage_key IS NULL AND stored_filename IS NOT NULL
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

  await pool.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_customer_invoice_files_invoice_filetype'
      ) THEN
        ALTER TABLE customer_invoice_files
          ADD CONSTRAINT uq_customer_invoice_files_invoice_filetype UNIQUE (invoice_id, file_type);
      END IF;
    END $$;
  `);
};

router.get("/customers", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    await ensureInvoicesSchemaCompat();

    const result = await pool.query(
      "SELECT id, customer_number, company_name FROM customers ORDER BY company_name ASC"
    );
    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Server error" });
  }
});

router.post(
  "/admin/invoices",
  authMiddleware,
  upload.fields([
    { name: "invoice_pdf", maxCount: 1 },
    { name: "transaction_report_pdf", maxCount: 1 },
  ]),
  async (req, res) => {
    try {
      if (!requireAdmin(req, res)) return;
      await ensureInvoicesSchemaCompat();

      const customerIdNum = parseInt(req.body?.customer_id, 10);
      if (!Number.isInteger(customerIdNum) || customerIdNum < 1) {
        return res.status(400).json({ message: "Invalid customer_id" });
      }

      const invoiceNoInput = String(req.body?.invoice_no || "").trim();
      const invoiceNo = invoiceNoInput || generateInvoiceNo();

      const invoiceDateRaw = String(req.body?.invoice_date || "").trim();
      const periodStartRaw = String(req.body?.period_start || "").trim();
      const periodEndRaw = String(req.body?.period_end || "").trim();

      if (!(await ensureCustomerExists(customerIdNum))) {
        return res.status(404).json({ message: "Customer not found" });
      }

      const { totalsProvided, numbers } = buildTotalsPayload(req.body);

      const files = req.files || {};
      const invoiceFile = files.invoice_pdf ? files.invoice_pdf[0] : null;
      const reportFile = files.transaction_report_pdf
        ? files.transaction_report_pdf[0]
        : null;

      if (!invoiceFile) {
        return res.status(400).json({ message: "invoice_pdf is required" });
      }

      const sanitizedInvoiceNo = sanitizeInvoiceNo(invoiceNo);
      const baseDir = path.join(
        __dirname,
        "..",
        "uploads",
        "invoices",
        String(customerIdNum),
        sanitizedInvoiceNo
      );
      fs.mkdirSync(baseDir, { recursive: true });

      const fileWrites = [];
      const storedFiles = {};

      const writeFile = (file, fileType) => {
        if (!file) return null;
        const ext = path.extname(file.originalname || "").toLowerCase() || ".pdf";
        const filename = `${fileType}${ext}`;
        const fullPath = path.join(baseDir, filename);
        fs.writeFileSync(fullPath, file.buffer);
        const storageKey = path.relative(path.join(__dirname, ".."), fullPath);
        storedFiles[fileType] = {
          original_name: file.originalname || null,
          mime_type: file.mimetype || null,
          size_bytes: file.size || null,
          storage_key: storageKey,
        };
        return storageKey;
      };

      fileWrites.push(writeFile(invoiceFile, "invoice_pdf"));
      fileWrites.push(writeFile(reportFile, "transaction_report_pdf"));

      const invoiceInsert = await pool.query(
        `
        INSERT INTO customer_invoices (
          customer_id,
          invoice_no,
          invoice_date,
          period_start,
          period_end,
          subtotal,
          gst,
          hst,
          pst,
          qst,
          total,
          totals_provided
        )
        VALUES (
          $1,
          $2,
          COALESCE(NULLIF($3, '')::date, CURRENT_DATE),
          NULLIF($4, '')::date,
          NULLIF($5, '')::date,
          $6,
          $7,
          $8,
          $9,
          $10,
          $11,
          $12
        )
        ON CONFLICT (customer_id, invoice_no)
        DO UPDATE SET
          invoice_date = EXCLUDED.invoice_date,
          period_start = EXCLUDED.period_start,
          period_end = EXCLUDED.period_end,
          subtotal = EXCLUDED.subtotal,
          gst = EXCLUDED.gst,
          hst = EXCLUDED.hst,
          pst = EXCLUDED.pst,
          qst = EXCLUDED.qst,
          total = EXCLUDED.total,
          totals_provided = EXCLUDED.totals_provided
        RETURNING id, invoice_no, invoice_date, period_start, period_end, total, totals_provided
      `,
        [
          customerIdNum,
          invoiceNo,
          invoiceDateRaw,
          periodStartRaw,
          periodEndRaw,
          numbers.subtotal,
          numbers.gst,
          numbers.hst,
          numbers.pst,
          numbers.qst,
          numbers.total,
          totalsProvided,
        ]
      );

      const invoiceRow = invoiceInsert.rows[0];

      const upsertFile = async (fileType) => {
        const payload = storedFiles[fileType];
        if (!payload) return;
        await pool.query(
          `
          INSERT INTO customer_invoice_files (
            invoice_id,
            file_type,
            original_name,
            mime_type,
            size_bytes,
            storage_key
          )
          VALUES ($1, $2, $3, $4, $5, $6)
          ON CONFLICT (invoice_id, file_type)
          DO UPDATE SET
            original_name = EXCLUDED.original_name,
            mime_type = EXCLUDED.mime_type,
            size_bytes = EXCLUDED.size_bytes,
            storage_key = EXCLUDED.storage_key,
            created_at = now()
        `,
          [
            invoiceRow.id,
            fileType,
            payload.original_name,
            payload.mime_type,
            payload.size_bytes,
            payload.storage_key,
          ]
        );
      };

      await upsertFile("invoice_pdf");
      await upsertFile("transaction_report_pdf");

      res.json({
        invoice_id: invoiceRow.id,
        invoice_no: invoiceRow.invoice_no,
        invoice_date: invoiceRow.invoice_date,
        period_start: invoiceRow.period_start,
        period_end: invoiceRow.period_end,
        total: invoiceRow.total,
        totals_provided: invoiceRow.totals_provided,
        files: {
          invoice_pdf: Boolean(storedFiles.invoice_pdf),
          transaction_report_pdf: Boolean(storedFiles.transaction_report_pdf),
        },
      });
    } catch (err) {
      console.error(err);
      res.status(500).json({ message: "Server error" });
    }
  }
);

router.get("/admin/invoices", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    await ensureInvoicesSchemaCompat();

    const values = [];
    let whereSql = "";
    if (req.query?.customer_id) {
      const customerIdNum = parseInt(req.query.customer_id, 10);
      if (!Number.isInteger(customerIdNum) || customerIdNum < 1) {
        return res.status(400).json({ message: "Invalid customer_id" });
      }
      values.push(customerIdNum);
      whereSql = "WHERE ci.customer_id = $1";
    }

    const result = await pool.query(
      `
      SELECT
        ci.id AS invoice_id,
        ci.invoice_no,
        ci.invoice_date,
        ci.period_start,
        ci.period_end,
        ci.total,
        ci.totals_provided,
        ci.status,
        ci.customer_id,
        c.customer_number,
        c.company_name,
        COALESCE(
          array_agg(cif.file_type) FILTER (WHERE cif.file_type IS NOT NULL),
          ARRAY[]::text[]
        ) AS file_types
      FROM customer_invoices ci
      JOIN customers c ON c.id = ci.customer_id
      LEFT JOIN customer_invoice_files cif ON cif.invoice_id = ci.id
      ${whereSql}
      GROUP BY ci.id, c.customer_number, c.company_name
      ORDER BY ci.invoice_date DESC, ci.id DESC
    `,
      values
    );

    const data = result.rows.map((row) => {
      const fileTypes = row.file_types || [];
      const downloadUrls = {};
      if (fileTypes.includes("invoice_pdf")) {
        downloadUrls.invoice_pdf = `/api/admin/invoices/${row.invoice_id}/files/invoice_pdf`;
      }
      if (fileTypes.includes("transaction_report_pdf")) {
        downloadUrls.transaction_report_pdf = `/api/admin/invoices/${row.invoice_id}/files/transaction_report_pdf`;
      }
      return {
        ...row,
        file_types: fileTypes,
        download_urls: downloadUrls,
      };
    });

    res.json(data);
  } catch (err) {
    const errorId = `inv-list-admin-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
    console.error(`[${errorId}] GET /api/admin/invoices failed`, err);
    res.status(500).json({ message: "Server error", errorId });
  }
});

router.patch("/admin/invoices/:invoiceId", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    await ensureInvoicesSchemaCompat();

    const invoiceId = parseInt(req.params.invoiceId, 10);
    if (!Number.isInteger(invoiceId) || invoiceId < 1) {
      return res.status(400).json({ message: "Invalid invoiceId" });
    }

    if (req.body?.total == null || String(req.body.total).trim() === "") {
      return res.status(400).json({ message: "total is required" });
    }

    const fields = [
      "invoice_date",
      "period_start",
      "period_end",
      "subtotal",
      "gst",
      "hst",
      "pst",
      "qst",
      "total",
      "status",
    ];

    const values = [];
    const updates = [];
    for (const field of fields) {
      if (req.body?.[field] == null) continue;
      const trimmed = String(req.body[field]).trim();
      if (trimmed === "") continue;
      values.push(trimmed);
      updates.push(`${field} = $${values.length}`);
    }

    if (updates.length === 0) {
      return res.status(400).json({ message: "No fields to update" });
    }

    values.push(invoiceId);

    const result = await pool.query(
      `
      UPDATE customer_invoices
      SET ${updates.join(", ")}
      WHERE id = $${values.length}
      RETURNING id AS invoice_id, invoice_no, invoice_date, period_start, period_end, total, totals_provided, status
    `,
      values
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "Invoice not found" });
    }

    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Server error" });
  }
});

router.get("/customer/invoices", authMiddleware, async (req, res) => {
  try {
    await ensureInvoicesSchemaCompat();
    if (!req.user?.customer_id) {
      return res.status(403).json({ message: "Access denied" });
    }

    const result = await pool.query(
      `
      SELECT
        ci.id AS invoice_id,
        ci.invoice_no,
        ci.invoice_date,
        ci.period_start,
        ci.period_end,
        ci.total,
        ci.totals_provided,
        ci.status,
        COALESCE(
          array_agg(cif.file_type) FILTER (WHERE cif.file_type IS NOT NULL),
          ARRAY[]::text[]
        ) AS file_types
      FROM customer_invoices ci
      LEFT JOIN customer_invoice_files cif ON cif.invoice_id = ci.id
      WHERE ci.customer_id = $1
      GROUP BY ci.id
      ORDER BY ci.invoice_date DESC, ci.id DESC
    `,
      [req.user.customer_id]
    );

    const data = result.rows.map((row) => {
      const fileTypes = row.file_types || [];
      const downloadUrls = {};
      if (fileTypes.includes("invoice_pdf")) {
        downloadUrls.invoice_pdf = `/api/customer/invoices/${row.invoice_id}/files/invoice_pdf`;
      }
      if (fileTypes.includes("transaction_report_pdf")) {
        downloadUrls.transaction_report_pdf =
          `/api/customer/invoices/${row.invoice_id}/files/transaction_report_pdf`;
      }
      return {
        ...row,
        file_types: fileTypes,
        download_urls: downloadUrls,
      };
    });

    res.json(data);
  } catch (err) {
    const errorId = `inv-list-customer-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
    console.error(`[${errorId}] GET /api/customer/invoices failed`, err);
    res.status(500).json({ message: "Server error", errorId });
  }
});

const sendInvoiceFile = async ({ invoiceId, fileType, customerId }, res) => {
  const values = [invoiceId, fileType];
  let whereSql = "WHERE cif.invoice_id = $1 AND cif.file_type = $2";
  if (customerId) {
    values.push(customerId);
    whereSql += " AND ci.customer_id = $3";
  }

  const result = await pool.query(
    `
    SELECT cif.storage_key, cif.mime_type, cif.original_name
    FROM customer_invoice_files cif
    JOIN customer_invoices ci ON ci.id = cif.invoice_id
    ${whereSql}
    LIMIT 1
  `,
    values
  );

  if (result.rows.length === 0) {
    return res.status(404).json({ message: "File not found" });
  }

  const row = result.rows[0];
  const resolved = ensureSafePath(row.storage_key);
  if (!resolved) {
    return res.status(400).json({ message: "Invalid storage key" });
  }

  if (!fs.existsSync(resolved)) {
    return res.status(404).json({ message: "File missing" });
  }

  res.setHeader("Content-Type", row.mime_type || "application/pdf");
  res.setHeader("Content-Disposition", `inline; filename="${row.original_name || "invoice.pdf"}"`);
  return res.sendFile(resolved);
};

router.get("/customer/invoices/:invoiceId/files/:fileType", authMiddleware, async (req, res) => {
  try {
    await ensureInvoicesSchemaCompat();
    if (!req.user?.customer_id) {
      return res.status(403).json({ message: "Access denied" });
    }

    const invoiceId = parseInt(req.params.invoiceId, 10);
    const fileType = req.params.fileType;
    if (!Number.isInteger(invoiceId) || invoiceId < 1) {
      return res.status(400).json({ message: "Invalid invoiceId" });
    }
    if (!["invoice_pdf", "transaction_report_pdf"].includes(fileType)) {
      return res.status(400).json({ message: "Invalid fileType" });
    }

    await sendInvoiceFile(
      { invoiceId, fileType, customerId: req.user.customer_id },
      res
    );
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Server error" });
  }
});

router.get("/admin/invoices/:invoiceId/files/:fileType", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    await ensureInvoicesSchemaCompat();

    const invoiceId = parseInt(req.params.invoiceId, 10);
    const fileType = req.params.fileType;
    if (!Number.isInteger(invoiceId) || invoiceId < 1) {
      return res.status(400).json({ message: "Invalid invoiceId" });
    }
    if (!["invoice_pdf", "transaction_report_pdf"].includes(fileType)) {
      return res.status(400).json({ message: "Invalid fileType" });
    }

    await sendInvoiceFile({ invoiceId, fileType }, res);
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Server error" });
  }
});

module.exports = router;
