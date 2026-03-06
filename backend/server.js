const path = require("path");
const supportRequestsRouter = require("./routes/supportRequests");
require("dotenv").config({ path: path.resolve(__dirname, "../.env") });
const adminRoutes = require("./routes/admin");
console.log("🔥 CLEAN SERVER VERSION 🔥");

const express = require("express");
const cors = require("cors");
const pool = require("./db");
const authRoutes = require("./routes/auth");
const cardsRoutes = require("./routes/cards");
const ratesRoutes = require("./routes/rates");
const invoicesRouter = require("./routes/invoices");
const billedTransactionsRouter = require("./routes/billedTransactions");
const authMiddleware = require("./middleware/authMiddleware");
const { recalculateTransactions } = require("./services/recalculateTransactions");

const app = express();

app.use(cors({
  // Reflect any origin to avoid Vercel preflight failures.
  // We still rely on JWT auth for protected endpoints.
  origin: true,
  credentials: true,
  methods: ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
  allowedHeaders: ["Content-Type", "Authorization"],
}));
app.use(express.json());

app.use(express.json());
app.use("/api/admin", adminRoutes);
app.use("/api/auth", authRoutes);
app.use("/api/cards", cardsRoutes);
app.use("/api/rates", ratesRoutes);
app.use("/api", invoicesRouter);
app.use("/api", supportRequestsRouter);
app.use("/api", billedTransactionsRouter);

app.get("/", async (req, res) => {
  const result = await pool.query("SELECT NOW()");
  res.json(result.rows);
});

app.get("/api/health", (_req, res) => {
  res.json({ ok: true, service: "fuellink-backend" });
});

app.get("/api/dashboard", authMiddleware, (req, res) => {
  res.json({
    message: "Welcome to Fuel Link Dashboard",
    user: req.user,
  });
});

app.get("/api/me", authMiddleware, async (req, res) => {
  try {
    let profile = {
      company_name: null,
      customer_number: null,
    };

    if (req.user?.customer_id) {
      const customerResult = await pool.query(
        "SELECT company_name, customer_number FROM customers WHERE id = $1 LIMIT 1",
        [req.user.customer_id]
      );
      if (customerResult.rows.length > 0) {
        profile = customerResult.rows[0];
      }
    }

    return res.json({
      user: req.user,
      ...profile,
    });
  } catch (err) {
    console.error("GET /api/me error:", err);
    return res.status(500).json({ message: "Server error" });
  }
});

app.get("/api/transactions", authMiddleware, async (req, res) => {
  console.log("🚀 TRANSACTIONS ROUTE HIT");

  try {
    const {
      date_from,
      date_to,
      card_number,
      driver_name,
      customer_id,
      page = "1",
      pageSize = "50",
      sortBy = "date",
      order = "desc",
    } = req.query;

    const pageNum = parseInt(page, 10);
    const pageSizeNum = parseInt(pageSize, 10);
    if (!Number.isInteger(pageNum) || pageNum < 1) {
      return res.status(400).json({ message: "Invalid page" });
    }
    if (!Number.isInteger(pageSizeNum) || pageSizeNum < 1 || pageSizeNum > 500) {
      return res.status(400).json({ message: "Invalid pageSize" });
    }

    const isAdmin = req.user.role === "admin";
    const sortOrder = String(order).toLowerCase() === "asc" ? "ASC" : "DESC";
    const allowedSortAdmin = {
      date: "purchase_datetime",
      amount: "total_amount",
      litres: "volume_liters",
      card_number: "card_number",
      driver_name: "driver_name",
      location: "location",
    };
    const allowedSortCustomer = {
      date: "d.purchase_datetime",
      amount: "COALESCE(d.total, d.total_amount, 0)",
      litres: "d.volume_liters",
      card_number: "d.card_number",
      driver_name: "d.driver_name",
      location: "d.location",
    };

    const where = [];
    const values = [];
    const addClause = (clause, value) => {
      values.push(value);
      where.push(clause.replace("?", `$${values.length}`));
    };

    const isValidDate = (value) => {
      if (!value) return false;
      const d = new Date(value);
      return !Number.isNaN(d.getTime());
    };
    const isDateOnly = (value) => /^\d{4}-\d{2}-\d{2}$/.test(String(value || "").trim());

    if (isValidDate(date_from)) {
      addClause("purchase_datetime >= ?", date_from);
    } else if (date_from) {
      return res.status(400).json({ message: "Invalid date_from" });
    }

    if (isValidDate(date_to)) {
      if (isDateOnly(date_to)) {
        addClause("purchase_datetime < (?::date + INTERVAL '1 day')", date_to);
      } else {
        addClause("purchase_datetime <= ?", date_to);
      }
    } else if (date_to) {
      return res.status(400).json({ message: "Invalid date_to" });
    }

    if (card_number) {
      addClause("card_number = ?", String(card_number).trim());
    }
    if (driver_name) {
      addClause("driver_name ILIKE ?", `%${String(driver_name).trim()}%`);
    }

    if (isAdmin) {
      if (customer_id) {
        const customerIdNum = parseInt(customer_id, 10);
        if (!Number.isInteger(customerIdNum) || customerIdNum < 1) {
          return res.status(400).json({ message: "Invalid customer_id" });
        }
        addClause("customer_id = ?", customerIdNum);
      }
    } else {
      addClause("customer_id = ?", req.user.customer_id);
    }

    if (isAdmin) {
      const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
      const sortColumn = allowedSortAdmin[sortBy] || allowedSortAdmin.date;

      const totalsSql = `
        WITH deduped AS (
          SELECT DISTINCT ON (
            COALESCE(customer_id, -1),
            COALESCE(card_number, ''),
            COALESCE(document_number, ''),
            COALESCE(purchase_datetime, 'epoch'::timestamp),
            COALESCE(volume_liters, 0),
            COALESCE(total_amount, 0)
          )
            id,
            volume_liters,
            total_amount
          FROM transactions
          ${whereSql}
          ORDER BY
            COALESCE(customer_id, -1),
            COALESCE(card_number, ''),
            COALESCE(document_number, ''),
            COALESCE(purchase_datetime, 'epoch'::timestamp),
            COALESCE(volume_liters, 0),
            COALESCE(total_amount, 0),
            id DESC
        )
        SELECT
          COUNT(*)::int AS count,
          COALESCE(SUM(volume_liters), 0) AS total_litres,
          COALESCE(SUM(total_amount), 0) AS total_amount
        FROM deduped
      `;

      const totalsResult = await pool.query(totalsSql, values);
      const totals = totalsResult.rows[0] || {
        count: 0,
        total_litres: 0,
        total_amount: 0,
      };

      const offset = (pageNum - 1) * pageSizeNum;
      const dataSql = `
        WITH deduped AS (
          SELECT DISTINCT ON (
            COALESCE(t.customer_id, -1),
            COALESCE(t.card_number, ''),
            COALESCE(t.document_number, ''),
            COALESCE(t.purchase_datetime, 'epoch'::timestamp),
            COALESCE(t.volume_liters, 0),
            COALESCE(t.total_amount, 0)
          )
            t.*,
            COALESCE(c.company_name, c_card.company_name) AS customer_company_name
          FROM transactions t
          LEFT JOIN customers c ON c.id = t.customer_id
          LEFT JOIN LATERAL (
            SELECT c2.company_name
            FROM cards cd
            JOIN customers c2 ON c2.id = cd.customer_id
            WHERE cd.customer_id IS NOT NULL
              AND (
                regexp_replace(COALESCE(cd.card_number, ''), '\\D', '', 'g') =
                regexp_replace(COALESCE(t.card_number, ''), '\\D', '', 'g')
                OR (
                  right(regexp_replace(COALESCE(cd.card_number, ''), '\\D', '', 'g'), 4) <> ''
                  AND right(regexp_replace(COALESCE(cd.card_number, ''), '\\D', '', 'g'), 4) =
                      right(regexp_replace(COALESCE(t.card_number, ''), '\\D', '', 'g'), 4)
                )
              )
            ORDER BY cd.id DESC
            LIMIT 1
          ) c_card ON TRUE
          ${whereSql.replace(/FROM transactions\b/, "FROM transactions t")}
          ORDER BY
            COALESCE(t.customer_id, -1),
            COALESCE(t.card_number, ''),
            COALESCE(t.document_number, ''),
            COALESCE(t.purchase_datetime, 'epoch'::timestamp),
            COALESCE(t.volume_liters, 0),
            COALESCE(t.total_amount, 0),
            t.id DESC
        )
        SELECT * FROM deduped
        ORDER BY ${sortColumn} ${sortOrder}
        LIMIT $${values.length + 1} OFFSET $${values.length + 2}
      `;
      const dataResult = await pool.query(dataSql, values.concat([pageSizeNum, offset]));

      return res.json({
        data: dataResult.rows,
        totals,
        page: pageNum,
        pageSize: pageSizeNum,
      });
    }

    const whereSqlCustomer = where.length
      ? `WHERE ${where
          .join(" AND ")
          .replace(/\bpurchase_datetime\b/g, "t.purchase_datetime")
          .replace(/\bcard_number\b/g, "t.card_number")
          .replace(/\bdriver_name\b/g, "t.driver_name")
          .replace(/\bcustomer_id\b/g, "t.customer_id")}`
      : "";

    const sortColumnCustomer = allowedSortCustomer[sortBy] || allowedSortCustomer.date;

    const assignmentsExistsResult = await pool.query(
      `SELECT to_regclass('public.customer_rate_group_assignments') IS NOT NULL AS exists`
    );
    const hasAssignmentsTable = !!assignmentsExistsResult.rows[0]?.exists;
    const customerRateJoinSql = hasAssignmentsTable
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

    const totalsResult = await pool.query(
      `WITH deduped AS (
         SELECT DISTINCT ON (
           COALESCE(t.customer_id, -1),
           COALESCE(t.card_number, ''),
           COALESCE(t.document_number, ''),
           COALESCE(t.purchase_datetime, 'epoch'::timestamp),
           COALESCE(t.volume_liters, 0),
           COALESCE(t.total_amount, 0)
         )
           t.*
         FROM transactions t
         LEFT JOIN customers c ON c.id = t.customer_id
         ${customerRateJoinSql}
         ${whereSqlCustomer}
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
         COUNT(*)::int AS count,
         COALESCE(SUM(d.volume_liters), 0) AS total_litres,
         COALESCE(SUM(COALESCE(d.total, d.total_amount, 0)), 0) AS total_amount
       FROM deduped d`,
      values
    );
    const totals = totalsResult.rows[0] || {
      count: 0,
      total_litres: 0,
      total_amount: 0,
    };

    const offset = (pageNum - 1) * pageSizeNum;
    const dataResult = await pool.query(
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
           c.company_name AS customer_company_name,
           COALESCE(rg.markup_per_liter, 0) AS effective_markup_per_liter
         FROM transactions t
         LEFT JOIN customers c ON c.id = t.customer_id
         ${customerRateJoinSql}
         ${whereSqlCustomer}
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
         d.purchase_datetime,
         d.card_number,
         d.driver_name,
         COALESCE(d.customer_company_name, d.source_raw_json->>'Company Name', d.source_raw_json->>'company_name') AS company_name,
         d.location,
         COALESCE(d.province, '') AS province,
         d.product,
         d.volume_liters,
         COALESCE(
           d.computed_rate_per_liter,
           CASE
             WHEN COALESCE(d.source_raw_json->>'rate_per_ltr', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (d.source_raw_json->>'rate_per_ltr')::numeric
             WHEN COALESCE(d.source_raw_json->>'rate_per_liter', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (d.source_raw_json->>'rate_per_liter')::numeric
             ELSE NULL
           END,
           CASE
             WHEN COALESCE(d.volume_liters, 0) > 0 THEN ROUND(
               (COALESCE(d.subtotal, d.total, d.total_amount, 0) / d.volume_liters) +
               COALESCE(d.effective_markup_per_liter, 0),
               4
             )
             ELSE NULL
           END
         ) AS computed_rate_per_liter,
         COALESCE(
           d.subtotal,
           CASE
             WHEN COALESCE(d.source_raw_json->>'subtotal', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (d.source_raw_json->>'subtotal')::numeric
             ELSE NULL
           END,
           COALESCE(d.total, d.total_amount, 0) - COALESCE(d.gst, 0) - COALESCE(d.pst, 0) - COALESCE(d.qst, 0)
         ) AS subtotal,
         COALESCE(
           d.gst,
           CASE
             WHEN COALESCE(d.source_raw_json->>'gst_hst', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (d.source_raw_json->>'gst_hst')::numeric
             WHEN COALESCE(d.source_raw_json->>'gst', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (d.source_raw_json->>'gst')::numeric
             ELSE NULL
           END,
           0
         ) AS gst,
         COALESCE(
           d.pst,
           CASE
             WHEN COALESCE(d.source_raw_json->>'pst', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (d.source_raw_json->>'pst')::numeric
             ELSE NULL
           END,
           0
         ) AS pst,
         COALESCE(
           d.qst,
           CASE
             WHEN COALESCE(d.source_raw_json->>'qst', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (d.source_raw_json->>'qst')::numeric
             ELSE NULL
           END,
           0
         ) AS qst,
         COALESCE(
           d.total,
           CASE
             WHEN COALESCE(d.source_raw_json->>'amount', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (d.source_raw_json->>'amount')::numeric
             ELSE NULL
           END,
           d.total_amount,
           COALESCE(
             d.subtotal,
             COALESCE(d.total, d.total_amount, 0) - COALESCE(d.gst, 0) - COALESCE(d.pst, 0) - COALESCE(d.qst, 0)
           ) + COALESCE(d.gst, 0) + COALESCE(d.pst, 0) + COALESCE(d.qst, 0)
         ) AS total
       FROM deduped d
       ORDER BY ${sortColumnCustomer} ${sortOrder}
       LIMIT $${values.length + 1} OFFSET $${values.length + 2}`,
      values.concat([pageSizeNum, offset])
    );

    return res.json({
      data: dataResult.rows,
      totals,
      page: pageNum,
      pageSize: pageSizeNum,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Server error" });
  }
});

app.get("/api/transactions/export", authMiddleware, async (req, res) => {
  try {
    if (req.user?.role === "admin" || !req.user?.customer_id) {
      return res.status(403).json({ message: "Access denied" });
    }

    const dateFrom = String(req.query.date_from || "").trim();
    const dateTo = String(req.query.date_to || "").trim();
    if (!dateFrom || !dateTo) {
      return res.status(400).json({ message: "date_from and date_to are required" });
    }

    const assignmentsExistsResult = await pool.query(
      `SELECT to_regclass('public.customer_rate_group_assignments') IS NOT NULL AS exists`
    );
    const hasAssignmentsTable = !!assignmentsExistsResult.rows[0]?.exists;
    const customerRateJoinSql = hasAssignmentsTable
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

    const result = await pool.query(
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
         LEFT JOIN customers c ON c.id = t.customer_id
         ${customerRateJoinSql}
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
         d.purchase_datetime,
         d.card_number,
         d.driver_name,
         d.location,
         COALESCE(d.province, '') AS province,
         d.product,
         d.volume_liters,
         COALESCE(
           d.computed_rate_per_liter,
           CASE
             WHEN COALESCE(d.source_raw_json->>'rate_per_ltr', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (d.source_raw_json->>'rate_per_ltr')::numeric
             WHEN COALESCE(d.source_raw_json->>'rate_per_liter', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (d.source_raw_json->>'rate_per_liter')::numeric
             ELSE NULL
           END,
           CASE
             WHEN COALESCE(d.volume_liters, 0) > 0 THEN ROUND(
               (COALESCE(d.subtotal, d.total, d.total_amount, 0) / d.volume_liters) +
               COALESCE(d.effective_markup_per_liter, 0),
               4
             )
             ELSE NULL
           END
         ) AS computed_rate_per_liter,
         COALESCE(
           d.subtotal,
           CASE
             WHEN COALESCE(d.source_raw_json->>'subtotal', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (d.source_raw_json->>'subtotal')::numeric
             ELSE NULL
           END,
           COALESCE(d.total, d.total_amount, 0) - COALESCE(d.gst, 0) - COALESCE(d.pst, 0) - COALESCE(d.qst, 0)
         ) AS subtotal,
         COALESCE(
           d.gst,
           CASE
             WHEN COALESCE(d.source_raw_json->>'gst_hst', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (d.source_raw_json->>'gst_hst')::numeric
             WHEN COALESCE(d.source_raw_json->>'gst', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (d.source_raw_json->>'gst')::numeric
             ELSE NULL
           END,
           0
         ) AS gst,
         COALESCE(
           d.pst,
           CASE
             WHEN COALESCE(d.source_raw_json->>'pst', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (d.source_raw_json->>'pst')::numeric
             ELSE NULL
           END,
           0
         ) AS pst,
         COALESCE(
           d.qst,
           CASE
             WHEN COALESCE(d.source_raw_json->>'qst', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (d.source_raw_json->>'qst')::numeric
             ELSE NULL
           END,
           0
         ) AS qst,
         COALESCE(
           d.total,
           CASE
             WHEN COALESCE(d.source_raw_json->>'amount', '') ~ '^-?[0-9]+(\\.[0-9]+)?$'
               THEN (d.source_raw_json->>'amount')::numeric
             ELSE NULL
           END,
           d.total_amount,
           COALESCE(
             d.subtotal,
             COALESCE(d.total, d.total_amount, 0) - COALESCE(d.gst, 0) - COALESCE(d.pst, 0) - COALESCE(d.qst, 0)
           ) + COALESCE(d.gst, 0) + COALESCE(d.pst, 0) + COALESCE(d.qst, 0)
         ) AS total
       FROM deduped d
       ORDER BY d.purchase_datetime DESC, d.id DESC`,
      [req.user.customer_id, dateFrom, dateTo]
    );

    const header = [
      "purchase_datetime",
      "card_number",
      "driver_name",
      "location",
      "province",
      "product",
      "volume_liters",
      "computed_rate_per_liter",
      "subtotal",
      "gst",
      "pst",
      "qst",
      "total",
    ];
    const escape = (value) => {
      const text = String(value ?? "");
      if (/[",\n]/.test(text)) return `"${text.replace(/"/g, '""')}"`;
      return text;
    };
    const csvLines = [header.join(",")];
    result.rows.forEach((row) => {
      csvLines.push(header.map((key) => escape(row[key])).join(","));
    });

    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader(
      "Content-Disposition",
      `attachment; filename=\"transactions-${dateFrom}-to-${dateTo}.csv\"`
    );
    return res.send(`${csvLines.join("\n")}\n`);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ message: "Server error" });
  }
});

app.post("/api/transactions/recalculate", authMiddleware, async (req, res) => {
  try {
    if (!req.user || req.user.role !== "admin") {
      return res.status(403).json({ message: "Access denied" });
    }

    const { date_from, date_to, customer_id } = req.body || {};
    if (!date_from || !date_to) {
      return res.status(400).json({ message: "date_from and date_to are required" });
    }

    let customerIdNum = null;
    if (customer_id != null) {
      const parsed = parseInt(customer_id, 10);
      if (!Number.isInteger(parsed) || parsed < 1) {
        return res.status(400).json({ message: "Invalid customer_id" });
      }
      customerIdNum = parsed;
    }

    const result = await recalculateTransactions({
      date_from,
      date_to,
      customer_id: customerIdNum,
    });

    res.json({
      message: "Recalculation complete",
      ...result,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: err.message || "Server error" });
  }
});

app.get("/api/invoices/preview", authMiddleware, async (req, res) => {
  try {
    const { date_from, date_to, customer_id } = req.query;

    if (!date_from || !date_to) {
      return res.status(400).json({ message: "date_from and date_to are required" });
    }

    const isAdmin = req.user && req.user.role === "admin";
    const values = [date_from, date_to];
    let where = "WHERE purchase_datetime >= $1 AND purchase_datetime <= $2";

    if (isAdmin) {
      if (customer_id) {
        const parsed = parseInt(customer_id, 10);
        if (!Number.isInteger(parsed) || parsed < 1) {
          return res.status(400).json({ message: "Invalid customer_id" });
        }
        values.push(parsed);
        where += ` AND customer_id = $${values.length}`;
      }
    } else {
      values.push(req.user.customer_id);
      where += ` AND customer_id = $${values.length}`;
    }

    const sql = `
      SELECT
        customer_id,
        COUNT(*)::int AS transaction_count,
        COALESCE(SUM(volume_liters), 0) AS total_litres,
        COALESCE(SUM(subtotal), 0) AS subtotal,
        COALESCE(SUM(gst), 0) AS gst,
        COALESCE(SUM(pst), 0) AS pst,
        COALESCE(SUM(qst), 0) AS qst,
        COALESCE(SUM(total), 0) AS total
      FROM transactions
      ${where}
      GROUP BY customer_id
      ORDER BY customer_id ASC
    `;

    const result = await pool.query(sql, values);

    res.json({
      date_from,
      date_to,
      data: result.rows,
    });
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Server error" });
  }
});

app.get("/test", (req, res) => {
  res.send("Test route working");
});

const port = process.env.PORT ? parseInt(process.env.PORT, 10) : 8000;

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
