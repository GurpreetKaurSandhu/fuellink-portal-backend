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
  origin: (origin, cb) => {
    if (!origin) return cb(null, true); // allow curl/postman
    const allowed = [
      "http://localhost:5173",
      "https://fuellink-portal-frontend.vercel.app",
    ];
    if (allowed.includes(origin)) return cb(null, true);
    if (origin.endsWith(".vercel.app")) return cb(null, true); // allow preview deployments
    return cb(new Error("CORS blocked: " + origin));
  },
  
  credentials: true,
  methods: ["GET","POST","PUT","PATCH","DELETE","OPTIONS"],
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
      date: "t.purchase_datetime",
      amount: "COALESCE(t.total, t.total_amount, 0)",
      litres: "t.volume_liters",
      card_number: "t.card_number",
      driver_name: "t.driver_name",
      location: "t.location",
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

    if (isValidDate(date_from)) {
      addClause("purchase_datetime >= ?", date_from);
    } else if (date_from) {
      return res.status(400).json({ message: "Invalid date_from" });
    }

    if (isValidDate(date_to)) {
      addClause("purchase_datetime <= ?", date_to);
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
        SELECT
          COUNT(*)::int AS count,
          COALESCE(SUM(volume_liters), 0) AS total_litres,
          COALESCE(SUM(total_amount), 0) AS total_amount
        FROM transactions
        ${whereSql}
      `;

      const totalsResult = await pool.query(totalsSql, values);
      const totals = totalsResult.rows[0] || {
        count: 0,
        total_litres: 0,
        total_amount: 0,
      };

      const offset = (pageNum - 1) * pageSizeNum;
      const dataSql = `
        SELECT *
        FROM transactions
        ${whereSql}
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

    const totalsResult = await pool.query(
      `SELECT
         COUNT(*)::int AS count,
         COALESCE(SUM(t.volume_liters), 0) AS total_litres,
         COALESCE(SUM(COALESCE(t.total, t.total_amount, 0)), 0) AS total_amount
       FROM transactions t
       LEFT JOIN customers c ON c.id = t.customer_id
       LEFT JOIN rate_groups rg ON rg.id = c.rate_group_id
       ${whereSqlCustomer}`,
      values
    );
    const totals = totalsResult.rows[0] || {
      count: 0,
      total_litres: 0,
      total_amount: 0,
    };

    const offset = (pageNum - 1) * pageSizeNum;
    const dataResult = await pool.query(
      `SELECT
         t.purchase_datetime,
         t.card_number,
         t.driver_name,
         t.location,
         COALESCE(t.province, '') AS province,
         t.product,
         t.volume_liters,
         COALESCE(
           t.computed_rate_per_liter,
           CASE
             WHEN COALESCE(t.volume_liters, 0) > 0 THEN ROUND(
               (COALESCE(t.subtotal, t.total, t.total_amount, 0) / t.volume_liters) +
               COALESCE(t.markup_per_liter, rg.markup_per_liter, 0),
               4
             )
             ELSE NULL
           END
         ) AS computed_rate_per_liter,
         COALESCE(
           t.subtotal,
           COALESCE(t.total, t.total_amount, 0) - COALESCE(t.gst, 0) - COALESCE(t.pst, 0) - COALESCE(t.qst, 0)
         ) AS subtotal,
         COALESCE(t.gst, 0) AS gst,
         COALESCE(t.pst, 0) AS pst,
         COALESCE(t.qst, 0) AS qst,
         COALESCE(
           t.total,
           t.total_amount,
           COALESCE(
             t.subtotal,
             COALESCE(t.total, t.total_amount, 0) - COALESCE(t.gst, 0) - COALESCE(t.pst, 0) - COALESCE(t.qst, 0)
           ) + COALESCE(t.gst, 0) + COALESCE(t.pst, 0) + COALESCE(t.qst, 0)
         ) AS total
       FROM transactions t
       LEFT JOIN customers c ON c.id = t.customer_id
       LEFT JOIN rate_groups rg ON rg.id = c.rate_group_id
       ${whereSqlCustomer}
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

    const result = await pool.query(
      `SELECT
         t.purchase_datetime,
         t.card_number,
         t.driver_name,
         t.location,
         COALESCE(t.province, '') AS province,
         t.product,
         t.volume_liters,
         COALESCE(
           t.computed_rate_per_liter,
           CASE
             WHEN COALESCE(t.volume_liters, 0) > 0 THEN ROUND(
               (COALESCE(t.subtotal, t.total, t.total_amount, 0) / t.volume_liters) +
               COALESCE(t.markup_per_liter, rg.markup_per_liter, 0),
               4
             )
             ELSE NULL
           END
         ) AS computed_rate_per_liter,
       COALESCE(
           t.subtotal,
           COALESCE(t.total, t.total_amount, 0) - COALESCE(t.gst, 0) - COALESCE(t.pst, 0) - COALESCE(t.qst, 0)
         ) AS subtotal,
         COALESCE(t.gst, 0) AS gst,
         COALESCE(t.pst, 0) AS pst,
         COALESCE(t.qst, 0) AS qst,
         COALESCE(
           t.total,
           t.total_amount,
           COALESCE(
             t.subtotal,
             COALESCE(t.total, t.total_amount, 0) - COALESCE(t.gst, 0) - COALESCE(t.pst, 0) - COALESCE(t.qst, 0)
           ) + COALESCE(t.gst, 0) + COALESCE(t.pst, 0) + COALESCE(t.qst, 0)
         ) AS total
       FROM transactions t
       LEFT JOIN customers c ON c.id = t.customer_id
       LEFT JOIN rate_groups rg ON rg.id = c.rate_group_id
       WHERE t.customer_id = $1
         AND t.purchase_datetime >= $2
         AND t.purchase_datetime <= $3
       ORDER BY t.purchase_datetime DESC, t.id DESC`,
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
