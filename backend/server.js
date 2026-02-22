const path = require("path");
require("dotenv").config({ path: path.resolve(__dirname, "../.env") });

const adminRoutes = require("./routes/admin");
console.log("🔥 CLEAN SERVER VERSION 🔥");

const express = require("express");
const cors = require("cors");
const pool = require("./db");
const authRoutes = require("./routes/auth");
const cardsRoutes = require("./routes/cards");
const ratesRoutes = require("./routes/rates");
const authMiddleware = require("./middleware/authMiddleware");
const { recalculateTransactions } = require("./services/recalculateTransactions");

const app = express();

app.use(cors());
app.use(express.json());
app.use("/api/admin", adminRoutes);
app.use("/api/auth", authRoutes);
app.use("/api/cards", cardsRoutes);
app.use("/api/rates", ratesRoutes);

app.get("/", async (req, res) => {
  const result = await pool.query("SELECT NOW()");
  res.json(result.rows);
});

app.get("/api/dashboard", authMiddleware, (req, res) => {
  res.json({
    message: "Welcome to Fuel Link Dashboard",
    user: req.user,
  });
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

    const allowedSort = {
      date: "purchase_datetime",
      amount: "total_amount",
      litres: "volume_liters",
      card_number: "card_number",
      driver_name: "driver_name",
      location: "location",
    };
    const sortColumn = allowedSort[sortBy] || allowedSort.date;
    const sortOrder = String(order).toLowerCase() === "asc" ? "ASC" : "DESC";

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

    if (req.user.role === "admin") {
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

    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

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
    const dataValues = values.concat([pageSizeNum, offset]);
    const dataResult = await pool.query(dataSql, dataValues);

    res.json({
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
