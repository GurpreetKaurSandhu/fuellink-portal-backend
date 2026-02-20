const adminRoutes = require("./routes/admin");
console.log("🔥 CLEAN SERVER VERSION 🔥");

const express = require("express");
const cors = require("cors");
const pool = require("./db");
const authRoutes = require("./routes/auth");
const cardsRoutes = require("./routes/cards");
const authMiddleware = require("./middleware/authMiddleware");

const app = express();

app.use(cors());
app.use(express.json());
app.use("/api/admin", adminRoutes);
app.use("/api/auth", authRoutes);
app.use("/api/cards", cardsRoutes);

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

app.get("/test", (req, res) => {
  res.send("Test route working");
});

app.listen(8000, () => {
  console.log("Server running on port 8000");
});
