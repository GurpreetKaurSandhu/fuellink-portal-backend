const express = require("express");
const router = express.Router();
const pool = require("../db");
const authMiddleware = require("../middleware/authMiddleware");

router.get("/", authMiddleware, async (req, res) => {
  try {
    const { card_number, driver_name, status, customer_id } = req.query;
    const isAdmin = req.user && req.user.role === "admin";

    const where = [];
    const values = [];
    const addClause = (clause, value) => {
      values.push(value);
      where.push(clause.replace("?", `$${values.length}`));
    };

    if (card_number) {
      addClause("card_number = ?", String(card_number).trim());
    }
    if (driver_name) {
      addClause("driver_name ILIKE ?", `%${String(driver_name).trim()}%`);
    }
    if (status) {
      addClause("status = ?", String(status).trim());
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
      if (!req.user || !req.user.customer_id) {
        return res.status(400).json({ message: "Missing customer_id" });
      }
      addClause("customer_id = ?", req.user.customer_id);
    }

    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
    const result = await pool.query(
      `SELECT * FROM cards ${whereSql} ORDER BY id DESC`,
      values
    );

    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Server error" });
  }
});

router.get("/:card_number", authMiddleware, async (req, res) => {
  try {
    const cardNumber = String(req.params.card_number || "").trim();
    if (!cardNumber) {
      return res.status(400).json({ message: "Invalid card_number" });
    }

    const isAdmin = req.user && req.user.role === "admin";
    const values = [cardNumber];
    let whereSql = "WHERE card_number = $1";

    if (!isAdmin) {
      if (!req.user || !req.user.customer_id) {
        return res.status(400).json({ message: "Missing customer_id" });
      }
      values.push(req.user.customer_id);
      whereSql += " AND customer_id = $2";
    }

    const result = await pool.query(
      `SELECT * FROM cards ${whereSql} LIMIT 1`,
      values
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "Card not found" });
    }

    res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Server error" });
  }
});

async function setCardStatus(req, res, status) {
  try {
    const cardNumber = String(req.params.card_number || "").trim();
    if (!cardNumber) {
      return res.status(400).json({ message: "Invalid card_number" });
    }

    const isAdmin = req.user && req.user.role === "admin";
    const values = [status, cardNumber];
    let whereSql = "WHERE card_number = $2";

    if (!isAdmin) {
      if (!req.user || !req.user.customer_id) {
        return res.status(400).json({ message: "Missing customer_id" });
      }
      values.push(req.user.customer_id);
      whereSql += " AND customer_id = $3";
    }

    const client = await pool.connect();
    let result;
    try {
      await client.query("BEGIN");
      result = await client.query(
        `UPDATE cards SET status = $1 ${whereSql} RETURNING *`,
        values
      );

      if (result.rows.length === 0) {
        await client.query("ROLLBACK");
        return res.status(404).json({ message: "Card not found" });
      }

      const card = result.rows[0];
      await client.query(
        `INSERT INTO cards_history
         (card_id, customer_id, card_number, driver_name, status, changed_by_user_id)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [
          card.id,
          card.customer_id,
          card.card_number,
          card.driver_name,
          card.status,
          req.user.id || null,
        ]
      );

      await client.query("COMMIT");
      res.json(card);
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Server error" });
  }
}

router.post("/:card_number/block", authMiddleware, (req, res) => {
  setCardStatus(req, res, "blocked");
});

router.post("/:card_number/unblock", authMiddleware, (req, res) => {
  setCardStatus(req, res, "active");
});

router.post("/bulk", authMiddleware, async (req, res) => {
  try {
    const { data, customer_id: bodyCustomerId } = req.body || {};

    if (!req.user || req.user.role !== "admin") {
      return res.status(403).json({ message: "Access denied" });
    }

    if (!Array.isArray(data) || data.length === 0) {
      return res.status(400).json({ message: "data must be a non-empty array" });
    }
    if (data.length > 1000) {
      return res.status(400).json({ message: "data array too large" });
    }

    const rows = data.map((item, idx) => {
      if (!item || !item.card_number) {
        throw new Error(`Missing card_number at index ${idx}`);
      }

      const card_number = String(item.card_number).trim();
      if (!card_number) {
        throw new Error(`Invalid card_number at index ${idx}`);
      }

      const driver_name = item.driver_name ? String(item.driver_name).trim() : null;
      const status = item.status ? String(item.status).trim() : "active";

      const candidate = item.customer_id ?? bodyCustomerId;
      if (candidate == null) {
        throw new Error(`Missing customer_id for card_number ${card_number}`);
      }
      const parsed = parseInt(candidate, 10);
      if (!Number.isInteger(parsed) || parsed < 1) {
        throw new Error(`Invalid customer_id for card_number ${card_number}`);
      }
      const customer_id = parsed;

      return { card_number, driver_name, status, customer_id };
    });

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      for (const row of rows) {
        const upsertResult = await client.query(
          `INSERT INTO cards (customer_id, card_number, driver_name, status)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (card_number) DO UPDATE SET
             customer_id = EXCLUDED.customer_id,
             driver_name = EXCLUDED.driver_name,
             status = EXCLUDED.status
           RETURNING id, customer_id, card_number, driver_name, status`,
          [row.customer_id, row.card_number, row.driver_name, row.status]
        );

        const card = upsertResult.rows[0];
        await client.query(
          `INSERT INTO cards_history
           (card_id, customer_id, card_number, driver_name, status, changed_by_user_id)
           VALUES ($1, $2, $3, $4, $5, $6)`,
          [
            card.id,
            card.customer_id,
            card.card_number,
            card.driver_name,
            card.status,
            req.user.id || null,
          ]
        );
      }

      await client.query("COMMIT");
    } catch (err) {
      await client.query("ROLLBACK");
      throw err;
    } finally {
      client.release();
    }

    res.json({ processed: rows.length });
  } catch (err) {
    console.error(err);
    res.status(400).json({ message: err.message || "Invalid payload" });
  }
});

module.exports = router;
