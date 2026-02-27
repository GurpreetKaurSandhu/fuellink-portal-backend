const express = require("express");
const router = express.Router();
const pool = require("../db");
const authMiddleware = require("../middleware/authMiddleware");

/**
 * Customer-safe cards endpoint (NO PIN exposure).
 * - Customer: only their cards
 * - Admin: can view all (still no pin here; admin has /api/admin/cards)
 */
router.get("/", authMiddleware, async (req, res) => {
  try {
    if (!req.user) return res.status(401).json({ message: "Unauthorized" });

    // Admin can view all cards (still safe fields only)
    if (req.user.role === "admin") {
      const result = await pool.query(
        `SELECT
           card_number,
           driver_name,
           status,
           company_name,
           customer_number
         FROM cards
         ORDER BY id DESC`
      );
      return res.json(result.rows);
    }

    // Customer must be linked to a customer_id
    const customerId = req.user.customer_id;
    if (!customerId) return res.status(400).json({ message: "Missing customer_id" });

    const customerResult = await pool.query(
      `SELECT customer_number, company_name
       FROM customers
       WHERE id = $1
       LIMIT 1`,
      [customerId]
    );
    const customer = customerResult.rows[0] || {};
    const customerNumber = String(customer.customer_number || "").trim();
    const companyName = String(customer.company_name || "").trim();

    const result = await pool.query(
      `SELECT
         card_number,
         driver_name,
         status,
         company_name,
         customer_number
       FROM cards
       WHERE
         customer_id = $1
         OR (
           customer_id IS NULL
           AND (
             ($2 <> '' AND lower(trim(COALESCE(cards.customer_number, ''))) = lower(trim($2)))
             OR (
               $3 <> ''
               AND regexp_replace(lower(COALESCE(cards.company_name, '')), '[^a-z0-9]+', '', 'g') =
                   regexp_replace(lower($3), '[^a-z0-9]+', '', 'g')
             )
           )
         )
       ORDER BY id DESC`,
      [customerId, customerNumber, companyName]
    );

    return res.json(result.rows);
  } catch (err) {
    console.error("GET /api/cards error:", err);
    return res.status(500).json({ message: "Server error" });
  }
});

module.exports = router;
