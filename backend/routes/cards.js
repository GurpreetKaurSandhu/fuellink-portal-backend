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

    const result = await pool.query(
      `SELECT
         card_number,
         driver_name,
         status,
         company_name,
         customer_number
       FROM cards
       WHERE customer_id = $1
       ORDER BY id DESC`,
      [customerId]
    );

    return res.json(result.rows);
  } catch (err) {
    console.error("GET /api/cards error:", err);
    return res.status(500).json({ message: "Server error" });
  }
});

module.exports = router;
