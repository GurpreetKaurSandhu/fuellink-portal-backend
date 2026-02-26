const express = require("express");
const pool = require("../db");
const authMiddleware = require("../middleware/authMiddleware");

const router = express.Router();
const SUPPORT_NOTIFY_EMAIL = process.env.SUPPORT_NOTIFY_EMAIL || "support@fuellink.ca";

const sendSupportNotification = async ({
  requestId,
  requestType,
  cardNumber,
  message,
  customerId,
  customerNumber,
  companyName,
  customerLoginEmail,
}) => {
  let nodemailer;
  try {
    // Lazy load so app still runs if package is missing.
    nodemailer = require("nodemailer");
  } catch (err) {
    console.error("Support email notification disabled: nodemailer not installed");
    return;
  }

  const host = process.env.SMTP_HOST;
  const port = Number(process.env.SMTP_PORT || 587);
  const user = process.env.SMTP_USER;
  const pass = process.env.SMTP_PASS;
  const from = process.env.SMTP_FROM || user || "no-reply@fuellink.ca";

  if (!host || !user || !pass) {
    console.error("Support email notification skipped: SMTP env vars missing");
    return;
  }

  const transporter = nodemailer.createTransport({
    host,
    port,
    secure: port === 465,
    auth: { user, pass },
  });

  const subject = `FuelLink Support Request #${requestId} (${requestType})`;
  const text = [
    "A new support request was created in FuelLink Portal.",
    "",
    `Request ID: ${requestId}`,
    `Request Type: ${requestType || "N/A"}`,
    `Customer ID: ${customerId || "N/A"}`,
    `Customer Number: ${customerNumber || "N/A"}`,
    `Company Name: ${companyName || "N/A"}`,
    `Customer Login Email: ${customerLoginEmail || "N/A"}`,
    `Card Number: ${cardNumber || "N/A"}`,
    "",
    "Message:",
    message || "N/A",
  ].join("\n");

  await transporter.sendMail({
    from,
    to: SUPPORT_NOTIFY_EMAIL,
    subject,
    text,
  });
};

const requireAdmin = (req, res) => {
  if (!req.user || req.user.role !== "admin") {
    res.status(403).json({ message: "Access denied" });
    return false;
  }
  return true;
};

const statusValues = new Set(["open", "in_progress", "closed"]);

const tableColumns = async () => {
  const result = await pool.query(
    `SELECT column_name
     FROM information_schema.columns
     WHERE table_schema = 'public'
       AND table_name = 'support_requests'`
  );
  return new Set(result.rows.map((row) => row.column_name));
};

router.get("/support-requests", authMiddleware, async (req, res) => {
  try {
    const columns = await tableColumns();
    const hasInternalNotes = columns.has("internal_notes");
    const isAdmin = req.user && req.user.role === "admin";
    const customerId = req.user?.customer_id || null;
    if (!isAdmin && !customerId) {
      return res.status(403).json({ message: "Access denied" });
    }

    const values = [];
    let whereSql = "";
    if (!isAdmin) {
      values.push(customerId);
      whereSql = "WHERE sr.customer_id = $1";
    }

    const selectFields = [
      "sr.id",
      "sr.customer_id",
      "sr.created_at",
      "sr.request_type",
      "sr.card_id",
      "sr.card_number",
      "sr.status",
      "sr.message",
      hasInternalNotes ? "sr.internal_notes" : "NULL::text AS internal_notes",
      "c.customer_number",
      "c.company_name",
    ];

    const result = await pool.query(
      `SELECT ${selectFields.join(", ")}
       FROM support_requests sr
       JOIN customers c ON c.id = sr.customer_id
       ${whereSql}
       ORDER BY sr.created_at DESC`,
      values
    );

    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Server error" });
  }
});

router.get("/admin/support-requests", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;
    const columns = await tableColumns();
    const hasInternalNotes = columns.has("internal_notes");
    const selectFields = [
      "sr.id",
      "sr.customer_id",
      "sr.created_at",
      "sr.request_type",
      "sr.card_id",
      "sr.card_number",
      "sr.status",
      "sr.message",
      hasInternalNotes ? "sr.internal_notes" : "NULL::text AS internal_notes",
      "c.customer_number",
      "c.company_name",
    ];

    const result = await pool.query(
      `SELECT ${selectFields.join(", ")}
       FROM support_requests sr
       JOIN customers c ON c.id = sr.customer_id
       ORDER BY sr.created_at DESC`
    );

    res.json(result.rows);
  } catch (err) {
    console.error(err);
    res.status(500).json({ message: "Server error" });
  }
});

router.post("/support-requests", authMiddleware, async (req, res) => {
  try {
    if (!req.user || req.user.role === "admin" || !req.user.customer_id) {
      return res.status(403).json({ message: "Access denied" });
    }

    const requestType = String(req.body?.request_type || "").trim();
    const cardNumber = String(req.body?.card_number || "").trim();
    const message = String(req.body?.message || "").trim();

    if (!requestType) {
      return res.status(400).json({ message: "request_type is required" });
    }

    let cardId = null;
    if (cardNumber) {
      const cardResult = await pool.query(
        "SELECT id FROM cards WHERE card_number = $1 AND customer_id = $2 LIMIT 1",
        [cardNumber, req.user.customer_id]
      );
      if (cardResult.rows.length === 0) {
        return res.status(400).json({ message: "Invalid card_number for customer" });
      }
      cardId = cardResult.rows[0].id;
    }

    const columns = await tableColumns();
    const hasCardNumber = columns.has("card_number");
    const hasInternalNotes = columns.has("internal_notes");

    const insertColumns = ["customer_id", "card_id", "request_type", "message", "status"];
    const insertValues = [req.user.customer_id, cardId, requestType, message || null, "open"];
    if (hasCardNumber) {
      insertColumns.push("card_number");
      insertValues.push(cardNumber || null);
    }
    if (hasInternalNotes) {
      insertColumns.push("internal_notes");
      insertValues.push(null);
    }

    const placeholders = insertValues.map((_, index) => `$${index + 1}`).join(", ");
    const result = await pool.query(
      `INSERT INTO support_requests (${insertColumns.join(", ")})
       VALUES (${placeholders})
       RETURNING id, customer_id, card_id, request_type, message, status, created_at`,
      insertValues
    );

    const created = result.rows[0];
    try {
      const customerMeta = await pool.query(
        "SELECT customer_number, company_name FROM customers WHERE id = $1 LIMIT 1",
        [req.user.customer_id]
      );
      const meta = customerMeta.rows[0] || {};
      await sendSupportNotification({
        requestId: created.id,
        requestType: created.request_type,
        cardNumber: cardNumber || null,
        message: created.message,
        customerId: req.user.customer_id,
        customerNumber: meta.customer_number || null,
        companyName: meta.company_name || null,
        customerLoginEmail: req.user.email || null,
      });
    } catch (notifyErr) {
      console.error("Support notification email failed:", notifyErr.message || notifyErr);
    }

    return res.status(201).json(created);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ message: "Server error" });
  }
});

router.patch("/admin/support-requests/:id", authMiddleware, async (req, res) => {
  try {
    if (!requireAdmin(req, res)) return;

    const id = parseInt(req.params.id, 10);
    if (!Number.isInteger(id) || id < 1) {
      return res.status(400).json({ message: "Invalid id" });
    }

    const updates = [];
    const values = [];

    if (Object.prototype.hasOwnProperty.call(req.body || {}, "status")) {
      const status = String(req.body.status || "").trim().toLowerCase();
      if (!statusValues.has(status)) {
        return res.status(400).json({ message: "Invalid status. Use open, in_progress, or closed" });
      }
      values.push(status);
      updates.push(`status = $${values.length}`);
    }

    if (Object.prototype.hasOwnProperty.call(req.body || {}, "internal_notes")) {
      const columns = await tableColumns();
      if (columns.has("internal_notes")) {
        values.push(String(req.body.internal_notes || "").trim() || null);
        updates.push(`internal_notes = $${values.length}`);
      }
    }

    if (updates.length === 0) {
      return res.status(400).json({ message: "No updatable fields provided" });
    }

    values.push(id);
    const columns = await tableColumns();
    const hasInternalNotes = columns.has("internal_notes");
    const returningFields = [
      "id",
      "customer_id",
      "card_id",
      "request_type",
      "message",
      "status",
      hasInternalNotes ? "internal_notes" : "NULL::text AS internal_notes",
      "created_at",
    ];
    const result = await pool.query(
      `UPDATE support_requests
       SET ${updates.join(", ")}
       WHERE id = $${values.length}
       RETURNING ${returningFields.join(", ")}`,
      values
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "Support request not found" });
    }

    return res.json(result.rows[0]);
  } catch (err) {
    console.error(err);
    return res.status(500).json({ message: "Server error" });
  }
});

module.exports = router;
