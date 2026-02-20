const express = require("express");
const router = express.Router();
const multer = require("multer");
const pdf = require("pdf-parse");
const fs = require("fs");
const path = require("path");
const pool = require("../db");  // ← ADD THIS
const authMiddleware = require("../middleware/authMiddleware");

const upload = multer({
  dest: path.join(__dirname, "../uploads"),
});

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

      if (req.user.role !== "admin") {
        return res.status(403).json({
          message: "Access denied"
        });
      }

      const filePath = path.join(__dirname, "../uploads", req.file.filename);

     const dataBuffer = fs.readFileSync(filePath);
const data = await pdf(dataBuffer);

const lines = data.text.split("\n");

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

      await pool.query(
        `INSERT INTO transactions 
        (customer_id, card_number, driver_name, purchase_datetime, product, volume_liters, total_amount, document_number, location)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
        [
          1, // TEMP customer_id
          card_number,
          driver_name,
          `${purchase_date} ${purchase_time}`,
          product,
          volume,
          amount,
          document_number,
          location
        ]
      );

    } catch (err) {
      console.log("Skipped line:", line);
    }
  }
}

fs.unlinkSync(filePath);

res.json({
  message: "Transactions parsed and inserted successfully"
});

    } catch (err) {
      console.error("FULL ERROR:", err);
      res.status(500).json({
        message: "Server error while processing PDF",
        error: err.message
      });
    }
  }
);

module.exports = router;