const express = require("express");
const router = express.Router();
const pool = require("../db");
const bcrypt = require("bcrypt");
const jwt = require("jsonwebtoken");
const authMiddleware = require("../middleware/authMiddleware");

if (!process.env.JWT_SECRET) {
  throw new Error("JWT_SECRET is not set");
}

router.post("/register", async (req, res) => {
  try {
    const { name, email, password } = req.body;

    const hashedPassword = await bcrypt.hash(password, 10);

    const newUser = await pool.query(
      "INSERT INTO users (name, email, password) VALUES ($1, $2, $3) RETURNING id, name, email, role",
      [name, email, hashedPassword]
    );

    res.json({
      message: "User registered successfully",
      user: newUser.rows[0],
    });
  } catch (err) {
    console.error(err.message);
    res.status(500).send("Error registering user");
  }
});

router.post("/login", async (req, res) => {
  try {
    const email = String(req.body?.email || "").trim();
    const password = String(req.body?.password || "");

    if (!email || !password) {
      return res.status(400).json({ message: "email and password are required" });
    }

    const user = await pool.query(
      "SELECT * FROM users WHERE lower(trim(email)) = lower($1) LIMIT 1",
      [email]
    );

    if (user.rows.length === 0) {
      return res.status(400).json({ message: "User not found" });
    }

    const validPassword = await bcrypt.compare(
      password,
      user.rows[0].password
    );

    if (!validPassword) {
      return res.status(400).json({ message: "Invalid password" });
    }

    console.log("USER FROM DB:", user.rows[0]);

    const token = jwt.sign(
      {
        id: user.rows[0].id,
        email: user.rows[0].email,
        role: user.rows[0].role,
        customer_id: user.rows[0].customer_id,
      },
      process.env.JWT_SECRET,
      { expiresIn: "1h" }
    );

    res.json({
      message: "Login successful",
      token,
    });
  } catch (err) {
    console.error(err.message);
    res.status(500).send("Server error");
  }
});

router.post("/change-password", authMiddleware, async (req, res) => {
  try {
    if (!req.user?.id) {
      return res.status(401).json({ message: "Unauthorized" });
    }

    const currentPassword = String(req.body?.current_password || "");
    const newPassword = String(req.body?.new_password || "");

    if (!currentPassword || !newPassword) {
      return res.status(400).json({ message: "current_password and new_password are required" });
    }
    if (newPassword.length < 8) {
      return res.status(400).json({ message: "new_password must be at least 8 characters" });
    }

    const userResult = await pool.query(
      "SELECT id, password FROM users WHERE id = $1 LIMIT 1",
      [req.user.id]
    );
    if (userResult.rows.length === 0) {
      return res.status(404).json({ message: "User not found" });
    }

    const valid = await bcrypt.compare(currentPassword, userResult.rows[0].password);
    if (!valid) {
      return res.status(400).json({ message: "Invalid current_password" });
    }

    const hash = await bcrypt.hash(newPassword, 10);
    await pool.query(
      "UPDATE users SET password = $1, must_change_password = false WHERE id = $2",
      [hash, req.user.id]
    );

    return res.json({ message: "Password updated" });
  } catch (err) {
    console.error(err.message);
    return res.status(500).json({ message: "Server error" });
  }
});

module.exports = router;
