const jwt = require("jsonwebtoken");

module.exports = function (req, res, next) {
  const authHeader = req.header("Authorization");

  if (!authHeader) {
    return res.status(401).json({ message: "No token provided" });
  }

  try {
    const token = authHeader.split(" ")[1];

    const verified = jwt.verify(token, "fuellink_secret_key");

    req.user = verified;

    next();
  } catch (err) {
    res.status(401).json({ message: "Invalid token" });
  }
};