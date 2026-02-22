const pool = require("../db");

const isValidDateString = (value) => {
  if (!value) return false;
  const d = new Date(value);
  return !Number.isNaN(d.getTime());
};

const recalculateTransactions = async ({ date_from, date_to, customer_id }) => {
  if (!isValidDateString(date_from) || !isValidDateString(date_to)) {
    throw new Error("Invalid date range");
  }

  const values = [date_from, date_to];
  let where = "WHERE purchase_datetime >= $1 AND purchase_datetime <= $2";

  if (customer_id) {
    values.push(customer_id);
    where += ` AND customer_id = $${values.length}`;
  }

  const sql = `
    WITH target AS (
      SELECT id, customer_id, purchase_datetime, volume_liters, location, province
      FROM transactions
      ${where}
    ),
    updated AS (
      UPDATE transactions t
      SET
        province = COALESCE(t.province, extract_province_from_location(t.location)),
        computed_rate_per_liter = CASE
          WHEN r.price IS NULL THEN NULL
          ELSE ROUND(r.price + COALESCE(rg.markup_per_liter, 0), 4)
        END,
        subtotal = CASE
          WHEN r.price IS NULL THEN NULL
          ELSE ROUND((r.price + COALESCE(rg.markup_per_liter, 0)) * t.volume_liters, 4)
        END,
        gst = CASE
          WHEN r.price IS NULL THEN NULL
          ELSE ROUND(((r.price + COALESCE(rg.markup_per_liter, 0)) * t.volume_liters) * COALESCE(tr.gst_rate, 0), 4)
        END,
        pst = CASE
          WHEN r.price IS NULL THEN NULL
          ELSE ROUND(((r.price + COALESCE(rg.markup_per_liter, 0)) * t.volume_liters) * COALESCE(tr.pst_rate, 0), 4)
        END,
        qst = CASE
          WHEN r.price IS NULL THEN NULL
          ELSE ROUND(((r.price + COALESCE(rg.markup_per_liter, 0)) * t.volume_liters) * COALESCE(tr.qst_rate, 0), 4)
        END,
        total = CASE
          WHEN r.price IS NULL THEN NULL
          ELSE ROUND(((r.price + COALESCE(rg.markup_per_liter, 0)) * t.volume_liters) * (1 + COALESCE(tr.gst_rate, 0) + COALESCE(tr.pst_rate, 0) + COALESCE(tr.qst_rate, 0)), 4)
        END
      FROM target tt
      LEFT JOIN customers c ON c.id = tt.customer_id
      LEFT JOIN rate_groups rg ON rg.id = c.rate_group_id
      LEFT JOIN LATERAL rate_match(
        tt.customer_id,
        COALESCE(tt.province, extract_province_from_location(tt.location)),
        tt.location,
        tt.purchase_datetime
      ) r ON true
      LEFT JOIN LATERAL (
        SELECT gst_rate, pst_rate, qst_rate
        FROM tax_rules
        WHERE province = COALESCE(tt.province, extract_province_from_location(tt.location))
          AND effective_from <= tt.purchase_datetime::date
        ORDER BY effective_from DESC
        LIMIT 1
      ) tr ON true
      WHERE t.id = tt.id
      RETURNING t.id,
        (r.price IS NULL) AS missing_rate,
        (tr.gst_rate IS NULL AND tr.pst_rate IS NULL AND tr.qst_rate IS NULL) AS missing_tax
    )
    SELECT
      COUNT(*)::int AS updated_count,
      COALESCE(SUM(CASE WHEN missing_rate THEN 1 ELSE 0 END), 0)::int AS missing_rate_count,
      COALESCE(SUM(CASE WHEN missing_tax THEN 1 ELSE 0 END), 0)::int AS missing_tax_count
    FROM updated;
  `;

  const result = await pool.query(sql, values);
  return result.rows[0] || { updated_count: 0, missing_rate_count: 0, missing_tax_count: 0 };
};

module.exports = {
  recalculateTransactions,
};
