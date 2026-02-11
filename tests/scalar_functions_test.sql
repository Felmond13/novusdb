-- =============================================================
-- Test suite: Scalar Functions
-- =============================================================

-- Setup
INSERT INTO users VALUES {"name": "Alice", "city": "Paris", "age": 30, "salary": 55000.75};
INSERT INTO users VALUES {"name": "Bob", "city": "  Lyon  ", "age": 25, "salary": -1200.5};
INSERT INTO users VALUES {"name": "Charlie", "city": "Marseille", "age": 35, "salary": 72000};

-- ======================== STRING ========================

-- UPPER
SELECT UPPER(name) AS upper_name FROM users WHERE name = "Alice";
-- Expected: upper_name = "ALICE"

-- LOWER
SELECT LOWER(city) AS lower_city FROM users WHERE name = "Alice";
-- Expected: lower_city = "paris"

-- TRIM
SELECT TRIM(city) AS trimmed FROM users WHERE name = "Bob";
-- Expected: trimmed = "Lyon"

-- LENGTH
SELECT LENGTH(name) AS len FROM users WHERE name = "Charlie";
-- Expected: len = 7

-- SUBSTR with 2 args
SELECT SUBSTR(name, 1, 3) AS sub FROM users WHERE name = "Alice";
-- Expected: sub = "Ali"

-- SUBSTR with 2 args (no length)
SELECT SUBSTR(name, 4) AS sub FROM users WHERE name = "Charlie";
-- Expected: sub = "rlie"

-- CONCAT
SELECT CONCAT(name, " - ", city) AS full_info FROM users WHERE name = "Alice";
-- Expected: full_info = "Alice - Paris"

-- REPLACE
SELECT REPLACE(city, "Paris", "Bordeaux") AS new_city FROM users WHERE name = "Alice";
-- Expected: new_city = "Bordeaux"

-- INSTR
SELECT INSTR(name, "li") AS pos FROM users WHERE name = "Alice";
-- Expected: pos = 2

-- REVERSE
SELECT REVERSE(name) AS rev FROM users WHERE name = "Bob";
-- Expected: rev = "boB"

-- REPEAT
SELECT REPEAT(name, 2) AS rep FROM users WHERE name = "Bob";
-- Expected: rep = "BobBob"

-- ======================== MATH ========================

-- ABS
SELECT ABS(salary) AS abs_sal FROM users WHERE name = "Bob";
-- Expected: abs_sal = 1200.5

-- ROUND with no decimals
SELECT ROUND(salary) AS rounded FROM users WHERE name = "Alice";
-- Expected: rounded = 55001

-- ROUND with decimals
SELECT ROUND(salary, 1) AS rounded FROM users WHERE name = "Alice";
-- Expected: rounded = 55000.8

-- CEIL
SELECT CEIL(salary) AS ceiled FROM users WHERE name = "Bob";
-- Expected: ceiled = -1200

-- FLOOR
SELECT FLOOR(salary) AS floored FROM users WHERE name = "Alice";
-- Expected: floored = 55000

-- ======================== UTILITY ========================

-- COALESCE with NULL
SELECT COALESCE(email, name) AS result FROM users WHERE name = "Alice";
-- Expected: result = "Alice" (email is NULL)

-- TYPEOF
SELECT TYPEOF(age) AS type_age, TYPEOF(name) AS type_name, TYPEOF(salary) AS type_sal FROM users WHERE name = "Alice";
-- Expected: type_age = "integer", type_name = "text", type_sal = "real"

-- IFNULL
SELECT IFNULL(email, "no-email") AS mail FROM users WHERE name = "Bob";
-- Expected: mail = "no-email"

-- ======================== WHERE clause ========================

-- Scalar in WHERE
SELECT name FROM users WHERE UPPER(city) = "PARIS";
-- Expected: name = "Alice"

SELECT name FROM users WHERE LENGTH(name) > 4;
-- Expected: Alice, Charlie

-- ======================== COMBINED ========================

-- Nested scalar functions
SELECT UPPER(SUBSTR(name, 1, 3)) AS prefix FROM users WHERE name = "Charlie";
-- Expected: prefix = "CHA"

SELECT CONCAT(UPPER(name), " (", city, ")") AS display FROM users WHERE name = "Alice";
-- Expected: display = "ALICE (Paris)"
