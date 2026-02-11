-- ============================================================================
-- NovusDB — Tests complexes sur données nested / JSON
-- Base: series.dlite
-- Usage: NovusDB series.dlite < tests/nested_complex_test.sql
-- ============================================================================

-- ============================================================================
-- PHASE 1 : INSERTS avec dot-notation (champs imbriqués)
-- ============================================================================

INSERT INTO actors VALUES (name="Bryan Cranston", age=68, country="USA", awards.emmys=4, awards.golden_globes=1, awards.total=5);

INSERT INTO actors VALUES (name="Aaron Paul", age=44, country="USA", awards.emmys=3, awards.golden_globes=0, awards.total=3);

INSERT INTO actors VALUES (name="Pedro Pascal", age=49, country="Chile", awards.emmys=0, awards.golden_globes=1, awards.total=1);

INSERT INTO actors VALUES (name="Jenna Ortega", age=21, country="USA", awards.emmys=0, awards.golden_globes=0, awards.total=0);

INSERT INTO actors VALUES (name="Omar Sy", age=46, country="France", awards.emmys=0, awards.golden_globes=0, awards.total=0, awards.cesar=1);

-- Deep nested (3+ niveaux)
INSERT INTO actors VALUES (name="Millie Bobby Brown", age=20, country="UK", awards.emmys=0, awards.golden_globes=0, awards.total=0, contact.agent.name="WME", contact.agent.phone="+1-555-0100", contact.social.instagram="milliebobbybrown");

-- ============================================================================
-- PHASE 2 : INSERTS JSON natif (objets, arrays, nested objects dans arrays)
-- ============================================================================

INSERT INTO series VALUES {
  "title": "Breaking Bad",
  "year": 2008,
  "seasons": 5,
  "genre": ["Drama", "Crime", "Thriller"],
  "rating": {"imdb": 9.5, "rotten_tomatoes": 96},
  "network": "AMC",
  "creator": "Vince Gilligan",
  "status": "ended",
  "cast": [
    {"actor": "Bryan Cranston", "role": "Walter White"},
    {"actor": "Aaron Paul", "role": "Jesse Pinkman"}
  ],
  "production": {
    "company": "Sony Pictures Television",
    "country": "USA",
    "budget_per_episode": 3000000
  }
};

INSERT INTO series VALUES {
  "title": "The Last of Us",
  "year": 2023,
  "seasons": 2,
  "genre": ["Drama", "Action", "Sci-Fi"],
  "rating": {"imdb": 8.8, "rotten_tomatoes": 96},
  "network": "HBO",
  "creator": "Craig Mazin",
  "status": "running",
  "cast": [
    {"actor": "Pedro Pascal", "role": "Joel Miller"},
    {"actor": "Bella Ramsey", "role": "Ellie Williams"}
  ],
  "production": {
    "company": "PlayStation Productions",
    "country": "USA",
    "budget_per_episode": 10000000
  }
};

INSERT INTO series VALUES {
  "title": "Wednesday",
  "year": 2022,
  "seasons": 2,
  "genre": ["Comedy", "Horror", "Mystery"],
  "rating": {"imdb": 8.1, "rotten_tomatoes": 73},
  "network": "Netflix",
  "creator": "Tim Burton",
  "status": "running",
  "cast": [
    {"actor": "Jenna Ortega", "role": "Wednesday Addams"}
  ],
  "production": {
    "company": "MGM Television",
    "country": "USA",
    "budget_per_episode": 12000000
  }
};

INSERT INTO series VALUES {
  "title": "Lupin",
  "year": 2021,
  "seasons": 3,
  "genre": ["Crime", "Drama", "Mystery"],
  "rating": {"imdb": 7.5, "rotten_tomatoes": 98},
  "network": "Netflix",
  "creator": "George Kay",
  "status": "running",
  "cast": [
    {"actor": "Omar Sy", "role": "Assane Diop"}
  ],
  "production": {
    "company": "Gaumont",
    "country": "France",
    "budget_per_episode": 5000000
  }
};

INSERT INTO series VALUES {
  "title": "Stranger Things",
  "year": 2016,
  "seasons": 4,
  "genre": ["Drama", "Fantasy", "Horror"],
  "rating": {"imdb": 8.7, "rotten_tomatoes": 91},
  "network": "Netflix",
  "creator": "The Duffer Brothers",
  "status": "ended",
  "cast": [
    {"actor": "Millie Bobby Brown", "role": "Eleven"}
  ],
  "production": {
    "company": "21 Laps Entertainment",
    "country": "USA",
    "budget_per_episode": 30000000
  }
};

-- ============================================================================
-- PHASE 3 : INSERT d'une collection liée (reviews avec nested)
-- ============================================================================

INSERT INTO reviews VALUES (series_title="Breaking Bad", reviewer="John", score=10, detail.story=10, detail.acting=10, detail.visuals=9);

INSERT INTO reviews VALUES (series_title="Breaking Bad", reviewer="Alice", score=9, detail.story=9, detail.acting=10, detail.visuals=8);

INSERT INTO reviews VALUES (series_title="The Last of Us", reviewer="John", score=8, detail.story=9, detail.acting=8, detail.visuals=10);

INSERT INTO reviews VALUES (series_title="The Last of Us", reviewer="Bob", score=9, detail.story=9, detail.acting=9, detail.visuals=10);

INSERT INTO reviews VALUES (series_title="Wednesday", reviewer="Alice", score=7, detail.story=6, detail.acting=8, detail.visuals=9);

INSERT INTO reviews VALUES (series_title="Lupin", reviewer="Alice", score=8, detail.story=8, detail.acting=9, detail.visuals=7);

INSERT INTO reviews VALUES (series_title="Stranger Things", reviewer="Bob", score=8, detail.story=8, detail.acting=7, detail.visuals=9);

INSERT INTO reviews VALUES (series_title="Stranger Things", reviewer="John", score=9, detail.story=9, detail.acting=8, detail.visuals=9);

-- ============================================================================
-- PHASE 4 : VÉRIFICATION DES DONNÉES
-- ============================================================================

.tables

.schema

-- Simple SELECT pour vérifier les inserts
SELECT * FROM actors;
SELECT * FROM series;
SELECT * FROM reviews;

-- ============================================================================
-- PHASE 5 : SELECT sur champs nested (dot-notation)
-- ============================================================================

-- Test 5.1 : Accès nested niveau 1
SELECT name, awards.total FROM actors;

-- Test 5.2 : WHERE sur champ nested
SELECT name, awards.emmys FROM actors WHERE awards.emmys > 0;

-- Test 5.3 : Accès nested niveau 2 (rating.imdb)
SELECT title, rating.imdb, rating.rotten_tomatoes FROM series;

-- Test 5.4 : WHERE sur nested niveau 2
SELECT title, rating.imdb FROM series WHERE rating.imdb > 8.5;

-- Test 5.5 : Accès nested niveau 2 (production.country)
SELECT title, production.country, production.budget_per_episode FROM series;

-- Test 5.6 : WHERE sur nested niveau 2
SELECT title FROM series WHERE production.country = "France";

-- Test 5.7 : Deep nested (3 niveaux) — contact.agent.name
SELECT name, contact.agent.name, contact.social.instagram FROM actors WHERE contact.agent.name IS NOT NULL;

-- Test 5.8 : Champ nested qui n'existe pas chez tout le monde (awards.cesar)
SELECT name, awards.cesar FROM actors WHERE awards.cesar IS NOT NULL;

-- ============================================================================
-- PHASE 6 : CREATE INDEX sur champs nested
-- ============================================================================

-- Test 6.1 : Index sur nested simple
CREATE INDEX ON actors (awards.total);

-- Test 6.2 : Index sur nested niveau 2
CREATE INDEX ON series (rating.imdb);

-- Test 6.3 : Index sur nested production.country
CREATE INDEX ON series (production.country);

-- Vérifier les index
.indexes

-- Test 6.4 : Query qui devrait utiliser l'index nested
SELECT name, awards.total FROM actors WHERE awards.total > 3;

-- Test 6.5 : Query avec index sur rating.imdb
SELECT title FROM series WHERE rating.imdb >= 8.7;

-- ============================================================================
-- PHASE 7 : ORDER BY sur champs nested
-- ============================================================================

-- Test 7.1 : ORDER BY nested
SELECT name, awards.total FROM actors ORDER BY awards.total DESC;

-- Test 7.2 : ORDER BY nested niveau 2
SELECT title, rating.imdb FROM series ORDER BY rating.imdb DESC;

-- Test 7.3 : ORDER BY nested + LIMIT
SELECT title, production.budget_per_episode FROM series ORDER BY production.budget_per_episode DESC LIMIT 3;

-- ============================================================================
-- PHASE 8 : GROUP BY et agrégations sur champs nested
-- ============================================================================

-- Test 8.1 : GROUP BY sur champ nested
SELECT production.country, COUNT(*) FROM series GROUP BY production.country;

-- Test 8.2 : GROUP BY nested + AVG
SELECT production.country, AVG(rating.imdb) FROM series GROUP BY production.country;

-- Test 8.3 : GROUP BY nested + SUM
SELECT production.country, SUM(production.budget_per_episode) FROM series GROUP BY production.country;

-- Test 8.4 : GROUP BY nested + HAVING
SELECT production.country, COUNT(*) FROM series GROUP BY production.country HAVING COUNT(*) > 1;

-- Test 8.5 : GROUP BY sur reviews nested
SELECT series_title, AVG(detail.story), AVG(detail.acting), AVG(detail.visuals) FROM reviews GROUP BY series_title;

-- ============================================================================
-- PHASE 9 : JOIN entre collections sur champs nested
-- ============================================================================

-- Test 9.1 : JOIN simple (clé classique)
SELECT s.title, r.reviewer, r.score FROM series s JOIN reviews r ON s.title = r.series_title;

-- Test 9.2 : JOIN avec accès nested dans le SELECT
SELECT s.title, s.rating.imdb, r.reviewer, r.detail.story FROM series s JOIN reviews r ON s.title = r.series_title;

-- Test 9.3 : JOIN avec WHERE sur champ nested
SELECT s.title, r.reviewer, r.score FROM series s JOIN reviews r ON s.title = r.series_title WHERE s.rating.imdb > 8.5;

-- Test 9.4 : JOIN avec WHERE sur nested des deux côtés
SELECT s.title, r.reviewer FROM series s JOIN reviews r ON s.title = r.series_title WHERE s.rating.imdb > 8.0 AND r.detail.acting >= 9;

-- Test 9.5 : JOIN entre actors et series (champ nested production.country = actors.country)
SELECT a.name, s.title FROM actors a JOIN series s ON a.country = s.production.country;

-- Test 9.6 : LEFT JOIN — séries sans reviews
SELECT s.title, r.reviewer FROM series s LEFT JOIN reviews r ON s.title = r.series_title;

-- Test 9.7 : LEFT JOIN + IS NULL — séries sans aucune review
SELECT s.title FROM series s LEFT JOIN reviews r ON s.title = r.series_title WHERE r.reviewer IS NULL;

-- ============================================================================
-- PHASE 10 : JOIN + GROUP BY + agrégation sur nested
-- ============================================================================

-- Test 10.1 : JOIN + GROUP BY + AVG sur nested
SELECT s.title, AVG(r.detail.story) AS avg_story, AVG(r.detail.acting) AS avg_acting FROM series s JOIN reviews r ON s.title = r.series_title GROUP BY s.title;

-- Test 10.2 : JOIN + GROUP BY sur nested production.country
SELECT s.production.country, COUNT(*), AVG(r.score) FROM series s JOIN reviews r ON s.title = r.series_title GROUP BY s.production.country;

-- Test 10.3 : JOIN + GROUP BY + HAVING sur nested
SELECT s.title, AVG(r.score) AS avg_score FROM series s JOIN reviews r ON s.title = r.series_title GROUP BY s.title HAVING AVG(r.score) > 8.5;

-- ============================================================================
-- PHASE 11 : Subqueries sur champs nested
-- ============================================================================

-- Test 11.1 : Subquery IN simple
SELECT title FROM series WHERE title IN (SELECT series_title FROM reviews WHERE score >= 9);

-- Test 11.2 : Subquery scalaire sur nested
SELECT title, rating.imdb FROM series WHERE rating.imdb > (SELECT AVG(rating.imdb) FROM series);

-- Test 11.3 : Subquery corrélée sur nested
SELECT title, production.budget_per_episode FROM series s WHERE production.budget_per_episode > (SELECT AVG(production.budget_per_episode) FROM series WHERE production.country = s.production.country);

-- Test 11.4 : Subquery dans IN avec champ nested
SELECT name FROM actors WHERE country IN (SELECT production.country FROM series WHERE rating.imdb > 8.5);

-- ============================================================================
-- PHASE 12 : CASE WHEN sur champs nested
-- ============================================================================

-- Test 12.1 : CASE WHEN simple sur nested
SELECT title, CASE WHEN rating.imdb >= 9.0 THEN "masterpiece" WHEN rating.imdb >= 8.0 THEN "excellent" ELSE "good" END AS tier FROM series;

-- Test 12.2 : CASE WHEN sur nested production
SELECT title, CASE WHEN production.budget_per_episode > 10000000 THEN "blockbuster" WHEN production.budget_per_episode > 5000000 THEN "premium" ELSE "standard" END AS budget_tier FROM series;

-- Test 12.3 : CASE WHEN sur nested awards
SELECT name, CASE WHEN awards.total >= 5 THEN "legendary" WHEN awards.total >= 3 THEN "decorated" WHEN awards.total >= 1 THEN "awarded" ELSE "rising star" END AS status FROM actors;

-- Test 12.4 : CASE WHEN sur nested dans un WHERE
SELECT title FROM series WHERE CASE WHEN production.country = "France" THEN rating.rotten_tomatoes ELSE rating.imdb END > 90;

-- ============================================================================
-- PHASE 13 : EXPLAIN sur queries nested
-- ============================================================================

-- Test 13.1 : EXPLAIN basique sur nested WHERE
EXPLAIN SELECT * FROM actors WHERE awards.emmys > 0;

-- Test 13.2 : EXPLAIN avec index nested
EXPLAIN SELECT * FROM series WHERE rating.imdb > 9.0;

-- Test 13.3 : EXPLAIN JOIN avec nested
EXPLAIN SELECT s.title, r.score FROM series s JOIN reviews r ON s.title = r.series_title WHERE s.rating.imdb > 8.5;

-- Test 13.4 : EXPLAIN avec hint FORCE_INDEX sur nested
EXPLAIN SELECT /*+ FORCE_INDEX(rating.imdb) */ title FROM series WHERE rating.imdb > 8.0;

-- Test 13.5 : EXPLAIN avec hint FULL_SCAN
EXPLAIN SELECT /*+ FULL_SCAN */ * FROM actors WHERE awards.total > 3;

-- Test 13.6 : EXPLAIN avec hint HASH_JOIN
EXPLAIN SELECT /*+ HASH_JOIN */ s.title, r.score FROM series s JOIN reviews r ON s.title = r.series_title;

-- ============================================================================
-- PHASE 14 : UPDATE sur champs nested
-- ============================================================================

-- Test 14.1 : UPDATE nested simple
UPDATE actors SET awards.total = 6 WHERE name = "Bryan Cranston";

-- Vérification
SELECT name, awards.total FROM actors WHERE name = "Bryan Cranston";

-- Test 14.2 : UPDATE nested niveau 2
UPDATE series SET rating.imdb = 9.6 WHERE title = "Breaking Bad";

-- Vérification
SELECT title, rating.imdb FROM series WHERE title = "Breaking Bad";

-- Test 14.3 : UPDATE nested avec WHERE nested
UPDATE reviews SET detail.visuals = 10 WHERE detail.story >= 9 AND series_title = "Breaking Bad";

-- Vérification
SELECT * FROM reviews WHERE series_title = "Breaking Bad";

-- Test 14.4 : UPDATE deep nested (3 niveaux)
UPDATE actors SET contact.agent.phone = "+1-555-0200" WHERE name = "Millie Bobby Brown";

-- Vérification
SELECT name, contact.agent.phone FROM actors WHERE name = "Millie Bobby Brown";

-- ============================================================================
-- PHASE 15 : DELETE avec WHERE sur champs nested
-- ============================================================================

-- Test 15.1 : DELETE avec WHERE nested
DELETE FROM reviews WHERE detail.story < 7;

-- Vérification
SELECT * FROM reviews;

-- Test 15.2 : DELETE avec WHERE nested niveau 2
-- (on insère un dummy pour tester)
INSERT INTO series VALUES (title="Test Show", year=2020, seasons=1, rating.imdb=3.0, rating.rotten_tomatoes=20, production.country="Test", production.budget_per_episode=100);

DELETE FROM series WHERE rating.imdb < 5.0;

-- Vérification — Test Show ne doit plus être là
SELECT title, rating.imdb FROM series;

-- ============================================================================
-- PHASE 16 : Wildcard paths sur nested
-- ============================================================================

-- Test 16.1 : Wildcard direct children (awards.*)
SELECT name FROM actors WHERE awards.* > 3;

-- Test 16.2 : Wildcard deep recursive (awards.**)
SELECT name FROM actors WHERE awards.** >= 4;

-- Test 16.3 : Wildcard sur detail.* dans reviews
SELECT series_title, reviewer FROM reviews WHERE detail.* >= 10;

-- ============================================================================
-- PHASE 17 : DISTINCT sur champs nested
-- ============================================================================

-- Test 17.1 : DISTINCT sur nested
SELECT DISTINCT production.country FROM series;

-- Test 17.2 : COUNT DISTINCT sur nested
SELECT COUNT(DISTINCT production.country) FROM series;

-- Test 17.3 : DISTINCT nested dans reviews
SELECT DISTINCT detail.acting FROM reviews ORDER BY detail.acting DESC;

-- ============================================================================
-- PHASE 18 : LIKE sur champs nested (si applicable)
-- ============================================================================

-- Test 18.1 : LIKE sur nested string
SELECT name, contact.agent.name FROM actors WHERE contact.agent.name LIKE "W%";

-- ============================================================================
-- PHASE 19 : Combinaisons complexes
-- ============================================================================

-- Test 19.1 : JOIN + CASE WHEN + nested + ORDER BY
SELECT s.title, s.rating.imdb, CASE WHEN s.production.budget_per_episode > 10000000 THEN "mega" ELSE "normal" END AS scale, AVG(r.score) AS avg FROM series s JOIN reviews r ON s.title = r.series_title GROUP BY s.title ORDER BY avg DESC;

-- Test 19.2 : Subquery + nested + CASE WHEN
SELECT title, CASE WHEN rating.imdb > (SELECT AVG(rating.imdb) FROM series) THEN "above average" ELSE "below average" END AS vs_avg FROM series;

-- Test 19.3 : Arithmétique sur nested
SELECT title, production.budget_per_episode * seasons AS total_budget FROM series ORDER BY total_budget DESC;

-- Test 19.4 : Comparaison entre champs nested
SELECT series_title, reviewer FROM reviews WHERE detail.acting > detail.story;

-- ============================================================================
-- PHASE 20 : .schema et .dump finaux
-- ============================================================================

.schema

.indexes

.cache

-- ============================================================================
-- FIN — Résumé des tests
-- ============================================================================
-- Total: ~60 tests couvrant:
--   - INSERT dot-notation (simple, deep)
--   - INSERT JSON (objets, arrays, nested arrays d'objets)
--   - SELECT sur nested (1, 2, 3 niveaux)
--   - CREATE INDEX sur nested
--   - ORDER BY nested
--   - GROUP BY + agrégations (COUNT, AVG, SUM) sur nested
--   - JOIN (INNER, LEFT) avec nested dans SELECT, WHERE, ON
--   - JOIN + GROUP BY + HAVING sur nested
--   - Subqueries (IN, scalaire, corrélée) sur nested
--   - CASE WHEN sur nested
--   - EXPLAIN + hints sur nested
--   - UPDATE nested (1, 2, 3 niveaux)
--   - DELETE avec WHERE nested
--   - Wildcard paths (*, **)
--   - DISTINCT / COUNT DISTINCT sur nested
--   - LIKE sur nested
--   - Combinaisons complexes (JOIN + CASE + ORDER + nested)
--   - Arithmétique sur nested
--   - Comparaison entre champs nested
-- ============================================================================
