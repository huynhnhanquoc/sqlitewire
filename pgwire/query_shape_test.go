package pgwire

import "testing"

func TestReadQueryRecognizesReturningWithoutMistakingColumnName(t *testing.T) {
	if !isReadQuery(`WITH chosen AS (SELECT 1) SELECT * FROM chosen`) {
		t.Fatal("WITH SELECT was not recognized as a row-producing query")
	}
	for _, source := range []string{
		`UPDATE products SET title = 'changed' RETURNING id`,
		`DELETE FROM products WHERE id = 1 RETURNING *`,
		`INSERT INTO products (id) VALUES (1) RETURNING id`,
		`insert into products (id) values (1) returning id`,
		`INSERT INTO products DEFAULT VALUES RETURNING id`,
		`INSERT INTO products (id) VALUES (1) ON CONFLICT DO NOTHING RETURNING *`,
		`INSERT INTO products (id) VALUES (1) ON CONFLICT (id) DO UPDATE SET title = excluded.title RETURNING id`,
		`UPDATE products SET title = 'from' RETURNING id`,
		`UPDATE products SET price = (SELECT 1) RETURNING price`,
		`UPDATE products SET title = $1 WHERE id = $2 RETURNING *`,
		`DELETE FROM products WHERE title IS NULL RETURNING id`,
		`DELETE FROM products RETURNING id, title AS returning`,
		`UPDATE returning SET title = 'x' RETURNING title`,
		`DELETE FROM "from" RETURNING id`,
	} {
		if !isReadQuery(source) {
			t.Fatalf("row-producing query not recognized: %s", source)
		}
	}
	for _, source := range []string{
		`UPDATE products SET "returning" = 'value' WHERE id = 1`,
		`INSERT INTO products ("returning") VALUES ('value')`,
		`DELETE FROM returning WHERE id = 1`,
		`UPDATE products SET title = 'returning' WHERE id = 1`,
		"DELETE FROM products -- returning\nWHERE id = 1",
		`DELETE FROM products /* returning */ WHERE id = 1`,
		`INSERT INTO products SELECT 1 AS returning`,
		`UPDATE products SET returning = 1`,
		`DELETE FROM products WHERE returning = 1`,
		`DELETE FROM shop.returning WHERE id = 1`,
		`INSERT INTO products VALUES ((SELECT 1 AS returning))`,
		`DELETE FROM products USING returning WHERE products.id = returning.id`,
		`UPDATE products SET title = 'x' WHERE id = 1 ~ 2`,
		`INSERT INTO products (returning) VALUES (1)`,
		`UPDATE products SET title = CASE WHEN id = 1 THEN returning.title END FROM returning`,
	} {
		if isReadQuery(source) {
			t.Fatalf("non-row query was mistaken for RETURNING: %s", source)
		}
	}
}
