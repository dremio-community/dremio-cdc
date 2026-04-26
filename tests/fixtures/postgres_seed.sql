CREATE TABLE IF NOT EXISTS customers (
  id         SERIAL PRIMARY KEY,
  name       TEXT NOT NULL,
  email      TEXT,
  created_at TIMESTAMP DEFAULT NOW()
);
INSERT INTO customers (name, email) VALUES
  ('Alice',   'alice@example.com'),
  ('Bob',     'bob@example.com'),
  ('Charlie', 'charlie@example.com');

CREATE TABLE IF NOT EXISTS orders (
  id          SERIAL PRIMARY KEY,
  customer_id INT REFERENCES customers(id),
  amount      NUMERIC(10,2),
  status      TEXT DEFAULT 'pending',
  created_at  TIMESTAMP DEFAULT NOW()
);
INSERT INTO orders (customer_id, amount, status) VALUES
  (1, 99.99,  'completed'),
  (2, 149.50, 'pending'),
  (1, 25.00,  'completed');
