-- Grant binlog replication privileges
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;

USE testdb;

CREATE TABLE IF NOT EXISTS customers (
  id         INT AUTO_INCREMENT PRIMARY KEY,
  name       VARCHAR(255) NOT NULL,
  email      VARCHAR(255),
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO customers (name, email) VALUES
  ('Alice',   'alice@example.com'),
  ('Bob',     'bob@example.com'),
  ('Charlie', 'charlie@example.com');

CREATE TABLE IF NOT EXISTS orders (
  id          INT AUTO_INCREMENT PRIMARY KEY,
  customer_id INT,
  amount      DECIMAL(10,2),
  status      VARCHAR(50) DEFAULT 'pending',
  created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (customer_id) REFERENCES customers(id)
);
INSERT INTO orders (customer_id, amount, status) VALUES
  (1, 99.99,  'completed'),
  (2, 149.50, 'pending'),
  (1, 25.00,  'completed');
