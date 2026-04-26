-- Enable CDC at the database level
EXEC sys.sp_cdc_enable_db;
GO

-- Create tables
CREATE TABLE customers (
  id         INT IDENTITY(1,1) PRIMARY KEY,
  name       NVARCHAR(255) NOT NULL,
  email      NVARCHAR(255),
  created_at DATETIME2 DEFAULT GETUTCDATE()
);
INSERT INTO customers (name, email) VALUES
  ('Alice',   'alice@example.com'),
  ('Bob',     'bob@example.com'),
  ('Charlie', 'charlie@example.com');

CREATE TABLE orders (
  id          INT IDENTITY(1,1) PRIMARY KEY,
  customer_id INT REFERENCES customers(id),
  amount      DECIMAL(10,2),
  status      NVARCHAR(50) DEFAULT 'pending',
  created_at  DATETIME2 DEFAULT GETUTCDATE()
);
INSERT INTO orders (customer_id, amount, status) VALUES
  (1, 99.99,  'completed'),
  (2, 149.50, 'pending'),
  (1, 25.00,  'completed');
GO

-- Enable CDC on each table
EXEC sys.sp_cdc_enable_table
  @source_schema = 'dbo',
  @source_name   = 'customers',
  @role_name     = NULL;
GO

EXEC sys.sp_cdc_enable_table
  @source_schema = 'dbo',
  @source_name   = 'orders',
  @role_name     = NULL;
GO
