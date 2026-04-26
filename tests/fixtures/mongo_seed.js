// Seed data for MongoDB CDC tests.
// Runs inside the mongo container via docker-entrypoint-initdb.d BEFORE replica set init,
// so only plain inserts here (no change streams yet).

db = db.getSiblingDB('testdb');

db.customers.insertMany([
  { name: 'Alice',   email: 'alice@example.com',   score: 10.0 },
  { name: 'Bob',     email: 'bob@example.com',     score: 20.0 },
  { name: 'Charlie', email: 'charlie@example.com', score: 30.0 },
]);

db.orders.insertMany([
  { customer: 'Alice',   amount: 99.99,  status: 'completed' },
  { customer: 'Bob',     amount: 149.50, status: 'pending'   },
  { customer: 'Alice',   amount: 25.00,  status: 'completed' },
]);
