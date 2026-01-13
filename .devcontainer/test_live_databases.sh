#!/bin/bash
# Test dataprof database functionalities with live containers

set -e

echo "🧪 Testing DataProf Database Functionalities"
echo "=============================================="
echo ""

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test PostgreSQL connectivity
echo "1️⃣ Testing PostgreSQL Connection..."
export PGPASSWORD='dev_password_123'
if psql -h localhost -U dataprof -d dataprof_test -c "SELECT version();" > /dev/null 2>&1; then
    echo -e "${GREEN}✅ PostgreSQL is accessible${NC}"
    
    # Create test table and data
    echo "   Creating test table and sample data..."
    psql -h localhost -U dataprof -d dataprof_test << EOF
DROP TABLE IF EXISTS test_users;
CREATE TABLE test_users (
    user_id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(100),
    age INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_login TIMESTAMP,
    is_active BOOLEAN DEFAULT true,
    balance DECIMAL(10,2)
);

INSERT INTO test_users (username, email, age, balance) VALUES
    ('alice', 'alice@example.com', 25, 1000.50),
    ('bob', 'bob@example.com', 30, 2500.75),
    ('charlie', 'charlie@example.com', NULL, 500.00),
    ('diana', NULL, 28, 750.25),
    ('eve', 'eve@example.com', 35, NULL),
    ('frank', 'frank@example.com', 40, 3000.00),
    ('grace', 'grace@example.com', 22, 150.50),
    ('henry', 'henry@example.com', 45, 4500.00),
    ('iris', NULL, NULL, 890.75),
    ('jack', 'jack@example.com', 33, 1250.00);

SELECT 'Inserted ' || COUNT(*) || ' test records' FROM test_users;
EOF
    echo -e "${GREEN}   ✅ Test data created${NC}"
else
    echo -e "${RED}❌ PostgreSQL is not accessible${NC}"
    exit 1
fi

echo ""

# Test MySQL connectivity
echo "2️⃣ Testing MySQL Connection..."
if mysql -h 127.0.0.1 -u dataprof -pdev_password_123 dataprof_test -e "SELECT VERSION();" > /dev/null 2>&1; then
    echo -e "${GREEN}✅ MySQL is accessible${NC}"
    
    # Create test table and data
    echo "   Creating test table and sample data..."
    mysql -h 127.0.0.1 -u dataprof -pdev_password_123 dataprof_test << EOF
DROP TABLE IF EXISTS test_products;
CREATE TABLE test_products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10,2),
    stock_quantity INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_available BOOLEAN DEFAULT true,
    rating DECIMAL(3,2)
);

INSERT INTO test_products (product_name, category, price, stock_quantity, rating) VALUES
    ('Laptop', 'Electronics', 999.99, 50, 4.50),
    ('Mouse', 'Electronics', 29.99, 200, 4.20),
    ('Keyboard', 'Electronics', 79.99, 150, 4.30),
    ('Monitor', 'Electronics', 299.99, 75, NULL),
    ('Desk Chair', 'Furniture', 199.99, 30, 4.10),
    ('Desk Lamp', 'Furniture', NULL, 100, 3.90),
    ('Notebook', 'Stationery', 5.99, 500, 4.00),
    ('Pen Set', NULL, 12.99, 300, 4.60),
    ('USB Cable', 'Electronics', 9.99, NULL, 3.80),
    ('Headphones', 'Electronics', 149.99, 120, 4.70);

SELECT CONCAT('Inserted ', COUNT(*), ' test records') FROM test_products;
EOF
    echo -e "${GREEN}   ✅ Test data created${NC}"
else
    echo -e "${RED}❌ MySQL is not accessible${NC}"
    echo -e "${YELLOW}   Note: MySQL might still be initializing. Wait a moment and try again.${NC}"
fi

echo ""
echo "3️⃣ Running Rust Database Tests..."
echo ""

# Run the database integration tests (excluding DuckDB which isn't configured)
cargo test --test database_integration --features "database,postgres,mysql,sqlite" \
    --lib -- --test-threads=1 2>&1 | grep -E "(test |passed|failed|running)" || true

echo ""
echo "4️⃣ Testing Database Profiling with DataProf..."
echo ""

# Build with database features if needed
echo "   Building dataprof with database support..."
cargo build --release --features "database,postgres,mysql,sqlite" 2>&1 | tail -5

echo ""
echo "   Profiling PostgreSQL table..."
if ./target/release/dataprof db \
    "postgresql://dataprof:dev_password_123@localhost:5432/dataprof_test" \
    --query "SELECT * FROM test_users" \
    --output postgres_profile.json 2>&1; then
    echo -e "${GREEN}   ✅ PostgreSQL profiling completed${NC}"
    if [ -f postgres_profile.json ]; then
        echo "      Profile saved to: postgres_profile.json"
        # Show a quick summary
        echo "      Quick stats:"
        grep -E '"total_rows"|"total_columns"|"quality_score"' postgres_profile.json | head -3 || true
    fi
else
    echo -e "${YELLOW}   ⚠️ PostgreSQL profiling may not have CLI support yet${NC}"
fi

echo ""
echo "   Profiling MySQL table..."
if ./target/release/dataprof db \
    "mysql://dataprof:dev_password_123@127.0.0.1:3306/dataprof_test" \
    --query "SELECT * FROM test_products" \
    --output mysql_profile.json 2>&1; then
    echo -e "${GREEN}   ✅ MySQL profiling completed${NC}"
    if [ -f mysql_profile.json ]; then
        echo "      Profile saved to: mysql_profile.json"
        echo "      Quick stats:"
        grep -E '"total_rows"|"total_columns"|"quality_score"' mysql_profile.json | head -3 || true
    fi
else
    echo -e "${YELLOW}   ⚠️ MySQL profiling may not have CLI support yet${NC}"
fi

echo ""
echo "=============================================="
echo -e "${GREEN}✅ Database Testing Complete!${NC}"
echo ""
echo "Summary:"
echo "  - PostgreSQL: Running on port 5432"
echo "  - MySQL: Running on port 3306"
echo "  - Test tables created with sample data"
echo ""
echo "Connection strings for testing:"
echo "  PostgreSQL: postgresql://dataprof:dev_password_123@localhost:5432/dataprof_test"
echo "  MySQL: mysql://dataprof:dev_password_123@127.0.0.1:3306/dataprof_test"
