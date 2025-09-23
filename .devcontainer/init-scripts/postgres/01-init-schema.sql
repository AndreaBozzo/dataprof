-- PostgreSQL initialization script for DataProfiler development
-- This script sets up the test database schema and sample data

-- Create database if not exists (handled by POSTGRES_DB environment variable)

-- Create test schemas
CREATE SCHEMA IF NOT EXISTS dataprof_core;
CREATE SCHEMA IF NOT EXISTS dataprof_test;

-- Grant permissions to dataprof user
GRANT ALL PRIVILEGES ON SCHEMA dataprof_core TO dataprof;
GRANT ALL PRIVILEGES ON SCHEMA dataprof_test TO dataprof;

-- Create test tables for CSV-like data analysis
CREATE TABLE IF NOT EXISTS dataprof_test.sample_data (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(255),
    age INTEGER,
    salary DECIMAL(10,2),
    department VARCHAR(50),
    hire_date DATE,
    is_active BOOLEAN,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create test table with various data types
CREATE TABLE IF NOT EXISTS dataprof_test.data_types_test (
    id BIGSERIAL PRIMARY KEY,
    text_col TEXT,
    varchar_col VARCHAR(255),
    char_col CHAR(10),
    int_col INTEGER,
    bigint_col BIGINT,
    smallint_col SMALLINT,
    decimal_col DECIMAL(15,5),
    numeric_col NUMERIC(20,10),
    real_col REAL,
    double_col DOUBLE PRECISION,
    boolean_col BOOLEAN,
    date_col DATE,
    time_col TIME,
    timestamp_col TIMESTAMP,
    timestamptz_col TIMESTAMP WITH TIME ZONE,
    interval_col INTERVAL,
    uuid_col UUID,
    json_col JSON,
    jsonb_col JSONB,
    array_col INTEGER[],
    bytea_col BYTEA
);

-- Create indexes for performance testing
CREATE INDEX IF NOT EXISTS idx_sample_data_department ON dataprof_test.sample_data(department);
CREATE INDEX IF NOT EXISTS idx_sample_data_hire_date ON dataprof_test.sample_data(hire_date);
CREATE INDEX IF NOT EXISTS idx_sample_data_email ON dataprof_test.sample_data(email);

-- Insert sample test data
INSERT INTO dataprof_test.sample_data (name, email, age, salary, department, hire_date, is_active, metadata) VALUES
('John Doe', 'john.doe@example.com', 30, 75000.00, 'Engineering', '2020-01-15', TRUE, '{"skills": ["Python", "Rust"], "level": "senior"}'),
('Jane Smith', 'jane.smith@example.com', 28, 68000.50, 'Engineering', '2021-03-10', TRUE, '{"skills": ["JavaScript", "React"], "level": "mid"}'),
('Bob Johnson', 'bob.johnson@example.com', 45, 95000.00, 'Management', '2015-07-22', TRUE, '{"skills": ["Leadership"], "level": "executive"}'),
('Alice Brown', 'alice.brown@example.com', 32, 72000.75, 'Design', '2019-11-08', FALSE, '{"skills": ["UI/UX", "Figma"], "level": "senior"}'),
('Charlie Wilson', 'charlie.wilson@example.com', 26, 58000.00, 'Marketing', '2022-02-14', TRUE, '{"skills": ["SEO", "Analytics"], "level": "junior"}'),
('Diana Davis', NULL, 29, 71000.25, 'Engineering', '2020-09-30', TRUE, NULL),
('Eva Martinez', 'eva.martinez@invalid', -5, -1000.00, '', '1900-01-01', NULL, '{"invalid": "data"}'),
('Frank Lee', 'frank.lee@example.com', 150, 999999.99, 'Engineering', '2030-12-31', TRUE, '{"skills": [], "level": ""}');

-- Insert test data with various data types
INSERT INTO dataprof_test.data_types_test (
    text_col, varchar_col, char_col, int_col, bigint_col, smallint_col,
    decimal_col, numeric_col, real_col, double_col, boolean_col,
    date_col, time_col, timestamp_col, timestamptz_col, interval_col,
    uuid_col, json_col, jsonb_col, array_col, bytea_col
) VALUES
(
    'Sample text with special characters: àáâãäåæçèéêë',
    'VARCHAR sample',
    'CHAR      ',
    42,
    9223372036854775807,
    32767,
    12345.67890,
    98765.4321012345,
    3.14159,
    2.718281828459045,
    TRUE,
    '2023-12-25',
    '14:30:00',
    '2023-12-25 14:30:00',
    '2023-12-25 14:30:00+00',
    '1 year 2 months 3 days 4 hours 5 minutes 6 seconds',
    gen_random_uuid(),
    '{"key": "value", "number": 123, "array": [1, 2, 3]}',
    '{"indexed": true, "searchable": "yes"}',
    ARRAY[1, 2, 3, 4, 5],
    decode('48656c6c6f20576f726c64', 'hex')
),
(
    NULL,
    '',
    NULL,
    0,
    -9223372036854775808,
    -32768,
    0.00000,
    -99999.9999999999,
    0.0,
    -1.7976931348623157e+308,
    FALSE,
    '1970-01-01',
    '00:00:00',
    '1970-01-01 00:00:00',
    '1970-01-01 00:00:00+00',
    '0 seconds',
    gen_random_uuid(),
    '{}',
    '{}',
    ARRAY[]::INTEGER[],
    NULL
);

-- Create a view for testing view queries
CREATE OR replace VIEW dataprof_test.employee_summary AS
SELECT
    department,
    COUNT(*) as employee_count,
    AVG(age) as avg_age,
    AVG(salary) as avg_salary,
    MIN(hire_date) as earliest_hire,
    MAX(hire_date) as latest_hire
FROM dataprof_test.sample_data
WHERE is_active = TRUE
GROUP BY department;

-- Create stored procedure for testing
CREATE OR REPLACE FUNCTION dataprof_test.get_department_stats(dept_name VARCHAR)
RETURNS TABLE(
    total_employees BIGINT,
    avg_salary NUMERIC,
    avg_age NUMERIC
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        COUNT(*)::BIGINT,
        AVG(salary),
        AVG(age)
    FROM dataprof_test.sample_data
    WHERE department = dept_name AND is_active = TRUE;
END;
$$ LANGUAGE plpgsql;

-- Grant all permissions on the created objects
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA dataprof_test TO dataprof;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA dataprof_test TO dataprof;
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA dataprof_test TO dataprof;

-- Create test user with limited permissions for testing access controls
CREATE USER dataprof_readonly WITH PASSWORD 'readonly_pass';
GRANT CONNECT ON DATABASE dataprof_test TO dataprof_readonly;
GRANT USAGE ON SCHEMA dataprof_test TO dataprof_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA dataprof_test TO dataprof_readonly;

COMMIT;