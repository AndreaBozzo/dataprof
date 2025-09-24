-- MySQL initialization script for DataProfiler development
-- This script sets up the test database schema and sample data

-- Use the dataprof_test database
USE dataprof_test;

-- Create test tables for CSV-like data analysis
CREATE TABLE IF NOT EXISTS sample_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(255),
    age INT,
    salary DECIMAL(10,2),
    department VARCHAR(50),
    hire_date DATE,
    is_active BOOLEAN,
    metadata JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Create test table with various MySQL data types
CREATE TABLE IF NOT EXISTS data_types_test (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    text_col TEXT,
    varchar_col VARCHAR(255),
    char_col CHAR(10),
    int_col INT,
    bigint_col BIGINT,
    smallint_col SMALLINT,
    decimal_col DECIMAL(15,5),
    numeric_col NUMERIC(20,10),
    float_col FLOAT,
    double_col DOUBLE,
    boolean_col BOOLEAN,
    date_col DATE,
    time_col TIME,
    datetime_col DATETIME,
    timestamp_col TIMESTAMP,
    year_col YEAR,
    binary_col BINARY(16),
    varbinary_col VARBINARY(255),
    blob_col BLOB,
    json_col JSON,
    enum_col ENUM('small', 'medium', 'large'),
    set_col SET('read', 'write', 'execute')
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Create indexes for performance testing
CREATE INDEX idx_sample_data_department ON sample_data(department);
CREATE INDEX idx_sample_data_hire_date ON sample_data(hire_date);
CREATE INDEX idx_sample_data_email ON sample_data(email);

-- Insert sample test data
INSERT INTO sample_data (name, email, age, salary, department, hire_date, is_active, metadata) VALUES
('John Doe', 'john.doe@example.com', 30, 75000.00, 'Engineering', '2020-01-15', TRUE, '{"skills": ["Python", "Rust"], "level": "senior"}'),
('Jane Smith', 'jane.smith@example.com', 28, 68000.50, 'Engineering', '2021-03-10', TRUE, '{"skills": ["JavaScript", "React"], "level": "mid"}'),
('Bob Johnson', 'bob.johnson@example.com', 45, 95000.00, 'Management', '2015-07-22', TRUE, '{"skills": ["Leadership"], "level": "executive"}'),
('Alice Brown', 'alice.brown@example.com', 32, 72000.75, 'Design', '2019-11-08', FALSE, '{"skills": ["UI/UX", "Figma"], "level": "senior"}'),
('Charlie Wilson', 'charlie.wilson@example.com', 26, 58000.00, 'Marketing', '2022-02-14', TRUE, '{"skills": ["SEO", "Analytics"], "level": "junior"}'),
('Diana Davis', NULL, 29, 71000.25, 'Engineering', '2020-09-30', TRUE, NULL),
('Eva Martinez', 'eva.martinez@invalid', -5, -1000.00, '', '1900-01-01', NULL, '{"invalid": "data"}'),
('Frank Lee', 'frank.lee@example.com', 150, 999999.99, 'Engineering', '2030-12-31', TRUE, '{"skills": [], "level": ""}');

-- Insert test data with various data types
INSERT INTO data_types_test (
    text_col, varchar_col, char_col, int_col, bigint_col, smallint_col,
    decimal_col, numeric_col, float_col, double_col, boolean_col,
    date_col, time_col, datetime_col, timestamp_col, year_col,
    binary_col, varbinary_col, blob_col, json_col, enum_col, set_col
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
    '2023-12-25 14:30:00',
    2023,
    UNHEX('48656C6C6F20576F726C6400000000'),
    UNHEX('48656C6C6F'),
    'Binary blob data',
    '{"key": "value", "number": 123, "array": [1, 2, 3]}',
    'large',
    'read,write'
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
    '1970-01-01 00:00:01',
    1970,
    NULL,
    NULL,
    NULL,
    '{}',
    'small',
    'execute'
);

-- Create a view for testing view queries
CREATE OR REPLACE VIEW employee_summary AS
SELECT
    department,
    COUNT(*) as employee_count,
    AVG(age) as avg_age,
    AVG(salary) as avg_salary,
    MIN(hire_date) as earliest_hire,
    MAX(hire_date) as latest_hire
FROM sample_data
WHERE is_active = TRUE
GROUP BY department;

-- Create stored procedure for testing
DELIMITER //
CREATE PROCEDURE GetDepartmentStats(IN dept_name VARCHAR(50))
BEGIN
    SELECT
        COUNT(*) as total_employees,
        AVG(salary) as avg_salary,
        AVG(age) as avg_age
    FROM sample_data
    WHERE department = dept_name AND is_active = TRUE;
END //
DELIMITER ;

-- Create test user with limited permissions for testing access controls
CREATE USER IF NOT EXISTS 'dataprof_readonly'@'%' IDENTIFIED BY 'readonly_pass';
GRANT SELECT ON dataprof_test.* TO 'dataprof_readonly'@'%';

-- Grant permissions to main user
GRANT ALL PRIVILEGES ON dataprof_test.* TO 'dataprof'@'%';

FLUSH PRIVILEGES;
