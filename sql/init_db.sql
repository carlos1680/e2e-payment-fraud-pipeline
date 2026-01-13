CREATE DATABASE IF NOT EXISTS fraud_db;
USE fraud_db;

CREATE TABLE IF NOT EXISTS blacklist_users (
    user_id VARCHAR(50) PRIMARY KEY,
    reason VARCHAR(255),
    severity VARCHAR(20),
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert some "bad actors" for our demo
INSERT INTO blacklist_users (user_id, reason, severity) VALUES 
('user_5', 'High chargeback rate', 'HIGH'),
('user_12', 'Suspicious login pattern', 'MEDIUM');