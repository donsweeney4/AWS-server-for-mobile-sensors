-- This utility SQL script creates a table named 'campaign_locations' with the following columns:

-- To run this script:  mysql -u uhi -p uhi < createtable.sql
-- Example insert line: INSERT INTO campaign_locations (campaign_id, description, owners, date) VALUES ('Liv-2024-09-03', 'Heat map of Livermore on Sept 3, 2024', 'Don Sweeney', '2024-09-03');

USE uhi;
DROP TABLE IF EXISTS campaign_locations;
CREATE TABLE campaign_locations (
    campaign_id VARCHAR(20) NOT NULL,
    description TEXT NOT NULL,
    owners TEXT NOT NULL,
    date DATE NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (campaign_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

