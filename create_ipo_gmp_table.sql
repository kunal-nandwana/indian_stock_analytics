-- Create IPO GMP table to store live IPO Grey Market Premium data

CREATE TABLE IF NOT EXISTS bronze.ipo_gmp (
    id SERIAL PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL,
    status VARCHAR(50),  -- Upcoming, Open, Close, Listed
    gmp_value NUMERIC(10, 2),  -- Grey Market Premium value in ₹
    gmp_percentage NUMERIC(10, 2),  -- GMP as percentage
    rating INTEGER,  -- Fire emoji count (0-4)
    subscription_times NUMERIC(10, 2),  -- Subscription times (e.g., 52.98x)
    gmp_low NUMERIC(10, 2),  -- GMP low range
    gmp_high NUMERIC(10, 2),  -- GMP high range
    ipo_price NUMERIC(10, 2),  -- IPO price
    ipo_size_cr NUMERIC(10, 2),  -- IPO size in crores
    lot_size INTEGER,  -- Lot size
    open_date DATE,  -- Opening date
    close_date DATE,  -- Closing date
    boa_date DATE,  -- Basis of Allotment date
    listing_date DATE,  -- Listing date
    updated_on TIMESTAMP,  -- Last update timestamp from website
    has_anchor BOOLEAN,  -- Anchor investor status
    scraped_at TIMESTAMP NOT NULL,  -- When we scraped this data
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Add index on company_name and scraped_at for efficient queries
    CONSTRAINT unique_ipo_scrape UNIQUE (company_name, scraped_at)
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_ipo_gmp_company ON bronze.ipo_gmp(company_name);
CREATE INDEX IF NOT EXISTS idx_ipo_gmp_status ON bronze.ipo_gmp(status);
CREATE INDEX IF NOT EXISTS idx_ipo_gmp_scraped_at ON bronze.ipo_gmp(scraped_at);
CREATE INDEX IF NOT EXISTS idx_ipo_gmp_listing_date ON bronze.ipo_gmp(listing_date);

-- Add comment
COMMENT ON TABLE bronze.ipo_gmp IS 'Live IPO Grey Market Premium data scraped from InvestorGain.com';
