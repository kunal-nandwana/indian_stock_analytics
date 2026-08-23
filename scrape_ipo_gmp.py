#!/usr/bin/env python3
"""
IPO GMP (Grey Market Premium) Scraper

Scrapes live IPO GMP data from InvestorGain.com and stores it in PostgreSQL.
Data includes IPO company details, GMP pricing, subscription rates, and key dates.
"""

import os
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import sqlalchemy
from sqlalchemy import text
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scrape_ipo_gmp.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class IPOGMPScraper:
    def __init__(self, schema='bronze'):
        self.url = "https://www.investorgain.com/report/live-ipo-gmp/331/nonzero/"
        self.schema = schema
        self.engine = self._create_engine()
        
    def _create_engine(self):
        """Create PostgreSQL engine"""
        PG_USER = os.environ.get('DATABASE_USER', 'kunal.nandwana')
        PG_PASS = os.environ.get('DATABASE_PASSWORD', 'root')
        PG_HOST = os.environ.get('DATABASE_HOST', 'localhost')
        PG_PORT = os.environ.get('DATABASE_PORT', '5432')
        PG_DB = os.environ.get('DATABASE_NAME', 'kunal.nandwana')
        
        if 'DATABASE_URL' in os.environ:
            connection_string = os.environ['DATABASE_URL']
        else:
            connection_string = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
        
        logger.info(f"Connecting to database: {PG_HOST}:{PG_PORT} as {PG_USER}")
        return sqlalchemy.create_engine(connection_string)
    
    def _parse_status(self, name_text):
        """Extract company name and status from name column"""
        # Status indicators: U=Upcoming, O=Open, C=Close, L=Listed
        status_map = {
            'U': 'Upcoming',
            'O': 'Open', 
            'C': 'Close',
            'L': 'Listed'
        }
        
        # Check for status at the end
        match = re.search(r'\s+([UOCL])$', name_text)
        if match:
            status_code = match.group(1)
            company_name = name_text[:match.start()].strip()
            status = status_map.get(status_code, 'Unknown')
        else:
            company_name = name_text.strip()
            status = 'Unknown'
        
        # Remove "IPO" from company name if present
        company_name = re.sub(r'\s+IPO\s*', ' ', company_name).strip()
        
        return company_name, status
    
    def _parse_gmp(self, gmp_text):
        """Parse GMP value and percentage"""
        # Example: "₹58 (10.05%)" or "₹-- (0.00%)"
        gmp_value = None
        gmp_percentage = None
        
        if gmp_text and '₹' in gmp_text:
            # Extract value
            value_match = re.search(r'₹([\d,\.]+|-+)', gmp_text)
            if value_match:
                value_str = value_match.group(1)
                if value_str != '--' and value_str != '-':
                    try:
                        gmp_value = float(value_str.replace(',', ''))
                    except:
                        pass
            
            # Extract percentage
            pct_match = re.search(r'\(([-\d\.]+)%?\)', gmp_text)
            if pct_match:
                try:
                    gmp_percentage = float(pct_match.group(1))
                except:
                    pass
        
        return gmp_value, gmp_percentage
    
    def _parse_rating(self, rating_text):
        """Count fire emojis for rating"""
        if rating_text:
            return rating_text.count('🔥')
        return 0
    
    def _parse_subscription(self, sub_text):
        """Parse subscription times (e.g., "52.98x")"""
        if sub_text and 'x' in sub_text.lower():
            try:
                return float(sub_text.lower().replace('x', '').strip())
            except:
                pass
        return None
    
    def _parse_gmp_range(self, range_text):
        """Parse GMP low/high range (e.g., "23 ↓ / 62 ↑")"""
        gmp_low = None
        gmp_high = None
        
        if range_text and '/' in range_text:
            parts = range_text.split('/')
            
            # Low value (before /)
            low_match = re.search(r'([\d\.]+)', parts[0])
            if low_match:
                try:
                    gmp_low = float(low_match.group(1))
                except:
                    pass
            
            # High value (after /)
            if len(parts) > 1:
                high_match = re.search(r'([\d\.]+)', parts[1])
                if high_match:
                    try:
                        gmp_high = float(high_match.group(1))
                    except:
                        pass
        
        return gmp_low, gmp_high
    
    def _parse_price(self, price_text):
        """Parse IPO price"""
        if price_text:
            try:
                # Remove commas and extract number
                clean_price = re.sub(r'[^\d\.]', '', price_text)
                return float(clean_price) if clean_price else None
            except:
                pass
        return None
    
    def _parse_size(self, size_text):
        """Parse IPO size in crores"""
        if size_text:
            try:
                # Remove commas and extract number
                clean_size = size_text.replace(',', '').strip()
                return float(clean_size) if clean_size else None
            except:
                pass
        return None
    
    def _parse_date(self, date_text):
        """Parse date in DD-MMM format to date object"""
        if not date_text or date_text.strip() == '-':
            return None
        
        try:
            # Example: "21-Nov"
            date_str = date_text.strip()
            # Add current year
            current_year = datetime.now().year
            full_date_str = f"{date_str}-{current_year}"
            return datetime.strptime(full_date_str, '%d-%b-%Y').date()
        except:
            return None
    
    def _parse_updated_on(self, updated_text):
        """Parse updated timestamp"""
        if not updated_text or updated_text.strip() == '-':
            return None
        
        try:
            # Example: "20-Nov 17:56"
            current_year = datetime.now().year
            full_datetime_str = f"{updated_text}-{current_year}"
            return datetime.strptime(full_datetime_str, '%d-%b %H:%M-%Y')
        except:
            return None
    
    def _parse_anchor(self, anchor_text):
        """Parse anchor investor status"""
        if anchor_text:
            return '✅' in anchor_text or 'yes' in anchor_text.lower()
        return False
    
    def scrape_ipo_data(self):
        """Scrape IPO GMP data from the website"""
        logger.info(f"Fetching data from {self.url}")
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            }
            response = requests.get(self.url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the table
            table = soup.find('table')
            if not table:
                logger.error("Could not find IPO table on the page")
                return None
            
            # Extract table data
            rows = table.find_all('tr')
            data = []
            
            for row in rows[1:]:  # Skip header row
                cols = row.find_all('td')
                if len(cols) >= 14:  # Ensure we have all columns
                    
                    # Parse each column
                    name_text = cols[0].get_text(strip=True)
                    company_name, status = self._parse_status(name_text)
                    
                    gmp_text = cols[1].get_text(strip=True)
                    gmp_value, gmp_percentage = self._parse_gmp(gmp_text)
                    
                    rating = self._parse_rating(cols[2].get_text(strip=True))
                    subscription = self._parse_subscription(cols[3].get_text(strip=True))
                    
                    range_text = cols[4].get_text(strip=True)
                    gmp_low, gmp_high = self._parse_gmp_range(range_text)
                    
                    price = self._parse_price(cols[5].get_text(strip=True))
                    ipo_size = self._parse_size(cols[6].get_text(strip=True))
                    lot_size = int(cols[7].get_text(strip=True)) if cols[7].get_text(strip=True).isdigit() else None
                    
                    open_date = self._parse_date(cols[8].get_text(strip=True))
                    close_date = self._parse_date(cols[9].get_text(strip=True))
                    boa_date = self._parse_date(cols[10].get_text(strip=True))
                    listing_date = self._parse_date(cols[11].get_text(strip=True))
                    
                    updated_on = self._parse_updated_on(cols[12].get_text(strip=True))
                    anchor = self._parse_anchor(cols[13].get_text(strip=True))
                    
                    data.append({
                        'company_name': company_name,
                        'status': status,
                        'gmp_value': gmp_value,
                        'gmp_percentage': gmp_percentage,
                        'rating': rating,
                        'subscription_times': subscription,
                        'gmp_low': gmp_low,
                        'gmp_high': gmp_high,
                        'ipo_price': price,
                        'ipo_size_cr': ipo_size,
                        'lot_size': lot_size,
                        'open_date': open_date,
                        'close_date': close_date,
                        'boa_date': boa_date,
                        'listing_date': listing_date,
                        'updated_on': updated_on,
                        'has_anchor': anchor,
                        'scraped_at': datetime.now()
                    })
            
            df = pd.DataFrame(data)
            logger.info(f"✅ Scraped {len(df)} IPO records")
            return df
            
        except Exception as e:
            logger.error(f"Error scraping data: {e}")
            return None
    
    def load_to_database(self, df):
        """Load scraped data to PostgreSQL"""
        if df is None or df.empty:
            logger.warning("No data to load")
            return
        
        table_name = 'ipo_gmp'
        
        try:
            # Load to database using pandas (will create table if not exists)
            df.to_sql(
                table_name,
                self.engine,
                schema=self.schema,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            logger.info(f"✅ Loaded {len(df)} records to {self.schema}.{table_name}")
            
        except Exception as e:
            logger.error(f"Error loading to database: {e}")
    
    def run(self):
        """Main execution method"""
        logger.info("🚀 Starting IPO GMP scraper")
        
        # Scrape data
        df = self.scrape_ipo_data()
        
        if df is not None:
            # Save to CSV
            output_dir = "/Users/kunal.nandwana/Library/CloudStorage/OneDrive-OneWorkplace/Documents/Personal_Projects/Data/Indian Stock Analytics/ipo_data"
            os.makedirs(output_dir, exist_ok=True)
            
            current_date = datetime.now().strftime('%Y-%m-%d')
            csv_file = f"{output_dir}/ipo_gmp_{current_date}.csv"
            df.to_csv(csv_file, index=False)
            logger.info(f"💾 Saved to {csv_file}")
            
            # Load to database
            self.load_to_database(df)
        
        logger.info("✅ IPO GMP scraper completed")


def main():
    scraper = IPOGMPScraper()
    scraper.run()


if __name__ == "__main__":
    main()
