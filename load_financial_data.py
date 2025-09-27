#!/usr/bin/env python3
"""
Load Financial Data from CSV files to PostgreSQL Tables

This script processes financial data CSV files organized by company symbol
and loads them into corresponding PostgreSQL tables in the bron                logger.info(f"✅ Processing complete: {symbol} ({i+1}/{total_companies})")
                logger.info(f"   📊 {sections_loaded}/6 sections loaded successfully")
                
            except Exception as e:a.

Directory structure:
/path/to/financial_data/
├── SYMBOL1/
│   ├── balance_sheet.csv
│   ├── cash_flow.csv
│   ├── company_ratio.csv
│   ├── profit_loss.csv
│   ├── quarterly.csv
│   └── shareholding.csv
└── SYMBOL2/
    └── ...

Each section gets loaded into bronze.{section_name} with symbol column added.
"""

import os
import pandas as pd
import sqlalchemy
from sqlalchemy import text
import argparse
from pathlib import Path
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('load_financial_data.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class FinancialDataLoader:
    def __init__(self, data_dir, schema='bronze'):
        self.data_dir = Path(data_dir)
        self.schema = schema
        self.engine = self._create_engine()
        self.connection_string = self._get_connection_string()  # Store connection string too
        
        # Define sections and their corresponding table names
        self.sections = {
            'balance_sheet': 'balance_sheet',
            'cash_flow': 'cash_flow', 
            'company_ratio': 'company_ratio',
            'profit_loss': 'profit_loss',
            'quarterly': 'quarterly',
            'shareholding': 'shareholding'
        }
        
    def _get_connection_string(self):
        """Get the connection string for pandas"""
        PG_USER = os.environ.get('DATABASE_USER', 'kunal.nandwana')
        PG_PASS = os.environ.get('DATABASE_PASSWORD', 'root')
        PG_HOST = os.environ.get('DATABASE_HOST', 'localhost')
        PG_PORT = os.environ.get('DATABASE_PORT', '5432')
        PG_DB   = os.environ.get('DATABASE_NAME', 'kunal.nandwana')
        
        if 'DATABASE_URL' in os.environ:
            return os.environ['DATABASE_URL']
        else:
            return f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
        
    def _create_engine(self):
        """Create PostgreSQL engine using environment variables or defaults"""
        PG_USER = os.environ.get('DATABASE_USER', 'kunal.nandwana')
        PG_PASS = os.environ.get('DATABASE_PASSWORD', 'root')
        PG_HOST = os.environ.get('DATABASE_HOST', 'localhost')
        PG_PORT = os.environ.get('DATABASE_PORT', '5432')
        PG_DB   = os.environ.get('DATABASE_NAME', 'kunal.nandwana')
        
        if 'DATABASE_URL' in os.environ:
            connection_string = os.environ['DATABASE_URL']
        else:
            connection_string = f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
        
        logger.info(f"Connecting to database: {PG_HOST}:{PG_PORT} as {PG_USER}")
        return sqlalchemy.create_engine(connection_string)

    def get_company_folders(self):
        """Get list of company folders in the data directory"""
        if not self.data_dir.exists():
            raise FileNotFoundError(f"Data directory does not exist: {self.data_dir}")
        
        companies = [
            folder.name for folder in self.data_dir.iterdir() 
            if folder.is_dir() and not folder.name.startswith('.')
        ]
        logger.info(f"Found {len(companies)} company folders")
        return sorted(companies)

    def load_csv_with_symbol(self, csv_path, symbol, section_name=None):
        """Load CSV file and add company_name column"""
        try:
            df = pd.read_csv(csv_path)
            if df.empty:
                logger.warning(f"Empty CSV file: {csv_path}")
                return None
            
            # Add company_name column at the beginning (instead of symbol)
            df.insert(0, 'company_name', symbol)
            
            # Clean column names to match database schema
            df.columns = [self._sanitize_column_name(col) for col in df.columns]
            
            # Special handling for quarterly table - rename 'quarter' to 'year'
            if section_name == 'quarterly' and 'quarter' in df.columns:
                df = df.rename(columns={'quarter': 'year'})
                logger.info(f"Renamed 'quarter' to 'year' for quarterly data")
            
            # Convert percentage columns (remove % and convert to string)
            for col in df.columns:
                if df[col].dtype == 'object':
                    # Handle percentage columns
                    if df[col].astype(str).str.contains('%').any():
                        df[col] = df[col].astype(str).str.replace('%', '')
            
            return df
        except Exception as e:
            logger.error(f"Error reading {csv_path}: {e}")
            return None

    def _sanitize_column_name(self, name):
        """Sanitize column names for PostgreSQL compatibility"""
        import re
        name = re.sub(r"[^\w\s]", "", str(name))
        name = name.lower().strip().replace(" ", "_").replace("__", "_")
        return name.rstrip("_")

    def create_table_if_not_exists(self, table_name, df_sample):
        """Create table if it doesn't exist - tables already exist, so this is just a check"""
        # Tables already exist, just verify they're there
        check_sql = f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = '{self.schema}' 
            AND table_name = '{table_name}'
        )
        """
        
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(check_sql)).scalar()
                if result:
                    logger.info(f"Table {self.schema}.{table_name} exists")
                else:
                    logger.error(f"Table {self.schema}.{table_name} does not exist - please create it first")
        except Exception as e:
            logger.error(f"Error checking table {table_name}: {e}")

    def load_section_data(self, section_name, table_name, symbol, csv_path):
        """Load data from CSV into PostgreSQL table using raw SQL (avoiding pandas compatibility issues)"""
        df = self.load_csv_with_symbol(csv_path, symbol, section_name)
        if df is None or df.empty:
            return False

        # Check table exists
        self.create_table_if_not_exists(table_name, df)

        try:
            # Get table columns to match with CSV columns
            table_columns = self._get_table_columns(table_name)
            
            # Find matching columns between CSV and table
            matching_columns = [col for col in df.columns if col in table_columns]
            
            if not matching_columns:
                logger.error(f"No matching columns found for {table_name}")
                return False
            
            # Filter dataframe to only include matching columns
            df_filtered = df[matching_columns].copy()
            
            logger.info(f"Using columns for {table_name}: {list(df_filtered.columns)}")
            
            # Manual SQL insertion to avoid pandas compatibility issues
            with self.engine.begin() as conn:  # Use begin() for automatic commit
                # Insert data row by row with proper SQL parameterization
                for index, row in df_filtered.iterrows():
                    # Build the values dictionary for named parameters
                    values_dict = {col: row[col] for col in matching_columns}
                    
                    # Prepare named parameters for SQL
                    named_params = ', '.join([f":{col}" for col in matching_columns])
                    columns_str = ', '.join(matching_columns)
                    
                    # Build the ON CONFLICT UPDATE clause
                    update_columns = [col for col in matching_columns if col not in ['company_name', 'year']]
                    
                    if update_columns:
                        # Only create UPDATE clause if there are columns to update
                        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
                        
                        insert_sql = f"""
                            INSERT INTO {self.schema}.{table_name} ({columns_str}, updated_at)
                            VALUES ({named_params}, CURRENT_TIMESTAMP)
                            ON CONFLICT (company_name, year) 
                            DO UPDATE SET
                                {update_clause},
                                updated_at = CURRENT_TIMESTAMP
                        """
                    else:
                        # If only company_name and year, just do INSERT...ON CONFLICT DO NOTHING
                        insert_sql = f"""
                            INSERT INTO {self.schema}.{table_name} ({columns_str}, updated_at)
                            VALUES ({named_params}, CURRENT_TIMESTAMP)
                            ON CONFLICT (company_name, year) 
                            DO UPDATE SET
                                updated_at = CURRENT_TIMESTAMP
                        """
                    
                    conn.execute(text(insert_sql), values_dict)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error loading {section_name} for {symbol}: {e}")
            return False

    def _get_table_columns(self, table_name):
        """Get actual columns that exist in the target table"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = '{table_name}' 
                    AND table_schema = '{self.schema}'
                    AND column_name NOT IN ('id', 'created_at', 'updated_at')
                    ORDER BY ordinal_position
                """))
                return [row[0] for row in result.fetchall()]
        except Exception as e:
            logger.error(f"Error getting columns for {table_name}: {e}")
            return []

    def _get_merge_query_with_columns(self, table_name, temp_table_name, csv_columns, table_columns):
        """Generate table-specific merge queries using pre-fetched table columns"""
        
        # Find matching columns between CSV and table
        matching_columns = [col for col in csv_columns if col in table_columns]
        
        if not matching_columns:
            logger.error(f"No matching columns found for {table_name}")
            return []
        
        # Remove id, created_at from columns for insert/update
        data_columns = [col for col in matching_columns if col not in ['id', 'created_at']]
        update_columns = [col for col in data_columns if col not in ['company_name', 'year']]
        
        logger.info(f"Using columns for {table_name}: {data_columns}")
        
        # Base merge query template
        merge_query = f"""
        INSERT INTO {self.schema}.{table_name} 
        ({', '.join(data_columns)}, updated_at)
        SELECT {', '.join(data_columns)}, CURRENT_TIMESTAMP
        FROM {self.schema}.{temp_table_name}
        ON CONFLICT (company_name, year) 
        DO UPDATE SET
            {', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])},
            updated_at = CURRENT_TIMESTAMP
        """
        
        return [merge_query]

    def _get_merge_query(self, table_name, temp_table_name, csv_columns):
        """Generate table-specific merge queries based on actual table schema"""
        
        # Get actual table columns
        table_columns = self._get_table_columns(table_name)
        
        # Find matching columns between CSV and table
        matching_columns = [col for col in csv_columns if col in table_columns]
        
        if not matching_columns:
            logger.error(f"No matching columns found for {table_name}")
            return []
        
        # Remove id, created_at from columns for insert/update
        data_columns = [col for col in matching_columns if col not in ['id', 'created_at']]
        update_columns = [col for col in data_columns if col not in ['company_name', 'year']]
        
        logger.info(f"Using columns for {table_name}: {data_columns}")
        
        # Base merge query template
        merge_query = f"""
        INSERT INTO {self.schema}.{table_name} 
        ({', '.join(data_columns)}, updated_at)
        SELECT {', '.join(data_columns)}, CURRENT_TIMESTAMP
        FROM {self.schema}.{temp_table_name}
        ON CONFLICT (company_name, year) 
        DO UPDATE SET
            {', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])},
            updated_at = CURRENT_TIMESTAMP
        """
        
        return [merge_query]

    def process_company(self, symbol):
        """Process all sections for a single company"""
        company_dir = self.data_dir / symbol
        if not company_dir.exists():
            logger.warning(f"Company directory not found: {company_dir}")
            return
        
        logger.info(f"\n📊 Processing company: {symbol}")
        success_count = 0
        
        for section_name, table_name in self.sections.items():
            csv_file = company_dir / f"{section_name}.csv"
            
            if csv_file.exists():
                if self.load_section_data(section_name, table_name, symbol, csv_file):
                    success_count += 1
            else:
                logger.warning(f"CSV file not found: {csv_file}")
        
        logger.info(f"✅ Completed {symbol}: {success_count}/{len(self.sections)} sections loaded")

    def process_all_companies(self):
        """Process all companies in the data directory"""
        companies = self.get_company_folders()
        logger.info(f"\n🚀 Starting to process {len(companies)} companies...")
        
        success_companies = 0
        for i, company in enumerate(companies, 1):
            try:
                logger.info(f"\n[{i}/{len(companies)}] Processing {company}")
                self.process_company(company)
                success_companies += 1
                
                # Add 1.5 second sleep after each company for monitoring
                logger.info(f"⏳ Sleeping for 1.5 seconds...")
                time.sleep(1.5)
                
            except Exception as e:
                logger.error(f"Error processing company {company}: {e}")
        
        logger.info(f"\n🎉 Completed! Successfully processed {success_companies}/{len(companies)} companies")

    def process_specific_companies(self, company_list):
        """Process only specific companies"""
        logger.info(f"\n🎯 Processing specific companies: {company_list}")
        
        for company in company_list:
            try:
                self.process_company(company)
            except Exception as e:
                logger.error(f"Error processing company {company}: {e}")


def main():
    parser = argparse.ArgumentParser(description='Load financial data CSV files into PostgreSQL')
    parser.add_argument(
        '--data-dir', 
        default='/Users/kunal.nandwana/Library/CloudStorage/OneDrive-OneWorkplace/Documents/Personal_Projects/Data/Indian Stock Analytics/financial_data',
        help='Path to financial data directory'
    )
    parser.add_argument(
        '--schema', 
        default='bronze',
        help='PostgreSQL schema name'
    )
    parser.add_argument(
        '--companies',
        nargs='+',
        help='Process only specific companies (space-separated list)'
    )
    parser.add_argument(
        '--test',
        action='store_true',
        help='Test mode: process only first 3 companies'
    )
    
    args = parser.parse_args()
    
    # Initialize loader
    loader = FinancialDataLoader(args.data_dir, args.schema)
    
    try:
        if args.companies:
            # Process specific companies
            loader.process_specific_companies(args.companies)
        elif args.test:
            # Test mode: process first 3 companies
            companies = loader.get_company_folders()[:3]
            loader.process_specific_companies(companies)
        else:
            # Process all companies
            loader.process_all_companies()
            
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())