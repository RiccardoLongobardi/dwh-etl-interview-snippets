import pandas as pd
import hashlib
import logging
from datetime import datetime, timedelta

# =============================================================================
# PYTHON ETL PIPELINE SNIPPET
# Demonstrates: Data Transformation, SCD7 Pattern, Hash Generation, Error Handling
# =============================================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self, source_system):
        self.source_system = source_system
        self.proc_date = datetime.now()
        self.max_date = datetime.strptime('99991231', '%Y%m%d')

    def generate_sha256(self, record_string):
        """Generates a SHA256 hash for HUB/SAT keys."""
        return hashlib.sha256(record_string.encode()).hexdigest()

    def detect_changes(self, df_current, df_new):
        """
        Detects records that have changed between current and new versions.
        Used to determine which records need SCD7 history updates.
        """
        logger.info(f"Detecting changes for {len(df_new)} incoming records...")
        
        # In a real scenario, this would merge with actual current table
        # and identify SAT_KEY mismatches indicating attribute changes
        df_new['CHANGED'] = True
        return df_new

    def transform_data(self, df):
        """Applies business logic and technical keys for SCD7 processing."""
        logger.info(f"Transforming data for {self.source_system}...")
        
        # 1. Deduplication on business keys
        df = df.drop_duplicates(subset=['CONTRACT_ID', 'BANK_ID'], keep='first')
        logger.info(f"After deduplication: {len(df)} records")
        
        # 2. Generate HUB key (business key hash)
        df['X_HUB_KEY'] = df.apply(
            lambda x: self.generate_sha256(f"{x['CONTRACT_ID']}#{x['BANK_ID']}"), 
            axis=1
        )
        
        # 3. Generate SAT key (attribute hash for change detection)
        df['X_SAT_KEY'] = df.apply(
            lambda x: self.generate_sha256(
                f"{x['LOAN_STATUS_CD']}#{x['OUTSTANDING_AMT']}#{x['RATE_TYPE_CD']}"
            ), 
            axis=1
        )
        
        # 4. Add SCD7 technical columns
        df['X_EFFECTIVE_FROM_DT'] = self.proc_date
        df['X_EFFECTIVE_TO_DT'] = self.max_date
        df['SOURCE_SYSTEM_CD'] = self.source_system
        
        # 5. Add audit timestamps
        df['X_INSERT_TS'] = self.proc_date
        
        return df

    def prepare_history_load(self, df_changed):
        """
        Prepares records for HISTORY table insert (SCD Type 2).
        Only records with changed attributes are inserted as new versions.
        """
        logger.info(f"Preparing {len(df_changed)} records for HISTORY table...")
        
        history_cols = [
            'X_HUB_KEY', 'X_SAT_KEY', 'CONTRACT_ID', 'BANK_ID',
            'LOAN_STATUS_CD', 'LOAN_STATUS_DESC', 'RATE_TYPE_CD', 'MATURITY_DT',
            'X_EFFECTIVE_FROM_DT', 'X_INSERT_TS'
        ]
        
        df_history = df_changed[history_cols].copy()
        return df_history

    def prepare_current_load(self, df_transformed):
        """
        Prepares records for CURRENT table merge (SCD Type 1).
        All records overwrite the latest state.
        """
        logger.info(f"Preparing {len(df_transformed)} records for CURRENT table...")
        
        current_cols = [
            'X_HUB_KEY', 'X_SAT_KEY', 'CONTRACT_ID', 'BANK_ID',
            'LOAN_STATUS_CD', 'LOAN_STATUS_DESC', 'RATE_TYPE_CD', 'MATURITY_DT',
            'X_EFFECTIVE_FROM_DT', 'X_INSERT_TS'
        ]
        
        df_current = df_transformed[current_cols].copy()
        return df_current

    def run_load(self, data):
        """
        Main ETL orchestration: Load → Transform → Detect Changes → Prepare Loads
        """
        try:
            logger.info(f"=== ETL Job Start ({self.source_system}) ===")
            
            # Load and transform
            df = pd.DataFrame(data)
            logger.info(f"Loaded {len(df)} records from source")
            
            df_transformed = self.transform_data(df)
            
            # Detect changes (simplified for demo; real logic queries current table)
            df_changed = self.detect_changes(df, df_transformed)
            
            # Prepare loads for both tables
            df_history = self.prepare_history_load(df_changed)
            df_current = self.prepare_current_load(df_transformed)
            
            logger.info(f"Successfully prepared {len(df_history)} history records and {len(df_current)} current records")
            logger.info(f"=== ETL Job Complete ===")
            
            return {
                'history': df_history,
                'current': df_current,
                'transformed': df_transformed
            }
        except Exception as e:
            logger.error(f"ETL Failed: {str(e)}")
            raise


if __name__ == "__main__":
    # Example Usage: SCD7 ETL Pipeline
    sample_data = [
        {
            'CONTRACT_ID': 'LN1001',
            'BANK_ID': 'B001',
            'LOAN_STATUS_CD': 'ACT',
            'LOAN_STATUS_DESC': 'Active',
            'RATE_TYPE_CD': 'FIXED',
            'MATURITY_DT': '2025-12-31',
            'OUTSTANDING_AMT': 50000.00
        },
        {
            'CONTRACT_ID': 'LN1002',
            'BANK_ID': 'B001',
            'LOAN_STATUS_CD': 'DEL',
            'LOAN_STATUS_DESC': 'Delinquent',
            'RATE_TYPE_CD': 'VARIABLE',
            'MATURITY_DT': '2026-06-15',
            'OUTSTANDING_AMT': 12500.50
        }
    ]
    
    etl = ETLPipeline(source_system='CORE_BANKING')
    result = etl.run_load(sample_data)
    
    # Display results
    print("\n=== HISTORY TABLE OUTPUT ===")
    print(result['history'][['X_HUB_KEY', 'CONTRACT_ID', 'LOAN_STATUS_CD', 'X_EFFECTIVE_FROM_DT']])
    
    print("\n=== CURRENT TABLE OUTPUT ===")
    print(result['current'][['X_HUB_KEY', 'CONTRACT_ID', 'LOAN_STATUS_CD', 'X_EFFECTIVE_FROM_DT']])
