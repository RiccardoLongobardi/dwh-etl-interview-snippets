import pandas as pd
import hashlib
import logging
from datetime import datetime

# =============================================================================
# PYTHON ETL PIPELINE SNIPPET
# Demonstrates: Data Transformation, Hash Generation, and ETL Orchestration
# =============================================================================

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ETLPipeline:
    def __init__(self, source_system):
        self.source_system = source_system
        self.proc_date = datetime.now()

    def generate_sha256(self, record_string):
        """Generates a SHA256 hash for HUB/SAT keys."""
        return hashlib.sha256(record_string.encode()).hexdigest()

    def transform_data(self, df):
        """Applies business logic and technical keys."""
        logger.info(f"Transforming data for {self.source_system}...")
        
        # 1. Deduplication
        df = df.drop_duplicates(subset=['CONTRACT_ID', 'BANK_ID'])
        
        # 2. Add Technical Columns
        df['X_HUB_KEY'] = df.apply(
            lambda x: self.generate_sha256(f"{x['CONTRACT_ID']}#{x['BANK_ID']}"), 
            axis=1
        )
        
        df['X_SAT_KEY'] = df.apply(
            lambda x: self.generate_sha256(f"{x['LOAN_STATUS_CD']}#{x['OUTSTANDING_AMT']}"), 
            axis=1
        )
        
        df['X_EFFECTIVE_FROM_DT'] = self.proc_date
        df['SOURCE_SYSTEM_CD'] = self.source_system
        
        return df

    def run_load(self, data):
        """Mock load process."""
        try:
            df = pd.DataFrame(data)
            transformed_df = self.transform_data(df)
            
            logger.info(f"Successfully processed {len(transformed_df)} records.")
            return transformed_df
        except Exception as e:
            logger.error(f"ETL Failed: {str(e)}")
            raise

if __name__ == "__main__":
    # Example Usage
    sample_data = [
        {'CONTRACT_ID': 'LN1001', 'BANK_ID': 'B001', 'LOAN_STATUS_CD': 'ACT', 'OUTSTANDING_AMT': 50000.00},
        {'CONTRACT_ID': 'LN1002', 'BANK_ID': 'B001', 'LOAN_STATUS_CD': 'DEL', 'OUTSTANDING_AMT': 12500.50}
    ]
    
    etl = ETLPipeline(source_system='CORE_BANKING')
    result = etl.run_load(sample_data)
    print(result[['X_HUB_KEY', 'CONTRACT_ID', 'OUTSTANDING_AMT']])
