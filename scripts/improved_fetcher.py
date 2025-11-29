import GEOparse
import pandas as pd
import boto3
import os
from pathlib import Path
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class GEOFetcher:
    def __init__(self, config_path='config/datasets.yaml'):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        
        # Initialize S3 client using environment variables
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION_NAME', self.config.get('s3_region', 'eu-north-1'))
        )
        # Allow bucket override via environment variable, otherwise use config
        self.bucket = os.getenv('AWS_S3_BUCKET') or self.config['s3_bucket']
        
    def fetch_dataset(self, geo_id, parallel=False, max_workers=4):
        """Download GEO dataset - one file per person"""
        print(f"Fetching {geo_id}...")
        
        # Download metadata first (lightweight)
        gse = GEOparse.get_GEO(geo=geo_id, destdir='data/raw/', 
                                silent=False, how='brief')
        
        # Extract metadata
        metadata = self.extract_metadata(gse)
        
        # Save complete metadata file
        self.save_metadata(geo_id, metadata)
        
        # Filter samples based on metadata quality
        valid_samples = self.filter_samples(metadata)
        
        print(f"Found {len(valid_samples)} valid samples with age & sex")
        print(f"Processing mode: {'PARALLEL' if parallel else 'SEQUENTIAL'}")
        
        # Download full GEO data once
        print("Downloading full GEO dataset...")
        gse_full = GEOparse.get_GEO(geo=geo_id, destdir='data/raw/')
        
        # Check if we have expression data in series matrix
        expression_matrix = self.get_expression_matrix(gse_full)
        
        if expression_matrix is not None:
            print(f"Found expression matrix with shape: {expression_matrix.shape}")
            if parallel:
                self.process_samples_parallel_matrix(geo_id, expression_matrix, valid_samples, metadata, max_workers)
            else:
                self.process_samples_sequential_matrix(geo_id, expression_matrix, valid_samples, metadata)
        else:
            # Fallback to individual sample processing
            print("No series matrix found, processing individual samples...")
            if parallel:
                self.process_samples_parallel(geo_id, gse_full, valid_samples, metadata, max_workers)
            else:
                self.process_samples_sequential(geo_id, gse_full, valid_samples, metadata)
    
    def get_expression_matrix(self, gse):
        """Extract expression matrix from GSE object"""
        # Try to get the expression matrix from the series
        # This is usually in gse.pivot_samples() or gse.table
        try:
            # Method 1: Try pivot_samples (most common for expression data)
            if hasattr(gse, 'pivot_samples'):
                expr_matrix = gse.pivot_samples('VALUE')
                if expr_matrix is not None and not expr_matrix.empty:
                    print("Found expression data via pivot_samples()")
                    return expr_matrix
        except Exception as e:
            print(f"pivot_samples failed: {e}")
        
        try:
            # Method 2: Try the table attribute
            if hasattr(gse, 'table') and gse.table is not None:
                if not gse.table.empty and 'VALUE' in gse.table.columns:
                    print("Found expression data in gse.table")
                    return gse.table
        except Exception as e:
            print(f"gse.table check failed: {e}")
        
        # Method 3: Check if there's a phenotype matrix with expression
        if hasattr(gse, 'phenotype_data'):
            try:
                pheno = gse.phenotype_data()
                if pheno is not None and not pheno.empty:
                    print("Found phenotype data, checking for expression columns...")
            except Exception as e:
                print(f"phenotype_data check failed: {e}")
        
        return None
    
    def process_samples_sequential_matrix(self, geo_id, expr_matrix, sample_ids, metadata_df):
        """Process samples from expression matrix"""
        for idx, sample_id in enumerate(sample_ids, 1):
            print(f"Processing {idx}/{len(sample_ids)}: {sample_id}")
            self.process_single_sample_from_matrix(geo_id, expr_matrix, sample_id, metadata_df)
    
    def process_samples_parallel_matrix(self, geo_id, expr_matrix, sample_ids, metadata_df, max_workers=4):
        """Process samples from expression matrix in parallel"""
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_sample = {
                executor.submit(self.process_single_sample_from_matrix, geo_id, expr_matrix, sample_id, metadata_df): sample_id
                for sample_id in sample_ids
            }
            
            completed = 0
            for future in as_completed(future_to_sample):
                sample_id = future_to_sample[future]
                try:
                    future.result()
                    completed += 1
                    print(f"✓ Completed {completed}/{len(sample_ids)}: {sample_id}")
                except Exception as e:
                    print(f"✗ Failed {sample_id}: {e}")
    
    def process_single_sample_from_matrix(self, geo_id, expr_matrix, sample_id, metadata_df):
        """Extract single sample from expression matrix"""
        # Check if sample exists in matrix
        if sample_id not in expr_matrix.columns:
            print(f"Warning: {sample_id} not found in expression matrix")
            return
        
        # Extract this sample's expression data
        sample_expr = expr_matrix[[sample_id]].copy()
        sample_expr = sample_expr.reset_index()
        sample_expr.columns = ['gene_id', 'expression_value']
        
        # Get metadata for this sample
        sample_meta = metadata_df[metadata_df['sample_id'] == sample_id].iloc[0]
        
        # Check if we have actual data
        if sample_expr.empty or len(sample_expr) == 0:
            print(f"Warning: No expression data for {sample_id}")
            return
        
        print(f"  Sample {sample_id}: {len(sample_expr)} genes")
        
        # Save locally
        os.makedirs('data/processed', exist_ok=True)
        output_path = f'data/processed/{geo_id}_{sample_id}.csv'
        sample_expr.to_csv(output_path, index=False)
        
        # Upload to S3 with metadata
        s3_key = f'{geo_id}/samples/{sample_id}.csv'
        self.upload_to_s3_with_metadata(
            output_path, 
            s3_key,
            sample_meta
        )
        
        # Clean up local file
        os.remove(output_path)
    
    def process_samples_sequential(self, geo_id, gse, sample_ids, metadata_df):
        """Process samples one by one (original method)"""
        for idx, sample_id in enumerate(sample_ids, 1):
            print(f"Processing {idx}/{len(sample_ids)}: {sample_id}")
            self.process_single_sample(geo_id, gse, sample_id, metadata_df)
    
    def process_samples_parallel(self, geo_id, gse, sample_ids, metadata_df, max_workers=4):
        """Process samples in parallel using threading (original method)"""
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_sample = {
                executor.submit(self.process_single_sample, geo_id, gse, sample_id, metadata_df): sample_id
                for sample_id in sample_ids
            }
            
            completed = 0
            for future in as_completed(future_to_sample):
                sample_id = future_to_sample[future]
                try:
                    future.result()
                    completed += 1
                    print(f"✓ Completed {completed}/{len(sample_ids)}: {sample_id}")
                except Exception as e:
                    print(f"✗ Failed {sample_id}: {e}")
    
    def process_single_sample(self, geo_id, gse, sample_id, metadata_df):
        """Process a single sample: extract expression + add metadata (original method)"""
        if sample_id not in gse.gsms:
            print(f"Warning: {sample_id} not found in dataset")
            return
        
        # Get expression data
        gsm = gse.gsms[sample_id]
        
        # Check if table has data
        if gsm.table is None or gsm.table.empty:
            print(f"Warning: {sample_id} has no table data")
            return
        
        expr_df = gsm.table.copy()
        
        # Verify we have expression values
        if 'VALUE' not in expr_df.columns and len(expr_df.columns) < 2:
            print(f"Warning: {sample_id} table has no VALUE column")
            return
        
        print(f"  Sample {sample_id}: {len(expr_df)} rows, columns: {list(expr_df.columns)}")
        
        # Get metadata for this sample
        sample_meta = metadata_df[metadata_df['sample_id'] == sample_id].iloc[0]
        
        # Save locally
        os.makedirs('data/processed', exist_ok=True)
        output_path = f'data/processed/{geo_id}_{sample_id}.csv'
        expr_df.to_csv(output_path, index=False)
        
        # Upload to S3 with metadata in object tags
        s3_key = f'{geo_id}/samples/{sample_id}.csv'
        self.upload_to_s3_with_metadata(
            output_path, 
            s3_key,
            sample_meta
        )
        
        # Clean up local file to save space
        os.remove(output_path)
    
    def save_metadata(self, geo_id, metadata):
        """Save complete metadata file"""
        print("Saving metadata file...")
        
        # Save locally
        os.makedirs('data/processed', exist_ok=True)
        metadata_csv = f'data/processed/{geo_id}_metadata.csv'
        
        metadata.to_csv(metadata_csv, index=False)
        
        # Upload to S3
        self.upload_to_s3(metadata_csv, f'{geo_id}/metadata.csv')
        
        # Clean up
        os.remove(metadata_csv)
        
        print(f"Saved metadata for {len(metadata)} samples")
    
    def extract_metadata(self, gse):
        """Extract sample metadata"""
        samples = []
        for gsm_name, gsm in gse.gsms.items():
            sample_info = {
                'sample_id': gsm_name,
                'title': gsm.metadata.get('title', [''])[0],
                'age': self.parse_age(gsm.metadata),
                'sex': self.parse_sex(gsm.metadata),
                'tissue': gsm.metadata.get('tissue', ['unknown'])[0],
                'source': gsm.metadata.get('source_name_ch1', [''])[0],
                'organism': gsm.metadata.get('organism_ch1', [''])[0],
            }
            samples.append(sample_info)
        
        return pd.DataFrame(samples)
    
    def parse_age(self, metadata):
        """Extract age from metadata"""
        for field in ['age', 'characteristics_ch1', 'description']:
            if field in metadata:
                for item in metadata[field]:
                    if 'age' in item.lower():
                        import re
                        match = re.search(r'(\d+)', item)
                        if match:
                            return int(match.group(1))
        return None
    
    def parse_sex(self, metadata):
        """Extract sex from metadata"""
        for field in ['gender', 'sex', 'characteristics_ch1']:
            if field in metadata:
                for item in metadata[field]:
                    item_lower = str(item).lower()
                    if 'male' in item_lower or 'm' == item_lower:
                        return 'male' if 'female' not in item_lower else 'female'
                    if 'female' or 'f' in item_lower:
                        return 'female'
        return None
    
    def filter_samples(self, metadata):
        """Keep only samples with required metadata"""
        filtered = metadata[
            metadata['age'].notna() & 
            metadata['sex'].notna()
        ]
        return filtered['sample_id'].tolist()
    
    def upload_to_s3_with_metadata(self, local_path, s3_key, sample_metadata):
        """Upload file to S3 with sample metadata as object tags"""
        try:
            metadata = {
                'sample-id': str(sample_metadata['sample_id']),
                'age': str(sample_metadata['age']),
                'sex': str(sample_metadata['sex']),
            }
            
            self.s3_client.upload_file(
                local_path, 
                self.bucket, 
                f'raw/{s3_key}',
                ExtraArgs={'Metadata': metadata}
            )
            
        except Exception as e:
            print(f"Upload failed for {s3_key}: {e}")
    
    def upload_to_s3(self, local_path, s3_key):
        """Simple upload to S3"""
        try:
            self.s3_client.upload_file(
                local_path, 
                self.bucket, 
                f'raw/{s3_key}'
            )
        except Exception as e:
            print(f"Upload failed for {s3_key}: {e}")

if __name__ == '__main__':
    fetcher = GEOFetcher()

    datasets =[
    "GSE74813",
    "GSE68310",
    "GSE74811",
    "GSE48023",
    "GSE61754",
    "GSE90732",
    "GSE107990",
    "GSE59635",
    "GSE101709",
    "GSE59654",
    "GSE59743"]

    #datasets = ['GSE48762']
    #datasets = ['GSE58137', 'GSE63063'] #'GSE143507' tsv bitch
    for dataset in datasets:
        # Parallel processing (4 workers)
        fetcher.fetch_dataset(dataset, parallel=True, max_workers=4)