import GEOparse
import pandas as pd
import boto3
import os
from pathlib import Path
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

class GEOFetcher:
    def __init__(self, config_path='config/datasets.yaml'):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        
        self.s3_client = boto3.client('s3')
        self.bucket = self.config['s3_bucket']
        
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
        
        if parallel:
            self.process_samples_parallel(geo_id, gse_full, valid_samples, metadata, max_workers)
        else:
            self.process_samples_sequential(geo_id, gse_full, valid_samples, metadata)
    
    def process_samples_sequential(self, geo_id, gse, sample_ids, metadata_df):
        """Process samples one by one"""
        for idx, sample_id in enumerate(sample_ids, 1):
            print(f"Processing {idx}/{len(sample_ids)}: {sample_id}")
            self.process_single_sample(geo_id, gse, sample_id, metadata_df)
    
    def process_samples_parallel(self, geo_id, gse, sample_ids, metadata_df, max_workers=4):
        """Process samples in parallel using threading"""
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_sample = {
                executor.submit(self.process_single_sample, geo_id, gse, sample_id, metadata_df): sample_id
                for sample_id in sample_ids
            }
            
            # Process as they complete
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
        """Process a single sample: extract expression + add metadata"""
        if sample_id not in gse.gsms:
            print(f"Warning: {sample_id} not found in dataset")
            return
        
        # Get expression data
        gsm = gse.gsms[sample_id]
        expr_df = gsm.table.copy()
        
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
        #metadata_parquet = f'data/processed/{geo_id}_metadata.parquet'
        
        metadata.to_csv(metadata_csv, index=False)
        #metadata.to_parquet(metadata_parquet, compression='gzip')
        
        # Upload to S3
        self.upload_to_s3(metadata_csv, f'{geo_id}/metadata.csv')
        #self.upload_to_s3(metadata_parquet, f'{geo_id}/metadata.parquet')
        
        # Clean up
        os.remove(metadata_csv)
        #os.remove(metadata_parquet)
        
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
                    if 'male' in item_lower:
                        return 'male' if 'female' not in item_lower else 'female'
                    if 'female' in item_lower:
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
            # S3 object metadata (limited to simple string key-values)
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
    
    # Process datasets
    datasets = ['GSE132040', 'GSE63063', 'GSE58137']
    
    for dataset in datasets:
        # Sequential processing
        # fetcher.fetch_dataset(dataset, parallel=False)
        
        # Parallel processing (4 workers)
        fetcher.fetch_dataset(dataset, parallel=True, max_workers=4)
