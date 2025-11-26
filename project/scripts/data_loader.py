# data_loader.py
import boto3
import pandas as pd
import yaml
import io
from concurrent.futures import ThreadPoolExecutor, as_completed

class GeneExpressionDataLoader:
    def __init__(self, config_path='config/datasets.yaml'):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)
        self.s3_client = boto3.client('s3')
        self.bucket = self.config['s3_bucket']
        print(f"Connected to S3 bucket: {self.bucket}")
    
    def process_file(self, filepath):
        """Process a single file directly from S3 without downloading"""
        try:
            # Get metadata
            head_response = self.s3_client.head_object(Bucket=self.bucket, Key=filepath)
            metadata = head_response.get("Metadata", {})
            
            # Read CSV directly from S3 into memory (RAM)
            obj = self.s3_client.get_object(Bucket=self.bucket, Key=filepath)
            df = pd.read_csv(io.BytesIO(obj['Body'].read()))
            
            # Transpose: genes become columns, this sample becomes a row
            df_transposed = df.set_index(df.columns[0]).T  # Assumes first column is gene IDs
            
            # Add metadata as columns
            for key, value in metadata.items():
                df_transposed[key] = value
            
            print(f"Processed: {filepath.split('/')[-1]} - Shape: {df_transposed.shape}")
            return df_transposed
            
        except Exception as e:
            print(f"✗ Error processing {filepath}: {str(e)}")
            return None
    
    def build_dataset(self, geo_id, max_workers=10):
        """Build complete dataset by processing all files in parallel"""
        files = self.list_sample_files(geo_id)
        
        if not files:
            raise ValueError(f"No files found for {geo_id}")
        
        print(f"Building dataset from {len(files)} files...")
        
        # Process files in parallel
        all_dataframes = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {executor.submit(self.process_file, f): f for f in files}
            
            for future in as_completed(future_to_file):
                df = future.result()
                if df is not None:
                    all_dataframes.append(df)
        
        # Concatenate all at once (much faster than incremental concat)
        if not all_dataframes:
            raise ValueError("No dataframes were successfully processed")
        
        final_df = pd.concat(all_dataframes, ignore_index=True)
        print(f"Final dataset shape: {final_df.shape}")
        return final_df
    
    def list_sample_files(self, geo_id):
        """List all sample CSV files in S3"""
        prefix = f'raw/{geo_id}/samples/'
        response = self.s3_client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            print(f"✗ No files found in {prefix}")
            return []
        
        files = [obj['Key'] for obj in response['Contents'] 
                if obj['Key'].endswith('.csv')]
        print(f"✓ Found {len(files)} sample files in {prefix}")
        return files
    
    def save_dataset(self, df, geo_id, output_format='parquet'):
        """Save the final dataset back to S3"""
        output_key = f'processed/{geo_id}/merged_dataset.{output_format}'
        
        # Convert to bytes
        buffer = io.BytesIO()
        if output_format == 'parquet':
            df.to_parquet(buffer, index=False)
        elif output_format == 'csv':
            df.to_csv(buffer, index=False)
        else:
            raise ValueError(f"Unsupported format: {output_format}")
        
        # Upload to S3
        buffer.seek(0)
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=output_key,
            Body=buffer.getvalue()
        )
        print(f"✓ Saved dataset to s3://{self.bucket}/{output_key}")
        return output_key


# Usage example
if __name__ == "__main__":
    loader = GeneExpressionDataLoader()
    
    # Build dataset
    df = loader.build_dataset('GSE58137', max_workers=10)
    df.to_csv('scripts/merged.csv')
    # Inspect
    print("\nDataset preview:")
    print(df.head())
    #print(f"\nMetadata columns: {[col for col in df.columns if col not in df.select_dtypes(include='number').columns]}")
    
    # Save
    #loader.save_dataset(df, 'GSE58137', output_format='csv')