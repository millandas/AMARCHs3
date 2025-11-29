"""
TCGA Lung Cancer Fetcher with S3 Upload
Downloads TCGA-LUAD and TCGA-LUSC data and uploads to S3
- One file per patient with gene expression
- Metadata stored in S3 object tags
- Similar structure to GEO fetcher
"""

import requests
import json
import os
import pandas as pd
import time
import boto3
from typing import List, Dict, Optional
import gzip
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import yaml

# Load environment variables
load_dotenv()


class TCGALungFetcher:
    def __init__(self, config_path='config/datasets.yaml'):
        """Initialize TCGA fetcher with S3 credentials"""
        # Load config if exists
        self.config = {}
        if os.path.exists(config_path):
            with open(config_path) as f:
                self.config = yaml.safe_load(f)
        
        # GDC API base URL
        self.base_url = "https://api.gdc.cancer.gov"
        
        # Initialize S3 client
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=os.getenv('AWS_REGION_NAME', self.config.get('s3_region', 'eu-north-1'))
        )
        self.bucket = os.getenv('AWS_S3_BUCKET') or self.config.get('s3_bucket')
        
        # Project IDs
        self.projects = {
            'LUAD': 'TCGA-LUAD',
            'LUSC': 'TCGA-LUSC'
        }
        
        # Local temp directory
        self.temp_dir = 'data/tcga_temp'
        os.makedirs(self.temp_dir, exist_ok=True)
    
    def fetch_project(self, project_id: str, parallel: bool = True, 
                     max_workers: int = 4, protein_coding_only: bool = False):
        """
        Main method to fetch entire TCGA project
        
        Args:
            project_id: TCGA-LUAD or TCGA-LUSC
            parallel: Use parallel processing
            max_workers: Number of parallel workers
            protein_coding_only: Filter to ~20K protein-coding genes only
        """
        print(f"\n{'='*70}")
        print(f"Fetching {project_id}")
        print(f"{'='*70}")
        
        # Step 1: Get and upload clinical metadata
        print("\n[1/3] Fetching clinical metadata...")
        clinical_df = self.fetch_clinical_metadata(project_id)
        self.upload_metadata(project_id, clinical_df)
        
        # Step 2: Get list of gene expression files
        print("\n[2/3] Finding gene expression files...")
        file_list = self.get_gene_expression_files(project_id)
        
        if not file_list:
            print(f"No gene expression files found for {project_id}")
            return
        
        # Step 3: Download and upload expression files
        print(f"\n[3/3] Processing {len(file_list)} expression files...")
        print(f"Mode: {'PARALLEL' if parallel else 'SEQUENTIAL'}")
        print(f"Filter: {'Protein-coding only (~20K genes)' if protein_coding_only else 'All genes (~60K)'}")
        
        if parallel:
            self.process_files_parallel(project_id, file_list, clinical_df, 
                                       max_workers, protein_coding_only)
        else:
            self.process_files_sequential(project_id, file_list, clinical_df,
                                         protein_coding_only)
        
        print(f"\n✓ Completed {project_id}")
    
    def fetch_clinical_metadata(self, project_id: str) -> pd.DataFrame:
        """Fetch all clinical metadata for project"""
        filters = {
            "op": "=",
            "content": {
                "field": "cases.project.project_id",
                "value": [project_id]
            }
        }
        
        fields = [
            "case_id",
            "submitter_id",
            "demographic.gender",
            "demographic.race",
            "demographic.ethnicity",
            "demographic.vital_status",
            "demographic.days_to_birth",
            "demographic.days_to_death",
            "diagnoses.age_at_diagnosis",
            "diagnoses.primary_diagnosis",
            "diagnoses.tumor_stage",
            "diagnoses.tissue_or_organ_of_origin",
            "diagnoses.days_to_last_follow_up",
            "exposures.cigarettes_per_day",
            "exposures.pack_years_smoked",
            "exposures.years_smoked",
            "treatments.treatment_type",
            "treatments.therapeutic_agents",
            "treatments.days_to_treatment_start",
            "treatments.days_to_treatment_end",
            "treatments.treatment_outcome"
        ]
        
        params = {
            "filters": json.dumps(filters),
            "fields": ",".join(fields),
            "format": "JSON",
            "size": 10000
        }
        
        response = requests.get(f"{self.base_url}/cases", params=params)
        
        if response.status_code != 200:
            print(f"Error fetching clinical data: {response.status_code}")
            return pd.DataFrame()
        
        data = response.json()['data']['hits']
        
        # Flatten nested structure
        records = []
        for case in data:
            # Base record
            record = {
                'case_id': case.get('case_id'),
                'patient_id': case.get('submitter_id'),
            }
            
            # Demographics
            demo = case.get('demographic', {})
            if demo:
                record.update({
                    'gender': demo.get('gender'),
                    'race': demo.get('race'),
                    'ethnicity': demo.get('ethnicity'),
                    'vital_status': demo.get('vital_status'),
                    'age_years': -int(demo.get('days_to_birth', 0)) / 365.25 if demo.get('days_to_birth') else None,
                    'days_to_death': demo.get('days_to_death')
                })
            
            # Diagnosis
            diagnoses = case.get('diagnoses', [])
            if diagnoses:
                diag = diagnoses[0]
                record.update({
                    'age_at_diagnosis': diag.get('age_at_diagnosis'),
                    'primary_diagnosis': diag.get('primary_diagnosis'),
                    'tumor_stage': diag.get('tumor_stage'),
                    'tissue_or_organ_of_origin': diag.get('tissue_or_organ_of_origin'),
                    'days_to_last_follow_up': diag.get('days_to_last_follow_up')
                })
            
            # Exposures (smoking)
            exposures = case.get('exposures', [])
            if exposures:
                exp = exposures[0]
                record.update({
                    'cigarettes_per_day': exp.get('cigarettes_per_day'),
                    'pack_years_smoked': exp.get('pack_years_smoked'),
                    'years_smoked': exp.get('years_smoked')
                })
            
            # Treatments
            treatments = case.get('treatments', [])
            if treatments:
                for i, treatment in enumerate(treatments):
                    treatment_record = record.copy()
                    treatment_record.update({
                        'treatment_number': i + 1,
                        'treatment_type': treatment.get('treatment_type'),
                        'therapeutic_agents': treatment.get('therapeutic_agents'),
                        'days_to_treatment_start': treatment.get('days_to_treatment_start'),
                        'days_to_treatment_end': treatment.get('days_to_treatment_end'),
                        'treatment_outcome': treatment.get('treatment_outcome')
                    })
                    records.append(treatment_record)
            else:
                records.append(record)
        
        df = pd.DataFrame(records)
        print(f"✓ Retrieved metadata for {df['case_id'].nunique()} unique cases")
        print(f"  Total records (including multiple treatments): {len(df)}")
        
        return df
    
    def upload_metadata(self, project_id: str, metadata_df: pd.DataFrame):
        """Upload metadata CSV to S3"""
        print("Uploading metadata to S3...")
        
        # Save locally first
        local_path = os.path.join(self.temp_dir, f'{project_id}_metadata.csv')
        metadata_df.to_csv(local_path, index=False)
        
        # Upload to S3
        s3_key = f'{project_id}/metadata.csv'
        try:
            self.s3_client.upload_file(
                local_path,
                self.bucket,
                f'raw/{s3_key}'
            )
            print(f"✓ Uploaded metadata: s3://{self.bucket}/raw/{s3_key}")
        except Exception as e:
            print(f"✗ Upload failed: {e}")
        finally:
            os.remove(local_path)
    
    def get_gene_expression_files(self, project_id: str) -> List[Dict]:
        """Get list of all gene expression files for download"""
        filters = {
            "op": "and",
            "content": [
                {
                    "op": "=",
                    "content": {
                        "field": "cases.project.project_id",
                        "value": [project_id]
                    }
                },
                {
                    "op": "=",
                    "content": {
                        "field": "files.data_category",
                        "value": ["Transcriptome Profiling"]
                    }
                },
                {
                    "op": "=",
                    "content": {
                        "field": "files.data_type",
                        "value": ["Gene Expression Quantification"]
                    }
                },
                {
                    "op": "=",
                    "content": {
                        "field": "files.experimental_strategy",
                        "value": ["RNA-Seq"]
                    }
                },
                {
                    "op": "=",
                    "content": {
                        "field": "files.analysis.workflow_type",
                        "value": ["STAR - Counts"]
                    }
                }
            ]
        }
        
        params = {
            "filters": json.dumps(filters),
            "fields": "file_id,file_name,cases.submitter_id,file_size",
            "format": "JSON",
            "size": 10000
        }
        
        response = requests.get(f"{self.base_url}/files", params=params)
        
        if response.status_code != 200:
            print(f"Error: {response.status_code}")
            return []
        
        files = response.json()['data']['hits']
        
        # Calculate total size
        total_size_gb = sum(f['file_size'] for f in files) / (1024**3)
        print(f"✓ Found {len(files)} files")
        print(f"  Total size: {total_size_gb:.2f} GB")
        
        return files
    
    def get_protein_coding_genes(self) -> set:
        """
        Get set of protein-coding gene IDs from GENCODE
        Reduces from ~60K to ~20K genes
        """
        print("  Loading protein-coding gene list...")
        
        # Use cached list if available
        cache_file = os.path.join(self.temp_dir, 'protein_coding_genes.txt')
        
        if os.path.exists(cache_file):
            with open(cache_file) as f:
                genes = set(line.strip() for line in f)
            print(f"  ✓ Loaded {len(genes)} protein-coding genes from cache")
            return genes
        
        # Download from GENCODE
        gencode_url = "https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_22/gencode.v22.annotation.gtf.gz"
        
        try:
            print("  Downloading GENCODE annotation...")
            response = requests.get(gencode_url, stream=True)
            
            protein_coding_genes = set()
            
            # Parse GTF
            for line in response.iter_lines():
                if line:
                    line = line.decode('utf-8')
                    
                    if line.startswith('#'):
                        continue
                    
                    fields = line.split('\t')
                    if len(fields) < 9:
                        continue
                    
                    feature_type = fields[2]
                    attributes = fields[8]
                    
                    if feature_type == 'gene':
                        if 'gene_type "protein_coding"' in attributes:
                            for attr in attributes.split(';'):
                                if 'gene_id' in attr:
                                    gene_id = attr.split('"')[1].split('.')[0]
                                    protein_coding_genes.add(gene_id)
                                    break
            
            # Cache for future use
            with open(cache_file, 'w') as f:
                for gene in sorted(protein_coding_genes):
                    f.write(f"{gene}\n")
            
            print(f"  ✓ Loaded {len(protein_coding_genes)} protein-coding genes")
            return protein_coding_genes
        
        except Exception as e:
            print(f"  Warning: Could not load GENCODE: {e}")
            return set()
    
    def process_files_sequential(self, project_id: str, file_list: List[Dict],
                                 clinical_df: pd.DataFrame, protein_coding_only: bool):
        """Process files one by one"""
        protein_coding = self.get_protein_coding_genes() if protein_coding_only else set()
        
        for idx, file_info in enumerate(file_list, 1):
            print(f"[{idx}/{len(file_list)}] Processing {file_info['file_id'][:8]}...", end=' ')
            
            try:
                self.process_single_file(project_id, file_info, clinical_df, protein_coding)
                print("✓")
            except Exception as e:
                print(f"✗ {e}")
            
            # Rate limiting
            time.sleep(0.1)
    
    def process_files_parallel(self, project_id: str, file_list: List[Dict],
                              clinical_df: pd.DataFrame, max_workers: int,
                              protein_coding_only: bool):
        """Process files in parallel"""
        protein_coding = self.get_protein_coding_genes() if protein_coding_only else set()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_file = {
                executor.submit(
                    self.process_single_file, 
                    project_id, 
                    file_info, 
                    clinical_df,
                    protein_coding
                ): file_info
                for file_info in file_list
            }
            
            completed = 0
            for future in as_completed(future_to_file):
                file_info = future_to_file[future]
                file_id = file_info['file_id'][:8]
                
                try:
                    future.result()
                    completed += 1
                    print(f"✓ [{completed}/{len(file_list)}] {file_id}")
                except Exception as e:
                    print(f"✗ [{completed}/{len(file_list)}] {file_id}: {e}")
    
    def process_single_file(self, project_id: str, file_info: Dict,
                           clinical_df: pd.DataFrame, protein_coding: set):
        """
        Download and process a single gene expression file
        - Download from GDC
        - Filter genes if needed
        - Add patient metadata
        - Upload to S3
        """
        file_id = file_info['file_id']
        file_name = file_info['file_name']
        
        # Get patient ID
        patient_id = file_info['cases'][0]['submitter_id'] if file_info.get('cases') else 'unknown'
        
        # Download file
        url = f"{self.base_url}/data/{file_id}"
        response = requests.get(url, stream=True)
        
        if response.status_code != 200:
            raise Exception(f"Download failed: {response.status_code}")
        
        # Read gene expression data
        if file_name.endswith('.gz'):
            content = gzip.decompress(response.content)
            df = pd.read_csv(io.BytesIO(content), sep='\t', comment='#')
        else:
            df = pd.read_csv(io.BytesIO(response.content), sep='\t', comment='#')
        
        # Clean gene IDs (remove version numbers)
        if 'gene_id' in df.columns:
            df['gene_id_clean'] = df['gene_id'].str.split('.').str[0]
        
        # Filter for protein-coding genes if requested
        if protein_coding:
            df = df[df['gene_id_clean'].isin(protein_coding)]
        
        # Select relevant columns
        # STAR counts format: gene_id, gene_name, gene_type, unstranded, stranded_first, stranded_second
        if 'unstranded' in df.columns:
            expr_df = df[['gene_id_clean', 'gene_name', 'unstranded']].copy()
            expr_df.columns = ['gene_id', 'gene_name', 'expression_value']
        elif 'tpm_unstranded' in df.columns:
            expr_df = df[['gene_id_clean', 'gene_name', 'tpm_unstranded']].copy()
            expr_df.columns = ['gene_id', 'gene_name', 'expression_value']
        else:
            # Fallback: try to find count column
            count_cols = [c for c in df.columns if 'count' in c.lower() or 'tpm' in c.lower()]
            if count_cols:
                expr_df = df[['gene_id_clean', count_cols[0]]].copy()
                expr_df.columns = ['gene_id', 'expression_value']
        
        # Get patient metadata
        patient_meta = clinical_df[clinical_df['patient_id'] == patient_id].iloc[0] if len(clinical_df[clinical_df['patient_id'] == patient_id]) > 0 else {}
        
        # Save locally
        local_path = os.path.join(self.temp_dir, f'{patient_id}.csv')
        expr_df.to_csv(local_path, index=False)
        
        # Upload to S3 with metadata
        s3_key = f'{project_id}/samples/{patient_id}.csv'
        self.upload_to_s3_with_metadata(local_path, s3_key, patient_meta, patient_id)
        
        # Clean up
        os.remove(local_path)
    
    def upload_to_s3_with_metadata(self, local_path: str, s3_key: str,
                                   patient_meta: Dict, patient_id: str):
        """Upload file to S3 with patient metadata in object tags"""
        try:
            # Prepare metadata (S3 metadata values must be strings)
            metadata = {
                'patient-id': str(patient_id),
                'project': s3_key.split('/')[0],
            }
            
            # Add clinical metadata if available
            if isinstance(patient_meta, pd.Series):
                metadata.update({
                    'age': str(patient_meta.get('age_years', 'unknown')),
                    'gender': str(patient_meta.get('gender', 'unknown')),
                    'tumor-stage': str(patient_meta.get('tumor_stage', 'unknown')),
                    'vital-status': str(patient_meta.get('vital_status', 'unknown')),
                })
            
            self.s3_client.upload_file(
                local_path,
                self.bucket,
                f'raw/{s3_key}',
                ExtraArgs={'Metadata': metadata}
            )
            
        except Exception as e:
            raise Exception(f"S3 upload failed: {e}")
    
    def fetch_both_projects(self, parallel: bool = True, max_workers: int = 4,
                           protein_coding_only: bool = False):
        """Download both LUAD and LUSC"""
        for cancer_type, project_id in self.projects.items():
            print(f"\n{'#'*70}")
            print(f"# Starting {cancer_type} ({project_id})")
            print(f"{'#'*70}\n")
            
            self.fetch_project(
                project_id,
                parallel=parallel,
                max_workers=max_workers,
                protein_coding_only=protein_coding_only
            )
            
            print(f"\n✓ Completed {cancer_type}\n")


def main():
    # Configuration
    print("\n" + "="*70)
    parallel = input("Use parallel processing? (y/n) [y]: ").lower() != 'n'
    
    if parallel:
        max_workers = input("Number of parallel workers [4]: ").strip()
        max_workers = int(max_workers) if max_workers else 4
    else:
        max_workers = 1
    
    protein_coding = input("Filter to protein-coding genes only (~20K vs ~60K)? (y/n) [y]: ").lower() != 'n'
    
    print("\n" + "="*70)
    print("Configuration:")
    print(f"  Parallel: {parallel}")
    if parallel:
        print(f"  Workers: {max_workers}")
    print(f"  Gene filter: {'Protein-coding only (~20K)' if protein_coding else 'All genes (~60K)'}")
    print(f"  Estimated size per project: {'~1-1.5 GB' if protein_coding else '~3-4 GB'}")
    print(f"  Total estimated size: {'~2-3 GB' if protein_coding else '~6-7 GB'}")
    print("="*70)
    
    confirm = input("\nProceed with download? (y/n): ").lower()
    if confirm != 'y':
        print("Cancelled.")
        return
    
    # Initialize fetcher
    fetcher = TCGALungFetcher()
    
    # Download both projects
    start_time = time.time()
    fetcher.fetch_both_projects(
        parallel=parallel,
        max_workers=max_workers,
        protein_coding_only=protein_coding
    )
    
    elapsed = time.time() - start_time
    print("\n" + "="*70)
    print("✓ ALL DOWNLOADS COMPLETE!")
    print("="*70)
    print(f"Total time: {elapsed/60:.1f} minutes")
    print(f"\nData uploaded to: s3://{fetcher.bucket}/raw/")
    print("  • TCGA-LUAD/")
    print("  • TCGA-LUSC/")


if __name__ == '__main__':
    main()  