All notebooks were run using the NVIDIA A100 GPU on Google Colab, which was necessary due to the size of the dataset.

You have to add the file 'embbedings_with_molecule_names.csv' and 'GSE70138_Broad_LINCS_gene_info_2017-03-06.txt' to your Drive otherwise you will not be able tu run the notebook named 'dataset.ipynb'.


Additionally, the aws_access_key_id and aws_secret_access_key for the S3 bucket must be stored in Colab Secrets.

Finally, you have to run the notebooks in this order : 

 1) from_S3_to_Drive(1).ipynb
 2) dataset(1).ipynb
 3) EDA(3).ipynb
 4) prediction(2).ipynb
