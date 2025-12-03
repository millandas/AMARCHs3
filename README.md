Dear reader,

Welcome to our project repository on **data collection, storage, and integration**, where we investigate whether molecular structure data—represented using the **Simplified Molecular Input Line Entry System (SMILES)**—can be successfully combined with **genome-wide gene expression signatures** to form a unified dataset that reflects drug-induced cellular perturbations.
Our primary objective is to assess whether the **LINCS L1000 dataset**, enriched with molecular embeddings, provides a solid foundation for future predictive modeling of drug responses.

The repository is organized as follows:

1. **`script` folder**
   Contains all data-collection scripts as well as the code used to upload data to the S3 bucket.
   *(Note: You will need credentials similar to those in the `.env.example` file. The dataset consists of ~5 GB of compressed data.)*

2. **`notebook` folder**
   Includes all preprocessing, exploratory data analysis (EDA), and predictive modeling notebooks.
   You will also find instructions to access the final cleaned dataset.
   *(Training was performed on Google Colab using an A100 GPU.)*

3. **`weights` folder**
   Stores the final trained model used in our experiments.
   *(A Random Forest with 10 trees was used due to memory constraints.)*

4. **`mol-insight` folder**
   Contains a web application that visualizes the results and insights produced in this project. (version with our preds on empty-branch)
   To launch locally:

   ```bash
   npm install  
   npm run dev
   ```

Best regards,
**Millan Das, Arthur de Leusse & Alexis Vannson**
