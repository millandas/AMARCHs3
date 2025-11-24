docker run --rm -v "C:\Users\cestm\Downloads\data_collection\data_collection:/data" mysalmon salmon quant `
  -i /data/ref/salmon_index `
  -l A `
  -r /data/fastq_subset/SRR5227313_1.fastq.gz `
  -o /data/SRR5227313_quant
