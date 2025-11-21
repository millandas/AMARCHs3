import gzip

with gzip.open("C:/Users/cestm/Downloads/data_collection/data_collection/fastq_subset/SRR12345678_1.fastq.gz", "rt") as f:
    for i, line in enumerate(f):
        print(line.strip())
        if i > 20:
            break
