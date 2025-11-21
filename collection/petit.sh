mkdir fastq_subset -ErrorAction SilentlyContinue

Get-Content run.txt | ForEach-Object {
    $srr = $_.Trim()
    if ($srr -ne "") {
        Write-Host "Processing $srr ..."
        prefetch $srr
        fastq-dump $srr `
            -X 100000 `
            --split-files `
            --gzip `
            --outdir fastq_subset
    }
}
