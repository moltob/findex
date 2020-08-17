# File Index and Comparison

## Index

First index:

    python -m findex.cli index -db index-h.db \\?\H:\

Second index:

    python -m findex.cli index -db index-z.db \\?\Z:\


## Comparison

    python -m findex.cli compare -db comparison-h-z.db index-h.db index-z.db
    
## Reporting

    python -m findex.cli report -xlsx comparison-h-z.xlsx comparison-h-z.db
