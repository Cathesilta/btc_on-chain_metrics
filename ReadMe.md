# Abstract

This repo includes workflow from indexing Bitcoin data to metrics 


## Env

### Hardware
- Consumer level computer

### System
- OS: Ubuntu 24.04
- kernel: 6.14.0-37-generic

### Runtime
- Python: 3.12.3
- PostgreSQL: psql (PostgreSQL) 16.11 (Ubuntu 16.11-0ubuntu0.24.04.1)
- bitcoin-cli: Bitcoin Core RPC client version v30.0.0

### Dependencies

```
pip install -r requirements.txt
```

## Indexing

- Retrieve data from Bitcoin raw blocks and make them easier to query.
- For more instruction, look to indexing/ReadMe.md

## Metrics

- Do the Metrics from indexed data. 
- Look at metrics folder.



## Helpers

- Postgres is heavily used. Helpers includes table operations.  