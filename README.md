# UCFC — Agent Fiscalite FR/Corse

Ce depot contient les bases d'un agent fiscalite francaise et corse en mode "zero hallucination".

## Documentation
- `docs/versions_contract.md` : versioning, contacts, regles d'or, workflow, roadmap.
- `docs/workflow_v1.md` : workflow V1 (FR) et execution CLI.
- `docs/mermaid/` : diagrammes Mermaid (architecture, workflow v1).

## Configuration (PISTE)
- Copier `.env.example` vers `.env` et renseigner `PISTE_CLIENT_ID` et `PISTE_CLIENT_SECRET`.
- Ne jamais committer `.env`.

## CLI V1
- BOFiP: `python3 pfc_cli.py bofip-download --out data_fiscale/raw/bofip`
- LEGI (open data DILA): `python3 pfc_cli.py legi-download --mode full --out data_fiscale/raw/legi --verbose`
- Legifrance (POST + JSON): `python3 pfc_cli.py legifrance-fetch --path /search --method POST --body-file /chemin/vers/body.json --out data_fiscale/raw/legifrance --verbose`
- Lister endpoints Legifrance: `python3 pfc_cli.py legifrance-list-paths --filter /consult`
- Versioning: `python3 pfc_cli.py ingest-version --raw data_fiscale/raw --out data_fiscale/processed`
- Pipeline V1 (versioning + JSONL): `python3 pfc_cli.py ingest-v1 --raw data_fiscale/raw --out data_fiscale/processed --verbose`
- Extraction CGI (LEGI): `python3 pfc_cli.py legi-extract-cgi --verbose`
- Orchestrateur V1 (one-shot): `python3 pfc_cli.py orchestrate-v1 --verbose`
  - Inclut Legifrance, JUDILIBRE, Justice back (PISTE) si autorises
- Orchestrateur V1 (open data LEGI): `python3 pfc_cli.py orchestrate-v1 --legi-open-data --skip-legifrance --skip-judilibre --skip-justice-back --verbose`
- PDF inbox -> JSONL: `python3 pfc_cli.py pdf-normalize --verbose` (necessite `pypdf`)

## Modules
- `loader/usage_log.py` : journalisation d'usage (qui, quand, IP), sans donnees sensibles.
- `loader/connectors/bofip_downloader.py` : telechargement BOFiP (open data).
- `loader/connectors/legifrance_piste.py` : client PISTE (API Legifrance).
- `loader/pipeline_ingest.py` : versioning du corpus brut (manifest + hash).
- `loader/pipeline_v1.py` : versioning + normalisation JSONL.
- `loader/normalizers/jsonl_normalizer.py` : normalisation JSONL generique.

## Notes
- Ce depot ne doit jamais contenir de donnees sensibles ou re-identifiantes.
