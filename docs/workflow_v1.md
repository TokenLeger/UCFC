# Workflow V1 (FR) - commandes a copier/coller

## Objectif
Deployer un agent fiscalite FR/Corse en mode "zero hallucination" avec traceabilite d'usage.

## Prerequis
- Python 3.12+ (3.11 ok pour l'instant)
- Acces internet
- Fichier `.env` rempli si PISTE est utilise
- (Option ML) un environnement virtuel + dependances ML
- (Option PDF) `pypdf` pour lire les guides PDF

## Dossiers importants
- Donnees brutes: `data_fiscale/raw`
- Donnees normalisees: `data_fiscale/processed/<version_id>/normalized`
- Index ML: `data_fiscale/index/vector`
- Fiches: `fiches/` (a la racine)

## Sources officielles V1
Public:
- BOFiP open data
- LEGI open data (DILA): textes consolides, CGI, codes, lois et reglements
PISTE (optionnel):
- Legifrance API
- JUDILIBRE (jurisprudence Cour de cassation)
- Justice back (API JSON MJ)

## Configuration PISTE (.env)
1. Copier `.env.example` vers `.env`.
2. Renseigner `PISTE_CLIENT_ID` et `PISTE_CLIENT_SECRET`.
3. Optionnel: forcer les URLs si besoin.

Exemple (sandbox):
```bash
PISTE_ENV=sandbox
PISTE_API_BASE=https://sandbox-api.piste.gouv.fr/dila/legifrance/lf-engine-app
PISTE_TOKEN_URL=https://sandbox-oauth.piste.gouv.fr/api/oauth/token
PISTE_SCOPE=openid
PISTE_CLIENT_ID=your_client_id
PISTE_CLIENT_SECRET=your_client_secret
PISTE_AUTH_FLOW=client_credentials
PISTE_AUTH_URL=https://sandbox-oauth.piste.gouv.fr/api/oauth/authorize
PISTE_REDIRECT_URI=http://localhost:8787/callback
PISTE_TOKEN_CACHE=data_fiscale/auth/piste_token.json
PISTE_ACCESS_TOKEN=
# Optionnel: certaines APIs (ex: JUDILIBRE) exigent une API Key en header KeyId
PISTE_API_KEY=your_api_key
PISTE_API_KEY_HEADER=KeyId
```

Notes:
- Le swagger local est dans `loader/API_docs/`.
- Si 403: verifier CGU + souscription de l'API dans PISTE.
- Légifrance utilise le flow OAuth `access_code` (pas `client_credentials`).

## Commandes essentielles (copier/coller)
Sur Windows: remplacer `python3` par `py -3`.

### 1) Telechargements open data
BOFiP:
```bash
python3 pfc_cli.py bofip-download --out data_fiscale/raw/bofip --verbose
```

LEGI (DILA):
```bash
python3 pfc_cli.py legi-download --list
python3 pfc_cli.py legi-download --mode latest --out data_fiscale/raw/legi --verbose
python3 pfc_cli.py legi-download --mode all --limit 100 --out data_fiscale/raw/legi --verbose
```

### 2) Normalisation JSONL (versioning + normalisation)
```bash
python3 pfc_cli.py ingest-v1 --raw data_fiscale/raw --out data_fiscale/processed --workers 8 --verbose
```

### 3) Orchestrateur V1 (one-shot)
Sans PISTE:
```bash
python3 pfc_cli.py orchestrate-v1 --legi-open-data --skip-legifrance --skip-judilibre --skip-justice-back --verbose
```

Avec PISTE:
```bash
python3 pfc_cli.py orchestrate-v1 --verbose
```

### 4) Recherche extractive (sans ML)
```bash
python3 pfc_cli.py qa-search --query "impot sur le revenu" --source bofip --limit 5
```

### 5) Index ML (vectoriel) + recherche
Installer une fois:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-ml.txt
```

Construire l'index (recommande CGI + BOFiP):
```bash
python3 pfc_cli.py legi-extract-cgi --verbose
python3 pfc_cli.py qa-index --source bofip --source legi_cgi --batch-size 128 --max-chars 1500 --verbose --log-every 2000 --overwrite
```

Construire l'index (LEGI complet):
```bash
python3 pfc_cli.py qa-index --source bofip --source legi --batch-size 128 --max-chars 1500 --verbose --log-every 2000 --overwrite
```

Recherche ML:
```bash
python3 pfc_cli.py qa-search-vec --query "impot sur le revenu" --limit 5
```

### 6) Fiches (Markdown + JSON)
Fiches classiques (textes longs):
```bash
python3 pfc_cli.py fiches-build --limit 20 --verbose --max-text-chars 8000
```

Fiches avec index ML (structurees: Cadre legal + Doctrine + Extraits cles):
```bash
python3 pfc_cli.py fiches-build --use-vector --vector-index data_fiscale/index/vector --limit 20 --verbose --max-text-chars 8000
```

Ajuster le volume par section:
```bash
python3 pfc_cli.py fiches-build --use-vector --vector-index data_fiscale/index/vector --cadre-limit 10 --doctrine-limit 10 --cles-limit 10 --verbose --max-text-chars 8000
```

Texte complet (attention volume):
```bash
python3 pfc_cli.py fiches-build --limit 20 --verbose --max-text-chars 0
```

### 7) Extraction CGI (depuis LEGI JSONL)
```bash
python3 pfc_cli.py legi-extract-cgi --verbose
```

### 8) Extraire du texte depuis un PDF (guide PISTE)
Installer:
```bash
source .venv/bin/activate
pip install pypdf
```
Extraire:
```bash
python3 pfc_cli.py pdf-extract --in loader/API_docs/PISTE-Guide_Utilisateur.pdf --out /tmp/piste_guide.txt --pages 1-5
```

### 9) Indexer une inbox documents (RAG / ML)
But: convertir les documents dans `data_fiscale/pdf/ucfc_pdf_inbox/batch_*/` en JSONL (chunks) pour recherche extractive + vectorielle (PDF, DOCX, CSV, XLSX, TXT). Formats `.doc`/`.xls` non supportes (convertir en `.docx`/`.xlsx`/`.csv`).

Telecharger un batch de documents (liste d'URLs ou CSV manifest):
```bash
python3 pfc_cli.py pdf-batch --urls /chemin/vers/urls.txt --source "Legifrance/JUDILIBRE" --doc-type jurisprudence --jurisdiction FR --priority high --verbose
```
Ou via CSV (avec colonne `url`, optionnellement `title`, `year`, `source`, `doc_type`, `keywords`, `stream`, etc.):
```bash
python3 pfc_cli.py pdf-batch --manifest /chemin/vers/manifest.csv --verbose
```
Plusieurs streams dans un seul batch (chaque stream peut etre nomme):
```bash
python3 pfc_cli.py pdf-batch --manifest cjue=/chemin/cjue.csv --manifest cc=/chemin/cc.csv --urls ce=/chemin/ce_urls.txt --verbose
```
Astuce: si `--doc-type` n'est pas fourni, il est infere selon l'extension (dataset pour CSV/XLSX, document pour DOCX/TXT, jurisprudence pour PDF).
Les colonnes `stream` et `keywords` (si presentes) sont conservees dans les JSONL pour filtrage ou scoring.

Normaliser (ecrit `pdf_inbox.jsonl` dans la derniere version `data_fiscale/processed/<version_id>/normalized/`):
```bash
source .venv/bin/activate
pip install pypdf
python3 pfc_cli.py pdf-normalize --verbose
```
Note: `pypdf` est requis pour les PDFs, pas pour DOCX/CSV/XLSX/TXT.

Indexer (ML):
```bash
python3 pfc_cli.py qa-index --source pdf_inbox --batch-size 64 --max-chars 2500 --verbose --overwrite
```

Rechercher:
```bash
python3 pfc_cli.py qa-search-vec --query "TVA" --source pdf_inbox --limit 5
```

## Legifrance (PISTE) - endpoints et appels
Lister endpoints:
```bash
python3 pfc_cli.py legifrance-list-paths --filter /consult
```

Appeler un endpoint POST (body JSON):
```bash
python3 pfc_cli.py legifrance-fetch --path /search --method POST --body-file /chemin/vers/body.json --out data_fiscale/raw/legifrance --name search --verbose
```

Appel auto-methode via swagger:
```bash
python3 pfc_cli.py legifrance-fetch --path /consult/getArticleWithIdEliOrAlias --body-file /chemin/vers/body.json --out data_fiscale/raw/legifrance --name article
```

### OAuth accessCode (Légifrance)
1) Générer l'URL d'auth:
```bash
python3 pfc_cli.py legifrance-auth --redirect-uri http://localhost:8787/callback
```
2) Se connecter, récupérer le `code` et échanger:
```bash
python3 pfc_cli.py legifrance-auth --redirect-uri http://localhost:8787/callback --code "CODE"
```
Ensuite `legifrance-fetch` utilisera le token stocké dans `data_fiscale/auth/piste_token.json`.

## Endpoints Legifrance V1 (swagger local)
Source: `loader/API_docs/Legifrance*.json`

### /chrono
- GET /chrono/ping
- POST /chrono/textCid
- GET /chrono/textCid/{textCid}
- POST /chrono/textCidAndElementCid

### /consult
- POST /consult/acco
- POST /consult/circulaire
- POST /consult/cnil
- POST /consult/code
- POST /consult/code/tableMatieres
- POST /consult/concordanceLinksArticle
- POST /consult/debat
- POST /consult/dossierLegislatif
- POST /consult/eliAndAliasRedirectionTexte
- POST /consult/getArticle
- POST /consult/getArticleByCid
- POST /consult/getArticleWithIdAndNum
- POST /consult/getArticleWithIdEliOrAlias
- POST /consult/getBoccTextPdfMetadata
- POST /consult/getCnilWithAncienId
- POST /consult/getCodeWithAncienId
- POST /consult/getJoWithNor
- POST /consult/getJuriPlanClassement
- POST /consult/getJuriWithAncienId
- POST /consult/getSectionByCid
- POST /consult/getTables
- POST /consult/hasServicePublicLinksArticle
- POST /consult/jorf
- POST /consult/jorfCont
- POST /consult/jorfPart
- POST /consult/juri
- POST /consult/kaliArticle
- POST /consult/kaliCont
- POST /consult/kaliContIdcc
- POST /consult/kaliSection
- POST /consult/kaliText
- POST /consult/lastNJo
- POST /consult/lawDecree
- POST /consult/legi/tableMatieres
- POST /consult/legiPart
- GET /consult/ping
- POST /consult/relatedLinksArticle
- POST /consult/sameNumArticle
- POST /consult/servicePublicLinksArticle

### /list
- POST /list/bocc
- POST /list/boccTexts
- POST /list/boccsAndTexts
- POST /list/bodmr
- POST /list/code
- POST /list/conventions
- POST /list/debatsParlementaires
- POST /list/docsAdmins
- POST /list/dossiersLegislatifs
- POST /list/legislatures
- POST /list/loda
- GET /list/ping
- POST /list/questionsEcritesParlementaires

### /misc
- GET /misc/commitId
- GET /misc/datesWithoutJo
- GET /misc/yearsWithoutTable

### /search
- POST /search
- POST /search/canonicalArticleVersion
- POST /search/canonicalVersion
- POST /search/nearestVersion
- GET /search/ping

### /suggest
- POST /suggest
- POST /suggest/acco
- POST /suggest/pdc
- GET /suggest/ping
