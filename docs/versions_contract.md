# Versions & Contact

## Place dans la documentation
Ce document est la reference pour:
- le versioning du projet (code, donnees, corpus)
- les contacts/responsables
- les regles de traceabilite et d'anonymisation
- les etapes de developpement (roadmap)

Emplacement: `docs/versions_contract.md`.

## Versioning
- Code: semver (MAJOR.MINOR.PATCH).
- Corpus: version datee (YYYY-MM-DD) + hash du lot.
- Index: version liee a la version corpus + date d'indexation.
- Regle: aucune reponse ne peut citer une source non versionnee.
- Changelog: obligatoire a chaque livraison.

## Contacts (a completer)
- Responsable fonctionnel: [NOM / ROLE]
- Responsable technique: [NOM / ROLE]
- Securite / RGPD: [NOM / ROLE]
- Maintenance / MCO: [NOM / ROLE]

## Regles d'or (secret fiscal)
- Anonymisation des dossiers des la phase 1.
- Interdiction de stocker ou de reconstituer: nom, adresse, SIREN, identite du gerant, identifiants directs/indirects.
- Toute sortie doit etre non re-identifiante.
- Journalisation sans donnees sensibles.

## Traceabilite d'usage (module obligatoire)
Objectif: savoir qui consulte l'agent, quand, et depuis quelle IP.
- Champs minimaux: horodatage UTC, nom utilisateur, adresse IP, nom de l'agent, action.
- En option: session_id, request_id, user_agent, ressource.
- Regle: aucun contenu de dossier ne doit etre journalise.
- Retention: 12 mois, purge automatique.

## Sources et perimetre (Phase 1)
Sources officielles uniquement:
- CGI (code general des impots)
- BOFiP
- Legifrance (textes consolides)
- Jurisprudence nationale et regionale (CE/CAA/Cass., Conseil d'Etat)

Formats:
- HTML / XML (priorite)
- PDF (OCR si necessaire)

Exclusions phase 1:
- Videos (YouTube) et contenus non officiels: non utilisables comme sources juridiques.

## Extension (Phase 2)
- Assemblee nationale et Senat: les lois promulguees sont des normes (via Legifrance). Les travaux preparatoires, rapports et commissions sont utilisables pour contexte et interpretation, jamais comme substitut a la norme promulguee.
- Sites publics fiables et publications officielles (PDF telechargeables).
- Documentation secondaire: autorisee uniquement pour contexte, jamais pour fonder une conclusion.

## Workflow obligatoire (tra\u00e7abilite sans revelation du raisonnement interne)
1. Qualifier la question (type d'impot, periode, regime, corse ou non).
2. Recuperer les sources officielles pertinentes.
3. Extraire les passages cites (avec identifiants et versions).
4. Verifier la couverture: 100% des assertions citees.
5. Rendre la reponse: citations par phrase, ou "Je ne sais pas" si insuffisant.
6. Journaliser la requete et la reponse (sans donnees sensibles).

## Etapes de developpement
Phase 0 - Cadrage
- Perimetre precis (TVA, IS, IR, regimes, specificites Corse).
- Regles d'anonymisation et tests d'etancheite.
- Choix techniques (stockage, indexation, modele, audit).

Phase 1 - MVP Officiel
- Ingestion CGI + BOFiP + Legifrance + jurisprudence.
- Indexation lexicale + vecteurs (hybride).
- Moteur de reponse extractif avec citations obligatoires.
- CLI minimale (recherche, reponse, citations).
- Module de traceabilite d'usage.

Phase 2 - Jurisprudence et OCR
- Ajout jurisprudence regionale detaillee.
- OCR pour PDF scannes + controle qualite.
- Versioning complet (corpus + index + reponses).

Phase 3 - Conformite & audit
- RBAC (acces par role) + logs WORM.
- Tests "zero hallucination" automatise.
- Exports (PDF par defaut, CSV si besoin).

Phase 4 - Extension sources
- Assemblee nationale / Senat (contexte).
- Sites publics fiables + publications officielles PDF.
- Re-rank et priorisation multi-sources.

Phase 5 - Stabilisation
- Optimisation perf (corpus eleve).
- Documentation complete + procedures MCO.
- Publication GitHub privee (sans donnees sensibles).
