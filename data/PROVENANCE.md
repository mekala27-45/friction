# Provenance

Two published synthetic data sets and a simulator. No real person, card or account.

## IBM TabFormer card transactions

- Source: the IBM TabFormer repository (github.com/IBM/TabFormer), `data/credit_card/transactions.tgz`,
  served through git LFS. Member `card_transaction.v1.csv`.
- Digests: data/sources.lock.json holds the archive and member SHA-256 and sizes; scripts/fetch_sources.py
  refuses a file that differs.
- Terms: the repository is Apache 2.0; the data carries no separate terms and is used here for research
  and demonstration.
- Paper: Padhi et al., "Tabular Transformers for Modeling Multivariate Time Series", ICASSP 2021
  (arXiv 2011.01843). The paper describes more users than the released file holds; DECISIONS.md has the
  measured counts.

## RBA login data set

- Source: das-group/rba-dataset, release v1.0.0, asset `rba-dataset.zip`, member `rba-dataset.csv`.
- Digests: data/sources.lock.json.
- Licence: CC BY 4.0. Cite: Stephan Wiefling, Paul René Jørgensen, Sigurd Thunem, Luigi Lo Iacono,
  "Pump Up Password Security! Evaluating and Enhancing Risk-Based Authentication on a Real-World
  Large-Scale Online Service", ACM Transactions on Privacy and Security, 2022, doi 10.1145/3546069.
- The data set is synthesized from the login behavior of a real online service; addresses and
  identifiers in it are not published by this build past bronze (scripts/scan_for_planted_identifiers.py).

## The simulator

Everything the sources lack, labeled as simulated wherever it appears: chargeback labels with a
lag, friendly fraud reversals, the card lifecycle as a CDC feed with delivery faults, the attacker
models of the RBA papers, and the day 40 incident. Seeded keyed draws; config/simulator.yaml,
config/replay.yaml and config/incident.yaml; digests in data/replay.lock.json.
