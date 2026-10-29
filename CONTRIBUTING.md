# Contributing

Read AGENTS.md first: setup, the tests and gates, the rules, and the files that are never edited by
hand. Commits follow conventional commits (feat, fix, docs, test, ci, perf, chore). Every pull request
runs ci.yml: lint, types, tests with an 80 percent coverage floor, the fixture pipeline, the notebooks,
the gates and the claim gate. A change to evaluation/plan.yaml or evaluation/hypotheses.yaml needs a
dated DECISIONS.md entry and a new lock, and invalidates every graded number.
