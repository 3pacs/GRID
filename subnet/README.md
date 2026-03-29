# GRID Intelligence Subnet (Bittensor)

Financial intelligence research subnet where miners earn TAO by producing
high-quality market research, offshore network analysis, and company profiles.

## Architecture

- **Subnet Owner**: GRID (receives ~18% of emissions)
- **Validators**: GRID server (scores responses against confirmed data)
- **Miners**: Edge users with GPUs (run 7-8B models, earn TAO + API credits)

## How It Works

1. Validator pulls research tasks from `llm_task_backlog` (75K+ tasks)
2. Tasks are distributed to miners via Bittensor protocol
3. Miners run inference and return structured research
4. Validator scores quality using trust_scorer + confidence labels
5. Results stored in encrypted_intelligence (validator-side only)
6. Miners earn TAO proportional to quality scores
7. Best miners also earn GRID API access credits

## Scoring Criteria

- Specificity (names, numbers, dates vs vague statements)
- Accuracy (cross-referenced against ICIJ/SEC/EDGAR confirmed data)
- Structure (follows GRID confidence labeling convention)
- Novelty (new connections not already in the database)
- Consistency (doesn't contradict confirmed facts)
