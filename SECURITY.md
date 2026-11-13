# Security Policy

GRID is run by one operator. Software agents open and merge most changes, and every
merge to `main` deploys to the production server. This file explains how to report a
vulnerability and what happens after you report it.

## Reporting a vulnerability

**Do not open a public issue, pull request or discussion for a security problem.**

Report it privately through GitHub's vulnerability reporting:

1. Open the repository's **Security** tab.
2. Choose **Report a vulnerability**.
3. Fill in the advisory form.

Only the repository owner can read the report. If the **Report a vulnerability** button
is missing, private reporting is switched off. In that case, open a public issue that
says only "security contact requested", with no details, and the owner will contact you
privately.

Please include:

- the affected file, endpoint or workflow, and the commit SHA you looked at;
- steps to reproduce, or a proof of concept that does no harm;
- the impact you expect (for example, data exposure, code execution on a runner, or an
  auth bypass);
- whether you have told anyone else.

## What happens next

The owner reviews reports personally, so these are targets, not guarantees:

- acknowledgement within 7 days;
- an initial assessment within 14 days;
- for a confirmed issue, a fix on `main` (and so in production) before any public
  disclosure, then a published GitHub security advisory crediting you unless you ask
  not to be named.

## Supported versions

Only the current `main` branch is supported. There are no release branches, and
production always runs the latest deployed `main` commit.

## Scope

In scope:

- code in this repository, including the API (`api/`), auth, and the PWA (`pwa/`);
- GitHub Actions workflows in `.github/workflows/`, especially anything that could run
  untrusted code on a self-hosted runner or reach repository secrets;
- credentials or personal data committed to the repository or its history.

Out of scope:

- denial of service, load testing, or automated scanning against the production site;
- social engineering of the owner or of any service provider;
- the accuracy of research, signals, or trading output (that is a correctness question
  and belongs in an ordinary issue);
- vulnerabilities in third-party dependencies that are already public, unless this
  repository uses them in a way that makes them exploitable here.

## Rules for testing

- Use only accounts and data that belong to you. Do not read, change or delete anyone
  else's data.
- Do not test against the production deployment beyond what you need to confirm the
  issue.
- If you find a credential, report it and do not use it.
- Give the owner reasonable time to fix the issue before you disclose it.
