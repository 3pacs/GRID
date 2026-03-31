#!/usr/bin/env node
// gsd-hook-version: grid-custom
// GRID SQL Injection Guard — PreToolUse hook
// Scans Python file content being written for SQL injection patterns.
// Defense-in-depth: catches unsafe SQL string formatting before it enters the codebase.
//
// Triggers on: Write and Edit tool calls targeting .py files
// Action: Advisory warning (does not block) — logs detection for awareness
//
// Why advisory-only: Blocking would prevent legitimate development operations.
// The goal is to surface risky patterns so the developer can verify the code,
// not to create false-positive deadlocks. Developers may have valid reasons
// for dynamic SQL generation (e.g., in tests, in admin-only contexts).
//
// Patterns detected:
// - f-strings with SQL keywords: f"SELECT ... WHERE id = {var}"
// - .format() near SQL keywords: "SELECT ... WHERE id = {}".format(var)
// - String concatenation with SQL: "SELECT * FROM " + table_name
// - % formatting: "SELECT * FROM %s" % table_name
// - INTERVAL/ORDER BY/GROUP BY/HAVING/LIMIT with dynamic values

const fs = require('fs');
const path = require('path');

// SQL injection patterns (checks for risky string formatting patterns)
const SQL_INJECTION_PATTERNS = [
  // f-strings followed by SQL keywords
  /f['""].*?(?:SELECT|INSERT|UPDATE|DELETE|WHERE|FROM|JOIN|INNER|LEFT|RIGHT|OUTER|UNION|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|INTERVAL)\b/is,
  // f-strings with variable interpolation near SQL
  /f['""][^'"]*\{[^}]*\}[^'"]*(?:SELECT|INSERT|UPDATE|DELETE|WHERE|FROM|JOIN|GROUP|ORDER|HAVING|LIMIT|INTERVAL)/is,

  // .format() calls with SQL keywords before or after
  /(?:SELECT|INSERT|UPDATE|DELETE|WHERE|FROM|JOIN|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|INTERVAL)[^'"]*\.format\(/i,
  /\.format\([^)]*\)[^'"]*(?:SELECT|INSERT|UPDATE|DELETE|WHERE|FROM|JOIN|GROUP|ORDER|HAVING|LIMIT)/i,

  // String concatenation patterns
  /['""](?:SELECT|INSERT|UPDATE|DELETE|WHERE|FROM|JOIN|GROUP\s+BY|ORDER\s+BY|HAVING|LIMIT|INTERVAL)[^'"]*['"]\s*\+/i,
  /\+\s*['""](?:SELECT|INSERT|UPDATE|DELETE|WHERE|FROM|JOIN|GROUP|ORDER|HAVING|LIMIT|INTERVAL)/i,

  // % formatting (printf-style) with SQL keywords
  /['""](?:SELECT|INSERT|UPDATE|DELETE|WHERE|FROM|JOIN|INTERVAL)[^'"]*%s/i,
  /['""](?:SELECT|INSERT|UPDATE|DELETE|WHERE|FROM|JOIN)[^'"]*%\(/i,
];

// HIGH-RISK patterns that are almost certainly bugs
const HIGH_RISK_PATTERNS = [
  // INTERVAL with direct string formatting (regex/log.py:241 bug)
  /INTERVAL\s*['""][^'"]*\{[^}]*\}|INTERVAL\s*['""][^'"]*['""]\s*\+|INTERVAL\s*['""][^'"]*\.format/i,
  // .format() with INTERVAL (regime.py:85-93 bug)
  /['""]\s*\.format\s*\(\s*INTERVAL|INTERVAL\s*['""][^'"]*\.format\s*\(/i,
];

let input = '';
const stdinTimeout = setTimeout(() => process.exit(0), 3000);
process.stdin.setEncoding('utf8');
process.stdin.on('data', chunk => input += chunk);
process.stdin.on('end', () => {
  clearTimeout(stdinTimeout);
  try {
    const data = JSON.parse(input);
    const toolName = data.tool_name;

    // Only scan Write and Edit operations
    if (toolName !== 'Write' && toolName !== 'Edit') {
      process.exit(0);
    }

    const filePath = data.tool_input?.file_path || '';

    // Only scan Python files
    if (!filePath.endsWith('.py')) {
      process.exit(0);
    }

    // Get the content being written
    const content = data.tool_input?.content || data.tool_input?.new_string || '';
    if (!content) {
      process.exit(0);
    }

    // Scan for SQL injection patterns
    const findings = [];
    const riskLevel = [];

    // Check high-risk patterns first
    for (const pattern of HIGH_RISK_PATTERNS) {
      if (pattern.test(content)) {
        riskLevel.push(pattern.source);
      }
    }

    // Check standard injection patterns
    for (const pattern of SQL_INJECTION_PATTERNS) {
      if (pattern.test(content)) {
        findings.push(pattern.source);
      }
    }

    if (findings.length === 0 && riskLevel.length === 0) {
      process.exit(0);
    }

    // Build advisory message
    let severityLabel = 'WARNING';
    if (riskLevel.length > 0) {
      severityLabel = 'HIGH-RISK WARNING';
    }

    const output = {
      hookSpecificOutput: {
        hookEventName: 'PreToolUse',
        additionalContext: `\u26a0\ufe0f ${severityLabel}: SQL Injection Pattern Detected\n` +
          `File: ${path.basename(filePath)}\n` +
          `Patterns matched: ${(riskLevel.length > 0 ? riskLevel : findings).length}\n\n` +
          `GRID security rules prohibit dynamic SQL string formatting:\n` +
          `- f-strings with SQL keywords: f"SELECT ... WHERE id = {var}"\n` +
          `- .format() calls near SQL: "SELECT ... ".format(var)\n` +
          `- String concatenation with SQL: "SELECT * FROM " + table\n` +
          `- % formatting near SQL: "SELECT FROM %s" % table\n\n` +
          `Instead, use SQLAlchemy text() with parameterized queries:\n` +
          `  text("SELECT ... WHERE id = :id").bindparams(id=value)\n\n` +
          `If this is test code, admin-only code, or has another valid reason, ` +
          `please add a comment explaining why. For legitimate cases, the advisory ` +
          `will be safely ignored and development can continue.`,
      },
    };

    process.stdout.write(JSON.stringify(output));
  } catch {
    // Silent fail — never block tool execution
    process.exit(0);
  }
});
