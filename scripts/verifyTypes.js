/* eslint-disable no-console */
const fs = require('fs');
const path = require('path');

const TYPES_DIR = path.resolve(__dirname, '..', 'types');

function collectDtsFiles(dir) {
  const results = [];
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const fullPath = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      results.push(...collectDtsFiles(fullPath));
    } else if (entry.name.endsWith('.d.ts')) {
      results.push(fullPath);
    }
  }
  return results;
}

function extractImportPaths(content) {
  const paths = [];
  // Match: import ... from '...' or import ... from "..."
  const fromRegex = /from\s+['"]([^'"]+)['"]/g;
  // Match: import '...' or import "..."
  const bareRegex = /^import\s+['"]([^'"]+)['"]/gm;

  let match;
  while ((match = fromRegex.exec(content)) !== null) {
    paths.push(match[1]);
  }
  while ((match = bareRegex.exec(content)) !== null) {
    paths.push(match[1]);
  }
  return paths;
}

function isRelativePath(importPath) {
  return importPath.startsWith('./') || importPath.startsWith('../');
}

function resolveImport(importPath, fromFile) {
  const dir = path.dirname(fromFile);
  // Strip .js extension if present (TS resolves .js to .d.ts)
  const cleaned = importPath.replace(/\.js$/, '');
  const resolved = path.resolve(dir, cleaned);

  // Check: exact .d.ts, /index.d.ts, exact .ts, /index.ts
  const candidates = [
    resolved + '.d.ts',
    path.join(resolved, 'index.d.ts'),
    resolved + '.ts',
    path.join(resolved, 'index.ts'),
  ];

  return candidates.some(c => fs.existsSync(c));
}

const files = collectDtsFiles(TYPES_DIR);
const missing = [];

for (const file of files) {
  const content = fs.readFileSync(file, 'utf-8');
  const imports = extractImportPaths(content);

  for (const imp of imports) {
    if (!isRelativePath(imp)) { continue; }
    if (!resolveImport(imp, file)) {
      missing.push({ file: path.relative(TYPES_DIR, file), import: imp });
    }
  }
}

if (missing.length > 0) {
  console.error(`Found ${missing.length} unresolved type import(s):\n`);
  for (const { file, import: imp } of missing) {
    console.error(`  ${file} → ${imp}`);
  }
  process.exit(1);
} else {
  console.log(`All imports in ${files.length} .d.ts files resolve correctly.`);
}
