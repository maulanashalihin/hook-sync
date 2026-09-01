// Post-build script: inject OG image meta tags into all HTML files
import { readFile, writeFile, readdir } from 'node:fs/promises';
import { join } from 'node:path';

const siteURL = 'https://hook-sync.pages.dev';
const distDir = 'dist';

// Recursively find all index.html files (except in og/ directory)
async function findHtmlFiles(dir, base = '') {
  const files = [];
  const entries = await readdir(dir, { withFileTypes: true });
  for (const entry of entries) {
    const fullPath = join(dir, entry.name);
    const relPath = base ? `${base}/${entry.name}` : entry.name;
    if (entry.isDirectory()) {
      if (relPath === 'og') continue;
      files.push(...await findHtmlFiles(fullPath, relPath));
    } else if (entry.name === 'index.html') {
      files.push(relPath);
    }
  }
  return files;
}

const files = await findHtmlFiles(distDir);

let injected = 0;
for (const file of files) {
  const filePath = join(distDir, file);
  const html = await readFile(filePath, 'utf-8');

  // Calculate OG image path: /og/<page-slug>.png
  // e.g., dist/getting-started/overview/index.html → /og/getting-started/overview.png
  const pageSlug = file.replace('/index.html', '');
  const ogImage = pageSlug === '' ? '/og/getting-started/overview.png' : `/og/${pageSlug}.png`;
  const ogImageURL = `${siteURL}${ogImage}`;

  // Check if OG image meta already exists
  if (html.includes('og:image')) continue;

  // Insert before </head>
  const metaTags = `
    <meta property="og:image" content="${ogImageURL}" />
    <meta name="twitter:card" content="summary_large_image" />
    <meta name="twitter:image" content="${ogImageURL}" />`;

  const updated = html.replace('</head>', `${metaTags}\n  </head>`);

  if (updated !== html) {
    await writeFile(filePath, updated, 'utf-8');
    console.log(`  ✓ ${file} → ${ogImage}`);
    injected++;
  }
}

console.log(`\nDone: OG meta tags injected into ${injected} pages`);
