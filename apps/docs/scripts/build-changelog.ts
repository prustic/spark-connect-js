// Generates apps/docs/src/content/docs/changelog.mdx from packages/*/CHANGELOG.md.
// Runs with Node 22+ native TypeScript stripping; no bundler, no regex.
//
// Data sources:
//   - packages/*/CHANGELOG.md  -> bullets and reference links (changesets output)
//   - git tag list             -> release dates (tag committer date)
//   - npm / GitHub URLs        -> rendered links in the page

import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const DOCS_ROOT = resolve(HERE, "..");
const REPO_ROOT = resolve(DOCS_ROOT, "..", "..");

const REPO_URL = "https://github.com/prustic/spark-connect-js";
const PR_PREFIX = `${REPO_URL}/pull/`;
const COMMIT_PREFIX = `${REPO_URL}/commit/`;

interface Package {
  readonly name: string;
  readonly dir: string;
}

const PACKAGES: readonly Package[] = [
  { name: "@spark-connect-js/core", dir: "packages/spark-core" },
  { name: "@spark-connect-js/node", dir: "packages/spark-node" },
  { name: "@spark-connect-js/connect", dir: "packages/spark-connect" },
];

interface VersionEntry {
  readonly bullets: string[];
  readonly prs: Set<string>;
  readonly commits: Set<string>;
}

function newEntry(): VersionEntry {
  return { bullets: [], prs: new Set(), commits: new Set() };
}

const CHANGE_SECTIONS = new Set(["Major Changes", "Minor Changes", "Patch Changes"]);

// The credit line changesets emits ends with "...)! - " followed by the first
// body line. Everything after that marker is user-facing text.
const CREDIT_JOIN = ")! - ";

// changesets owns the CHANGELOG.md format, so this parser tracks heading state
// line by line. User-facing bullets from any change section are kept.
function parseChangelog(text: string): Map<string, VersionEntry> {
  const versions = new Map<string, VersionEntry>();
  let current: VersionEntry | undefined;
  let inChanges = false;

  for (const raw of text.split("\n")) {
    const line = raw.trimEnd();

    if (line.startsWith("## ") && !line.startsWith("### ")) {
      const version = line.slice(3).trim();
      if (isSemver(version)) {
        current = newEntry();
        versions.set(version, current);
        inChanges = false;
      }
      continue;
    }

    if (!current) {
      continue;
    }

    if (line.startsWith("### ")) {
      inChanges = CHANGE_SECTIONS.has(line.slice(4).trim());
      continue;
    }

    if (!inChanges || line.includes("Updated dependencies")) {
      continue;
    }

    collectLinks(line, current);

    // Nested user-facing bullets are indented with two spaces.
    if (line.startsWith("  - ")) {
      pushBullet(current, line.slice(4));
      continue;
    }

    // changesets joins the body's first line onto the credit line. Hand-edited
    // entries move it to an indented bullet instead, so the marker may be
    // followed by nothing.
    if (line.startsWith("- [")) {
      const join = line.indexOf(CREDIT_JOIN);
      if (join !== -1) {
        pushBullet(current, line.slice(join + CREDIT_JOIN.length));
      }
    }
  }

  return versions;
}

function pushBullet(entry: VersionEntry, text: string): void {
  const bullet = text.trim();

  // A joined first line that is itself a bullet starts with its own "- ".
  const cleaned = bullet.startsWith("- ") ? bullet.slice(2).trim() : bullet;

  // Skip workspace dependency-bump bullets like "@spark-connect-js/core@0.3.0".
  if (cleaned && !cleaned.startsWith("@spark-connect-js/")) {
    entry.bullets.push(cleaned);
  }
}

function isSemver(value: string): boolean {
  const parts = value.split(".");
  if (parts.length !== 3) {
    return false;
  }
  return parts.every((p) => p.length > 0 && [...p].every((c) => c >= "0" && c <= "9"));
}

// Markdown link target sits between `](` and `)`. Walk the line and pluck them
// without a regex.
function collectLinks(line: string, entry: VersionEntry): void {
  let cursor = 0;
  let start = line.indexOf("](", cursor);

  while (start !== -1) {
    const end = line.indexOf(")", start + 2);
    if (end === -1) {
      return;
    }

    const target = line.slice(start + 2, end);
    if (target.startsWith(PR_PREFIX)) {
      entry.prs.add(target.slice(PR_PREFIX.length));
    } else if (target.startsWith(COMMIT_PREFIX)) {
      entry.commits.add(target.slice(COMMIT_PREFIX.length));
    }

    cursor = end + 1;
    start = line.indexOf("](", cursor);
  }
}

// Each release has a `@spark-connect-js/node@VERSION` git tag created by
// changesets on publish. Use that as the canonical release date.
function tagDates(versions: readonly string[]): Map<string, string> {
  const dates = new Map<string, string>();

  for (const version of versions) {
    try {
      const iso = execFileSync(
        "git",
        ["log", "-1", "--format=%cI", `@spark-connect-js/node@${version}`],
        { cwd: REPO_ROOT, encoding: "utf8" },
      ).trim();

      if (iso) {
        dates.set(version, iso.slice(0, 10));
      }
    } catch {
      // Tag missing (pre-release local build): leave undated.
    }
  }

  return dates;
}

function compareSemverDesc(a: string, b: string): number {
  const pa = a.split(".").map(Number);
  const pb = b.split(".").map(Number);

  const major = pb[0] - pa[0];
  if (major !== 0) {
    return major;
  }

  const minor = pb[1] - pa[1];
  if (minor !== 0) {
    return minor;
  }

  return pb[2] - pa[2];
}

function formatDate(iso: string | undefined): string {
  if (!iso) {
    return "";
  }
  return new Date(`${iso}T00:00:00Z`).toLocaleDateString("en-GB", {
    year: "numeric",
    month: "long",
    day: "numeric",
    timeZone: "UTC",
  });
}

function encodeTag(version: string, pkg: string): string {
  return encodeURIComponent(`${pkg}@${version}`);
}

function render(
  byPackage: ReadonlyMap<string, Map<string, VersionEntry>>,
  dates: ReadonlyMap<string, string>,
  versions: readonly string[],
): string {
  const lines: string[] = [];
  pushFrontmatter(lines);

  for (let i = 0; i < versions.length; i++) {
    pushRelease(lines, versions[i], i === 0, byPackage, dates.get(versions[i]));
  }

  return lines.join("\n");
}

function pushFrontmatter(lines: string[]): void {
  lines.push(
    "---",
    "title: Changelog",
    "description: Release history for the @spark-connect-js packages.",
    "---",
    "",
    "import { Badge } from '@astrojs/starlight/components';",
    "import PackageHeader from '../../components/changelog/PackageHeader.astro';",
    "",
    "The three packages version and ship together. Per-package changelogs live alongside the source:",
    "",
  );

  for (const pkg of PACKAGES) {
    lines.push(`- [\`${pkg.name}\`](${REPO_URL}/blob/main/${pkg.dir}/CHANGELOG.md)`);
  }

  lines.push("");
}

function pushRelease(
  lines: string[],
  version: string,
  isLatest: boolean,
  byPackage: ReadonlyMap<string, Map<string, VersionEntry>>,
  date: string | undefined,
): void {
  lines.push(`## ${version}`, "", renderChips(version, date, isLatest), "");

  const refs = renderReferences(version, byPackage);
  if (refs) {
    lines.push(refs, "");
  }

  for (const pkg of PACKAGES) {
    const entry = byPackage.get(pkg.name)?.get(version);
    if (!entry || entry.bullets.length === 0) {
      continue;
    }

    lines.push(renderPackageHeader(pkg, version), "");
    for (const bullet of entry.bullets) {
      lines.push(`- ${bullet}`);
    }
    lines.push("");
  }
}

function renderChips(version: string, date: string | undefined, isLatest: boolean): string {
  const chips: string[] = [];

  const displayDate = formatDate(date);
  if (displayDate) {
    chips.push(`*${displayDate}*`);
  }

  const tagUrl = `${REPO_URL}/releases?q=${encodeURIComponent(version)}&expanded=true`;
  chips.push(`[GitHub release](${tagUrl})`);

  if (isLatest) {
    chips.push('<Badge text="Latest" variant="success" />');
  }

  return chips.join(" · ");
}

function renderReferences(
  version: string,
  byPackage: ReadonlyMap<string, Map<string, VersionEntry>>,
): string {
  const prs = new Set<string>();
  const commits = new Set<string>();

  for (const pkg of PACKAGES) {
    const entry = byPackage.get(pkg.name)?.get(version);
    if (!entry) {
      continue;
    }

    for (const pr of entry.prs) {
      prs.add(pr);
    }
    for (const sha of entry.commits) {
      commits.add(sha);
    }
  }

  if (prs.size === 0 && commits.size === 0) {
    return "";
  }

  const items: string[] = [];

  const sortedPrs = [...prs].sort((a, b) => Number(a) - Number(b));
  for (const pr of sortedPrs) {
    items.push(`<a href="${REPO_URL}/pull/${pr}">#${pr}</a>`);
  }

  for (const sha of commits) {
    items.push(`<a href="${REPO_URL}/commit/${sha}"><code>${sha.slice(0, 7)}</code></a>`);
  }

  return `<p class="release-references"><strong>References:</strong> ${items.join(" · ")}</p>`;
}

function renderPackageHeader(pkg: Package, version: string): string {
  const npmUrl = `https://www.npmjs.com/package/${pkg.name}/v/${version}`;
  const githubUrl = `${REPO_URL}/releases/tag/${encodeTag(version, pkg.name)}`;
  return `<PackageHeader name={${JSON.stringify(pkg.name)}} version={${JSON.stringify(version)}} npmUrl={${JSON.stringify(npmUrl)}} githubUrl={${JSON.stringify(githubUrl)}} />`;
}

function main(): void {
  const byPackage = new Map<string, Map<string, VersionEntry>>();
  const seen = new Set<string>();

  for (const pkg of PACKAGES) {
    const text = readFileSync(join(REPO_ROOT, pkg.dir, "CHANGELOG.md"), "utf8");
    const versions = parseChangelog(text);
    byPackage.set(pkg.name, versions);

    for (const v of versions.keys()) {
      seen.add(v);
    }
  }

  const versions = [...seen].sort(compareSemverDesc);
  const output = render(byPackage, tagDates(versions), versions);

  const dest = join(DOCS_ROOT, "src/content/docs/changelog.mdx");
  writeFileSync(dest, output);
  console.log(`[changelog] wrote ${versions.length} release(s) -> ${dest}`);
}

main();
