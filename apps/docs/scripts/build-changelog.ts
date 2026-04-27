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

const REPO = "prustic/spark-connect-js";
const REPO_URL = `https://github.com/${REPO}`;

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

const HEADING_VERSION = "## ";
const HEADING_SECTION = "### ";
const SECTION_MINOR = "Minor Changes";
const SECTION_PATCH = "Patch Changes";
const UPDATED_DEPS = "Updated dependencies";

// Parse changeset-generated CHANGELOG.md with line-based state. The structure
// is stable because changesets owns the rendering.
function parseChangelog(text: string): Map<string, VersionEntry> {
  const versions = new Map<string, VersionEntry>();
  let current: VersionEntry | undefined;
  let inMinor = false;

  for (const raw of text.split("\n")) {
    const line = raw.trimEnd();

    if (line.startsWith(HEADING_VERSION) && !line.startsWith(HEADING_SECTION)) {
      const version = line.slice(HEADING_VERSION.length).trim();
      if (isSemver(version)) {
        current = { bullets: [], prs: new Set(), commits: new Set() };
        versions.set(version, current);
        inMinor = false;
      }
      continue;
    }

    if (!current) continue;

    if (line.startsWith(HEADING_SECTION)) {
      const section = line.slice(HEADING_SECTION.length).trim();
      inMinor = section === SECTION_MINOR;
      continue;
    }

    if (!inMinor) continue;

    collectLinks(line, current);

    // Nested user-facing bullets are indented with two spaces.
    if (line.startsWith("  - ") && !line.includes(UPDATED_DEPS)) {
      const bullet = line.slice(4).trim();
      if (bullet && !looksLikeDependencyBump(bullet)) {
        current.bullets.push(bullet);
      }
    }
  }

  return versions;
}

function isSemver(value: string): boolean {
  const parts = value.split(".");
  if (parts.length !== 3) return false;
  return parts.every((p) => p.length > 0 && [...p].every((c) => c >= "0" && c <= "9"));
}

function looksLikeDependencyBump(bullet: string): boolean {
  // Examples: "@spark-connect-js/core@0.3.0", "@spark-connect-js/connect@0.3.0"
  return bullet.startsWith("@spark-connect-js/");
}

// Pull references out of markdown link targets without a regex.
// Markdown link target is always between `](` and `)`.
function collectLinks(line: string, entry: VersionEntry): void {
  const marker = "](";
  let cursor = 0;
  while (true) {
    const start = line.indexOf(marker, cursor);
    if (start === -1) return;
    const end = line.indexOf(")", start + marker.length);
    if (end === -1) return;
    const target = line.slice(start + marker.length, end);
    classifyTarget(target, entry);
    cursor = end + 1;
  }
}

function classifyTarget(target: string, entry: VersionEntry): void {
  const pullMarker = `${REPO_URL}/pull/`;
  const commitMarker = `${REPO_URL}/commit/`;
  if (target.startsWith(pullMarker)) {
    entry.prs.add(target.slice(pullMarker.length));
    return;
  }
  if (target.startsWith(commitMarker)) {
    entry.commits.add(target.slice(commitMarker.length));
  }
}

// Release date for each version comes from the git tag created by changesets
// on publish. All three packages share the tag suffix; we use the node tag.
function tagDates(versions: readonly string[]): Map<string, string> {
  const dates = new Map<string, string>();
  for (const version of versions) {
    const tag = `@spark-connect-js/node@${version}`;
    try {
      const iso = execFileSync("git", ["log", "-1", "--format=%cI", tag], {
        cwd: REPO_ROOT,
        encoding: "utf8",
      }).trim();
      if (iso) dates.set(version, iso.slice(0, 10));
    } catch {
      // Tag missing (pre-release local build): leave undated.
    }
  }
  return dates;
}

function compareSemverDesc(a: string, b: string): number {
  const pa = a.split(".").map(Number);
  const pb = b.split(".").map(Number);
  for (let i = 0; i < 3; i++) {
    if (pa[i] !== pb[i]) return pb[i] - pa[i];
  }
  return 0;
}

function formatDate(iso: string | undefined): string {
  if (!iso) return "";
  const date = new Date(`${iso}T00:00:00Z`);
  return date.toLocaleDateString("en-GB", {
    year: "numeric",
    month: "long",
    day: "numeric",
    timeZone: "UTC",
  });
}

function encodeTag(version: string, pkg: string): string {
  return `${pkg}@${version}`.replace("@", "%40").replace("/", "%2F").replace("@", "%40");
}

function render(
  byPackage: ReadonlyMap<string, Map<string, VersionEntry>>,
  dates: ReadonlyMap<string, string>,
  versions: readonly string[],
): string {
  const lines: string[] = [
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
  ];
  for (const pkg of PACKAGES) {
    lines.push(`- [\`${pkg.name}\`](${REPO_URL}/blob/main/${pkg.dir}/CHANGELOG.md)`);
  }
  lines.push("");

  for (let i = 0; i < versions.length; i++) {
    const version = versions[i];
    const date = dates.get(version);
    const displayDate = formatDate(date);

    lines.push(`## ${version}`);
    lines.push("");

    const tagUrl = `${REPO_URL}/releases?q=${encodeURIComponent(version)}&expanded=true`;
    const chips: string[] = [];
    if (displayDate) chips.push(`*${displayDate}*`);
    chips.push(`[GitHub release](${tagUrl})`);
    if (i === 0) chips.push('<Badge text="Latest" variant="success" />');
    lines.push(chips.join(" · "));
    lines.push("");

    const prs = new Set<string>();
    const commits = new Set<string>();
    for (const pkg of PACKAGES) {
      const entry = byPackage.get(pkg.name)?.get(version);
      if (!entry) continue;
      for (const pr of entry.prs) prs.add(pr);
      for (const sha of entry.commits) commits.add(sha);
    }

    const refsHtml: string[] = [];
    for (const pr of [...prs].sort((a, b) => Number(a) - Number(b))) {
      refsHtml.push(`<a href="${REPO_URL}/pull/${pr}">#${pr}</a>`);
    }
    for (const sha of commits) {
      refsHtml.push(`<a href="${REPO_URL}/commit/${sha}"><code>${sha.slice(0, 7)}</code></a>`);
    }
    if (refsHtml.length > 0) {
      lines.push(
        `<p class="release-references"><strong>References:</strong> ${refsHtml.join(" · ")}</p>`,
      );
      lines.push("");
    }

    for (const pkg of PACKAGES) {
      const entry = byPackage.get(pkg.name)?.get(version);
      if (!entry || entry.bullets.length === 0) continue;
      const npmUrl = `https://www.npmjs.com/package/${pkg.name}/v/${version}`;
      const githubUrl = `${REPO_URL}/releases/tag/${encodeTag(version, pkg.name)}`;
      lines.push(
        `<PackageHeader name={${JSON.stringify(pkg.name)}} version={${JSON.stringify(version)}} npmUrl={${JSON.stringify(npmUrl)}} githubUrl={${JSON.stringify(githubUrl)}} />`,
      );
      lines.push("");
      for (const bullet of entry.bullets) lines.push(`- ${bullet}`);
      lines.push("");
    }
  }

  return lines.join("\n");
}

function main(): void {
  const byPackage = new Map<string, Map<string, VersionEntry>>();
  const seen = new Set<string>();

  for (const pkg of PACKAGES) {
    const path = join(REPO_ROOT, pkg.dir, "CHANGELOG.md");
    const text = readFileSync(path, "utf8");
    const versions = parseChangelog(text);
    byPackage.set(pkg.name, versions);
    for (const v of versions.keys()) seen.add(v);
  }

  const versions = [...seen].sort(compareSemverDesc);
  const dates = tagDates(versions);
  const output = render(byPackage, dates, versions);

  const dest = join(DOCS_ROOT, "src/content/docs/changelog.mdx");
  writeFileSync(dest, output);
  console.log(`[changelog] wrote ${versions.length} release(s) -> ${dest}`);
}

main();
