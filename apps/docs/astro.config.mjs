import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import starlightTypeDoc, { typeDocSidebarGroup } from "starlight-typedoc";
import { sidebarDivider } from "./src/components/sidebar-divider";

export default defineConfig({
  site: "https://prustic.github.io",
  base: "/spark-connect-js",
  integrations: [
    starlight({
      title: "spark-connect-js",
      description: "A TypeScript client for Apache Spark, using the Spark Connect protocol.",
      lastUpdated: true,
      head: [
        {
          tag: "meta",
          attrs: { name: "referrer", content: "strict-origin-when-cross-origin" },
        },
      ],
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/prustic/spark-connect-js",
        },
        {
          icon: "npm",
          label: "npm",
          href: "https://www.npmjs.com/package/@spark-connect-js/node",
        },
      ],
      editLink: {
        baseUrl: "https://github.com/prustic/spark-connect-js/edit/main/apps/docs/",
      },
      customCss: ["./src/styles/custom.css"],
      components: {
        Header: "./src/components/overrides/Header.astro",
      },
      plugins: [
        starlightTypeDoc({
          entryPoints: [
            "../../packages/spark-core/src/index.ts",
            "../../packages/spark-node/src/index.ts",
          ],
          tsconfig: "../../packages/spark-core/tsconfig.json",
          output: "api",
          sidebar: {
            label: "API reference",
            collapsed: false,
          },
          typeDoc: {
            entryPointStrategy: "resolve",
            excludePrivate: true,
            excludeInternal: true,
            excludeExternals: true,
            readme: "none",
            githubPages: false,
            hideGenerator: true,
            useCodeBlocks: true,
            expandObjects: true,
            parametersFormat: "table",
            entryFileName: "index",
            name: "API reference",
          },
        }),
      ],
      sidebar: [
        {
          label: "Quickstart",
          slug: "quickstart",
        },
        {
          label: "Programming guides",
          items: [
            { label: "SQL and DataFrame", slug: "sql-and-dataframe-guide" },
            { label: "Functions", slug: "functions" },
            { label: "Catalog", slug: "catalog" },
            { label: "I/O", slug: "io" },
            { label: "Window functions", slug: "window-functions" },
            { label: "Structured Streaming", slug: "streaming" },
            { label: "Error handling", slug: "error-handling" },
          ],
        },
        {
          label: "Integrations",
          items: [
            { label: "Overview", slug: "integrations" },
            sidebarDivider({ label: "Runtimes" }),
            {
              label: "Node.js",
              slug: "integrations/runtimes/node",
              attrs: { class: "sl-sidebar-runtime sl-runtime-node" },
            },
          ],
        },
        {
          label: "Operating the client",
          items: [
            { label: "Configuration", slug: "configuration" },
            { label: "Security", slug: "security" },
            { label: "Troubleshooting", slug: "troubleshooting" },
            { label: "Compatibility", slug: "compatibility" },
          ],
        },
        {
          label: "Resources",
          items: [
            { label: "Examples", slug: "examples" },
            { label: "Comparison to PySpark", slug: "pyspark-comparison" },
            { label: "Architecture", slug: "architecture" },
          ],
        },
        typeDocSidebarGroup,
        {
          label: "Project",
          items: [
            { label: "Roadmap", slug: "roadmap" },
            { label: "Contributing", slug: "contributing" },
            { label: "Changelog", slug: "changelog" },
          ],
        },
      ],
    }),
  ],
});
