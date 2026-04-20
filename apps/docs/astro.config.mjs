import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

export default defineConfig({
  site: "https://prustic.github.io",
  base: "/spark-connect-js",
  integrations: [
    starlight({
      title: "spark-js",
      description: "Apache Spark, meet TypeScript. A first-class Spark Connect client for Node.js.",
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/prustic/spark-connect-js",
        },
      ],
      editLink: {
        baseUrl: "https://github.com/prustic/spark-connect-js/edit/main/apps/docs/",
      },
      customCss: ["./src/styles/custom.css"],
      sidebar: [
        {
          label: "Start here",
          items: [
            { label: "Getting started", slug: "getting-started" },
            { label: "Why spark-js", slug: "why-spark-js" },
            { label: "PySpark → spark-js", slug: "pyspark-to-spark-js" },
          ],
        },
        {
          label: "Guides",
          autogenerate: { directory: "guides" },
        },
        {
          label: "Recipes",
          autogenerate: { directory: "recipes" },
        },
        {
          label: "Reference",
          items: [
            { label: "Feature matrix", slug: "feature-matrix" },
            { label: "Changelog", slug: "changelog" },
            { label: "Contributing", slug: "contributing" },
          ],
        },
      ],
    }),
  ],
});
