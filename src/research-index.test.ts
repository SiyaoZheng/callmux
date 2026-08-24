import { test } from "node:test";
import assert from "node:assert/strict";
import {
  buildResearchIndex,
  canonicalizeWebUrl,
  extractResearchObservations,
} from "./research-index.js";

test("URL canonicalization removes tracking noise but preserves functional query state", () => {
  const normalized = canonicalizeWebUrl(
    "HTTPS://Example.COM/docs//guide/?utm_source=test&b=2&a=1#section"
  );
  assert.deepEqual(normalized, {
    url: "HTTPS://Example.COM/docs//guide/?utm_source=test&b=2&a=1#section",
    canonicalUrl: "https://example.com/docs/guide?a=1&b=2",
    domain: "example.com",
    path: "/docs/guide",
  });
});

test("research observations distinguish Exa discoveries from explicit fetches", () => {
  const search = extractResearchObservations(
    "web_search_exa",
    { query: "中国 政商关系" },
    {
      content: [{
        type: "text",
        text: "Title: Evidence\nURL: https://example.com/research/evidence\n",
      }],
    }
  );
  assert.deepEqual(search.queries, [{ provider: "exa", query: "中国 政商关系" }]);
  assert.equal(search.pages[0].relation, "discovered");
  assert.equal(search.pages[0].title, "Evidence");
  assert.equal(search.pages[0].query, "中国 政商关系");

  const fetch = extractResearchObservations("web_fetch_exa", {
    urls: ["https://example.com/research/evidence"],
  });
  assert.equal(fetch.pages[0].relation, "fetched");
  assert.equal(fetch.pages[0].domain, "example.com");
});

test("Sogou batch research records returned articles as fetched pages", () => {
  const observations = extractResearchObservations(
    "batch_research_sogou",
    { queries: ["干部 任命"] },
    {
      content: [{
        type: "text",
        text: "Title: 微信文章\nURL: https://mp.weixin.qq.com/s/example\n",
      }],
    }
  );
  assert.deepEqual(observations.queries, [{ provider: "sogou", query: "干部 任命" }]);
  assert.equal(observations.pages[0].relation, "fetched");
  assert.equal(observations.pages[0].domain, "mp.weixin.qq.com");
  assert.equal(observations.pages[0].query, "干部 任命");
});

test("URL tree groups pages by domain and path segments", () => {
  const index = buildResearchIndex(
    [{
      provider: "exa",
      query: "中国 政商关系",
      calls: 2,
      firstSeenAt: "2026-08-23T00:00:00.000Z",
      lastSeenAt: "2026-08-24T00:00:00.000Z",
      signatures: ["Codex/thread-a"],
      projects: [{ name: "CPED-OpenAleph", path: "/work/CPED-OpenAleph" }],
    }],
    [
      {
        provider: "exa",
        url: "https://example.com/research/a",
        canonicalUrl: "https://example.com/research/a",
        domain: "example.com",
        path: "/research/a",
        discoveries: 1,
        fetches: 2,
        queries: ["中国 政商关系"],
        firstSeenAt: "2026-08-23T00:00:00.000Z",
        lastSeenAt: "2026-08-24T00:00:00.000Z",
        signatures: ["Codex/thread-a"],
        projects: [{ name: "CPED-OpenAleph", path: "/work/CPED-OpenAleph" }],
      },
      {
        provider: "exa",
        url: "https://example.com/research/b",
        canonicalUrl: "https://example.com/research/b",
        domain: "example.com",
        path: "/research/b",
        discoveries: 1,
        fetches: 0,
        queries: ["中国 政商关系"],
        firstSeenAt: "2026-08-24T00:00:00.000Z",
        lastSeenAt: "2026-08-24T00:00:00.000Z",
        signatures: ["Codex/thread-b"],
        projects: [{ name: "callmux", path: "/work/callmux" }],
      },
    ]
  );
  assert.equal(index.urlTree[0].segment, "example.com");
  assert.equal(index.urlTree[0].pages, 2);
  assert.equal(index.urlTree[0].children[0].segment, "research");
  assert.deepEqual(index.urlTree[0].signatures, ["Codex/thread-a", "Codex/thread-b"]);
  assert.deepEqual(index.urlTree[0].projects.map((project) => project.name).sort(), ["CPED-OpenAleph", "callmux"]);
  assert.deepEqual(
    index.urlTree[0].children[0].children.map((node) => node.segment).sort(),
    ["a", "b"]
  );
  assert.equal(index.topQueries[0].calls, 2);
  assert.equal(index.topTerms.some((row) => row.term === "政商"), true);
});
