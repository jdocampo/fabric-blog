# fabric-blog

Static blog built with Astro, deployed to GitHub Pages from a **private** repository.
Markdown in, HTML out, no server.

## Create the private repo

I can't create it on your account — that needs your GitHub credentials. These commands do it
in about a minute. With the [GitHub CLI](https://cli.github.com/):

```bash
cd fabric-blog
git init -b main
git add .
git commit -m "Initial commit: Astro blog scaffold"

gh repo create fabric-blog --private --source=. --remote=origin --push
```

Without `gh`: create the repo in the web UI (Private), then:

```bash
git remote add origin git@github.com:<user>/fabric-blog.git
git push -u origin main
```

### One catch about private repos

GitHub Pages from a **private** repo requires a paid plan (Pro, Team or Enterprise) — on
GitHub Free, Pages is only served from public repos. Options:

1. **GitHub Pro** (~$4/month) — keep the repo private, everything below works as written.
2. **Keep the repo private, deploy elsewhere** — Cloudflare Pages and Netlify both build
   private GitHub repos on their free tiers. Delete `.github/workflows/deploy.yml` and point
   the provider at this repo; build command `npm run build`, output directory `dist`.
3. **Make the repo public** — it's a blog; the source being readable costs you nothing and
   people can send pull requests fixing your typos.

## Enable Pages

Repository → Settings → Pages → Source: **GitHub Actions**.

Push to `main` and `.github/workflows/deploy.yml` builds and publishes. First run takes
about two minutes, later ones under one.

## Point a domain at it

1. At your DNS provider, add a `CNAME` record: `www` → `<user>.github.io`.
   For an apex domain (`example.com`) add `A` records to GitHub's four Pages IPs instead.
2. Settings → Pages → Custom domain → enter the domain → save → tick **Enforce HTTPS**
   once the certificate is issued (a few minutes).
3. Set `site` in `astro.config.mjs` to the final URL and remove any `base`.

Until then, set `site: 'https://<user>.github.io'` and `base: '/fabric-blog'`.

## Local development

```bash
npm install
npm run dev      # http://localhost:4321
npm run build    # writes ./dist
npm run preview  # serve ./dist locally
```

Requires Node 20 or newer.

## Writing a post

Add a Markdown file to `src/content/blog/`. The filename becomes the URL, so
`v-order-tradeoff.md` publishes at `/blog/v-order-tradeoff/`. Front matter:

```yaml
---
title: 'OPTIMIZE and the ConcurrentAppendException nobody warned you about'
description: 'One or two sentences. Used on the index, in RSS and as the meta description.'
pubDate: 2026-03-11
tags: ['delta-lake', 'spark']
draft: false
# canonical: 'https://elsewhere.example/original'   # only if this is NOT the original
---
```

`draft: true` keeps a post visible in `npm run dev` and out of the build.

**Cross-posting.** This site is the canonical home. When you repost to LinkedIn or the
Fabric Community blog, that copy points its canonical link back here. Only set the
`canonical` field in front matter for the reverse case — a post that originated elsewhere.

## Code blocks

Highlighting is Shiki at build time, so there's no client-side JS and no flash of unstyled
code. Fence with a language:

````markdown
```python
df.write.mode("overwrite").saveAsTable("bronze.raw_events")
```
````

Change the theme in `astro.config.mjs` (`shikiConfig.theme`).

## What's here

```
src/
  content/blog/        posts (Markdown)
  content.config.ts    front matter schema — validated at build time
  lib/posts.ts         sorting and version stamps
  layouts/Base.astro   <head>, masthead, footer
  components/          Commit.astro — one entry in the log
  pages/               index, /blog/[slug], /tags/[tag], /about, /rss.xml, 404
  styles/global.css    all styling; design tokens at the top
  site.ts              title, description, social links
```

## Design note

The index is laid out as an append-only log because that's what a blog is. Each post gets
a version stamp (`v0001`, `v0002`…) assigned by publication order and it never changes,
so the newest post always has the highest number. Entries hang off a single spine rule.
All colour lives in `src/styles/global.css` under `:root` — the magenta is deliberately
spent only on commit stamps and link hovers, so change that one token and the whole site
follows.

Dark mode follows the system setting. No toggle, no flash.

## Things you'll probably want next

- **Comments** — [Giscus](https://giscus.app), backed by GitHub Discussions. Needs a public
  repo for the discussions, which can be a separate one from this.
- **Search** — [Pagefind](https://pagefind.app), runs after `npm run build` and indexes
  `dist`. Add it as a post-build step in the workflow.
- **Analytics** — anything that doesn't need a cookie banner: Plausible, Fathom, or
  Cloudflare Web Analytics if you end up hosting there.
- **Open Graph images** — `astro-og-canvas` generates one per post at build time.
