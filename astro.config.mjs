// @ts-check
import { defineConfig } from 'astro/config';
import sitemap from '@astrojs/sitemap';

// CHANGE ME: your final public URL. Used for canonical tags, sitemap and RSS.
// While you are on the default GitHub Pages domain use:
//   site: 'https://<user>.github.io', base: '/<repo>'
// Once you point a custom domain at it, drop `base` entirely.
export default defineConfig({
  site: 'https://example.com',
  integrations: [sitemap()],
  markdown: {
    shikiConfig: {
      theme: 'github-dark-dimmed',
      wrap: false,
    },
  },
  build: {
    format: 'directory',
  },
});
