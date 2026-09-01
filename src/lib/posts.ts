import { getCollection, type CollectionEntry } from 'astro:content';

export type Post = CollectionEntry<'blog'> & { version: string };

/**
 * Posts, newest first, each stamped with the version it got when it landed.
 * The oldest post is v0001 and numbers only ever go up — the list is an
 * append-only log, so a post's stamp never changes once published.
 */
export async function getPosts(): Promise<Post[]> {
  const all = await getCollection('blog', ({ data }) => import.meta.env.DEV || !data.draft);

  const oldestFirst = all.sort(
    (a, b) => a.data.pubDate.valueOf() - b.data.pubDate.valueOf()
  );

  const stamped = oldestFirst.map((entry, i) => ({
    ...entry,
    version: `v${String(i + 1).padStart(4, '0')}`,
  }));

  return stamped.reverse();
}

export function formatDate(date: Date): string {
  return date.toISOString().slice(0, 10);
}
