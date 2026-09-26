const basePath = import.meta.env.BASE_URL.replace(/\/$/, '');

export function sitePath(path = ''): string {
  return `${basePath}/${path.replace(/^\/+/, '')}`;
}
