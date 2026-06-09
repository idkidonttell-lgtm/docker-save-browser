export const JOB_ID = 'default';
export const CHUNK_SIZE = 8 * 1024 * 1024;
export const MAX_RELIABLE_COMPRESSED_SIZE = 2 * 1024 * 1024 * 1024;
export const MAX_PREVIEW_BYTES = 64 * 1024;
export const MAX_TEXT_FILE_PREVIEW_SIZE = 256 * 1024;
export const DEFAULT_PLATFORM = 'linux/amd64';

export const MANIFEST_ACCEPT = [
  'application/vnd.oci.image.index.v1+json',
  'application/vnd.oci.image.manifest.v1+json',
  'application/vnd.docker.distribution.manifest.list.v2+json',
  'application/vnd.docker.distribution.manifest.v2+json'
];

export const IMAGE_INDEX_MEDIA_TYPES = new Set([
  'application/vnd.oci.image.index.v1+json',
  'application/vnd.docker.distribution.manifest.list.v2+json'
]);

export const IMAGE_MANIFEST_MEDIA_TYPES = new Set([
  'application/vnd.oci.image.manifest.v1+json',
  'application/vnd.docker.distribution.manifest.v2+json'
]);

export const NON_DISTRIBUTABLE_LAYER_MEDIA_TYPES = new Set([
  'application/vnd.oci.image.layer.nondistributable.v1.tar',
  'application/vnd.oci.image.layer.nondistributable.v1.tar+gzip',
  'application/vnd.oci.image.layer.nondistributable.v1.tar+zstd',
  'application/vnd.docker.image.rootfs.foreign.diff.tar.gzip',
  'application/vnd.docker.image.rootfs.foreign.diff.tar'
]);

export function splitDigest(value) {
  const raw = String(value || '').trim();
  const match = /^([a-z0-9_+.-]+):([a-f0-9]{64,})$/i.exec(raw);
  if (!match) return null;
  return { algorithm: match[1].toLowerCase(), hex: match[2].toLowerCase(), digest: `${match[1].toLowerCase()}:${match[2].toLowerCase()}` };
}

export function parseImageReference(value) {
  const raw = String(value || '').trim();
  if (!raw) {
    throw new Error('Enter an image reference like library/alpine:latest or namespace/image@sha256:...');
  }

  const digestSeparator = raw.indexOf('@');
  if (digestSeparator >= 0) {
    const repository = raw.slice(0, digestSeparator).trim();
    const digest = raw.slice(digestSeparator + 1).trim();
    if (!repository || !splitDigest(digest)) {
      throw new Error('Digest references must look like namespace/image@sha256:<64-hex-digest>.');
    }
    return {
      repository,
      reference: digest.toLowerCase(),
      digest: digest.toLowerCase(),
      tag: null,
      repoTag: null,
      displayName: `${repository}@${digest.toLowerCase()}`
    };
  }

  const slashIndex = raw.lastIndexOf('/');
  const colonIndex = raw.lastIndexOf(':');
  const hasTag = colonIndex > slashIndex;
  const repository = (hasTag ? raw.slice(0, colonIndex) : raw).trim();
  const tag = (hasTag ? raw.slice(colonIndex + 1) : 'latest').trim();

  if (!repository || !tag) {
    throw new Error('Tag references must look like namespace/image:tag.');
  }

  return {
    repository,
    reference: tag,
    digest: null,
    tag,
    repoTag: `${repository}:${tag}`,
    displayName: `${repository}:${tag}`
  };
}

export function parseImageSourceInput(value) {
  const raw = normalizePastedDockerPullInput(value);
  if (!raw) {
    throw new Error('Enter an image like alpine:latest.');
  }

  if (!hasExplicitRegistryHost(raw) && !/^[a-z][a-z0-9+.-]*:\/\//i.test(raw)) {
    return buildDockerHubSourceInput(raw);
  }

  const candidate = /^[a-z][a-z0-9+.-]*:\/\//i.test(raw) ? raw : `https://${raw}`;

  let url;
  try {
    url = new URL(candidate);
  } catch {
    throw new Error('Enter an image like alpine:latest.');
  }

  if (!/^https?:$/.test(url.protocol)) {
    throw new Error('Image input must start with http:// or https://, or omit the scheme entirely.');
  }

  if (url.username || url.password) {
    throw new Error('Image input cannot include embedded credentials.');
  }

  if (url.search || url.hash) {
    throw new Error('Image input cannot include query strings or fragments.');
  }

  const rawPath = url.pathname.replace(/^\/+/, '').replace(/\/+$/, '');
  if (!rawPath) {
    throw new Error('Add a repository path after the registry host.');
  }

  const dockerHubHost = normalizeDockerHubHost(url.host);
  const normalizedPath = dockerHubHost ? normalizeDockerHubReference(rawPath) : rawPath;
  const parsedReference = parseImageReference(normalizedPath);
  const registryBaseUrl = normalizeRegistryBaseUrl(`${url.protocol}//${dockerHubHost || url.host}`);

  return {
    sourceImageInput: raw,
    registryBaseUrl,
    imageRef: parsedReference.displayName,
    repository: parsedReference.repository,
    reference: parsedReference.reference,
    displayName: `${stripProtocol(registryBaseUrl)}/${parsedReference.displayName}`
  };
}

export function normalizePastedDockerPullInput(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';

  const match = /^(?:sudo\s+)?docker(?:\s+image)?\s+pull\b/i.exec(raw);
  if (!match) return raw;

  const tokens = raw.slice(match[0].length).trim().split(/\s+/).filter(Boolean);
  if (!tokens.length) return '';

  const positionals = [];
  for (let index = 0; index < tokens.length; index += 1) {
    const token = tokens[index];
    if (!token) continue;
    if (token.startsWith('--')) {
      if (!token.includes('=') && index + 1 < tokens.length) {
        index += 1;
      }
      continue;
    }
    if (token.startsWith('-')) {
      continue;
    }
    positionals.push(token);
  }

  return positionals[positionals.length - 1] || '';
}

function hasExplicitRegistryHost(raw) {
  const normalized = String(raw || '').trim();
  const schemeLess = normalized.replace(/^[a-z][a-z0-9+.-]*:\/\//i, '');
  const slashIndex = schemeLess.indexOf('/');
  if (slashIndex <= 0) return false;
  const firstSegment = schemeLess.slice(0, slashIndex).toLowerCase();
  return firstSegment.includes('.') || firstSegment.includes(':') || firstSegment === 'localhost';
}

function buildDockerHubSourceInput(raw) {
  const normalizedReference = normalizeDockerHubReference(raw);
  const parsedReference = parseImageReference(normalizedReference);
  const registryBaseUrl = 'https://registry-1.docker.io';

  return {
    sourceImageInput: raw,
    registryBaseUrl,
    imageRef: parsedReference.displayName,
    repository: parsedReference.repository,
    reference: parsedReference.reference,
    displayName: `${stripProtocol(registryBaseUrl)}/${parsedReference.displayName}`
  };
}

function normalizeDockerHubReference(value) {
  const parsedReference = parseImageReference(value);
  const repository = parsedReference.repository.includes('/') ? parsedReference.repository : `library/${parsedReference.repository}`;
  return parsedReference.digest ? `${repository}@${parsedReference.digest}` : `${repository}:${parsedReference.tag}`;
}

function normalizeDockerHubHost(host) {
  const normalized = String(host || '').trim().toLowerCase();
  if (!normalized) return '';
  if (normalized === 'docker.io' || normalized === 'index.docker.io' || normalized === 'registry-1.docker.io') {
    return 'registry-1.docker.io';
  }
  return '';
}

export function normalizeRegistryBaseUrl(value) {
  const raw = String(value || '').trim();
  if (!raw) {
    throw new Error('Enter a registry base URL like https://registry.example.com.');
  }

  let url;
  try {
    url = new URL(raw);
  } catch {
    throw new Error('Registry base URL must be a valid absolute URL.');
  }

  if (!/^https?:$/.test(url.protocol)) {
    throw new Error('Registry base URL must start with http:// or https://.');
  }

  url.hash = '';
  url.search = '';
  if (url.pathname.endsWith('/')) {
    url.pathname = url.pathname.replace(/\/+$/, '');
  }
  if (!url.pathname) {
    url.pathname = '';
  }

  return url.toString().replace(/\/$/, '');
}

export function normalizeProxyBaseUrl(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';

  let url;
  try {
    url = new URL(raw);
  } catch {
    throw new Error('Cloudflare Worker proxy URL must be a valid absolute URL.');
  }

  if (!/^https?:$/.test(url.protocol)) {
    throw new Error('Cloudflare Worker proxy URL must start with http:// or https://.');
  }

  url.hash = '';
  url.search = '';
  return url.toString().replace(/\/$/, '');
}

export function buildRegistryV2Base(value) {
  const normalized = normalizeRegistryBaseUrl(value);
  return normalized.endsWith('/v2') ? normalized : `${normalized}/v2`;
}

export function detectLayerCompression(mediaType) {
  const type = String(mediaType || '').toLowerCase();
  if (!type) return 'identity';
  if (type.endsWith('+gzip') || type.includes('.tar.gzip')) return 'gzip';
  if (type.endsWith('+zstd')) return 'zstd';
  if (type.includes('.tar')) return 'identity';
  return 'unknown';
}

export function normalizeFsPath(value) {
  const raw = String(value || '').trim();
  if (!raw || raw === '.' || raw === './') return '/';

  const trimmed = raw.replace(/^\.\/+/, '').replace(/^\/+/, '').replace(/\/+/g, '/').replace(/\/$/, '');
  if (!trimmed) return '/';
  return `/${trimmed}`;
}

export function joinFsPath(parentPath, childName) {
  const parent = normalizeFsPath(parentPath);
  const child = String(childName || '').replace(/^\/+/, '').replace(/\/+$/, '');
  if (!child) return parent;
  return parent === '/' ? `/${child}` : `${parent}/${child}`;
}

export function parentFsPath(path) {
  const value = normalizeFsPath(path);
  if (value === '/') return null;
  const index = value.lastIndexOf('/');
  return index <= 0 ? '/' : value.slice(0, index);
}

export function basenameFsPath(path) {
  const value = normalizeFsPath(path);
  if (value === '/') return '/';
  const index = value.lastIndexOf('/');
  return index < 0 ? value : value.slice(index + 1);
}

export function sortEntries(entries) {
  return [...entries].sort((left, right) => {
    const leftType = left.type === 'dir' ? 0 : 1;
    const rightType = right.type === 'dir' ? 0 : 1;
    if (leftType !== rightType) return leftType - rightType;
    return String(left.name || '').localeCompare(String(right.name || ''), undefined, {
      numeric: true,
      sensitivity: 'base'
    });
  });
}

export function formatBytes(input) {
  const value = Number(input || 0);
  if (!Number.isFinite(value) || value <= 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let size = value;
  let unitIndex = 0;
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024;
    unitIndex += 1;
  }
  const digits = size >= 100 || unitIndex === 0 ? 0 : size >= 10 ? 1 : 2;
  return `${size.toFixed(digits)} ${units[unitIndex]}`;
}

export function formatNumber(input) {
  return new Intl.NumberFormat('en-US').format(Number(input || 0));
}

export function formatDate(input) {
  if (!input) return 'Unknown';
  const date = new Date(input);
  if (Number.isNaN(date.getTime())) return 'Unknown';
  return new Intl.DateTimeFormat('en-US', {
    dateStyle: 'medium',
    timeStyle: 'short'
  }).format(date);
}

export function stripProtocol(value) {
  return String(value || '').replace(/^https?:\/\//i, '').replace(/\/+$/, '');
}

export function normalizePlatformValue(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return '';

  const segments = raw
    .split('/')
    .map((segment) => segment.trim())
    .filter(Boolean);

  if (segments.length < 2 || segments.length > 3) {
    return '';
  }

  if (!segments.every((segment) => /^[a-z0-9][a-z0-9+_.-]*$/i.test(segment))) {
    return '';
  }

  if (segments[0] === 'unknown' || segments[1] === 'unknown') {
    return '';
  }

  return segments.join('/');
}

export function descriptorPlatformValue(platform) {
  return normalizePlatformValue(
    [platform?.os, platform?.architecture, platform?.variant]
      .map((segment) => String(segment || '').trim().toLowerCase())
      .filter(Boolean)
      .join('/')
  );
}

export function comparePlatforms(leftInput, rightInput) {
  const left = normalizePlatformValue(leftInput);
  const right = normalizePlatformValue(rightInput);
  if (!left && !right) return 0;
  if (!left) return 1;
  if (!right) return -1;

  const groupDiff = platformGroupRank(left) - platformGroupRank(right);
  if (groupDiff !== 0) {
    return groupDiff;
  }

  return left.localeCompare(right);
}

export function normalizePlatformList(values, { allowEmpty = false } = {}) {
  const seen = new Set();
  const normalized = [];

  for (const value of values || []) {
    const next = normalizePlatformValue(value);
    if (!next || seen.has(next)) continue;
    seen.add(next);
    normalized.push(next);
  }

  normalized.sort(comparePlatforms);

  if (normalized.length) {
    return normalized;
  }

  return allowEmpty ? [] : [DEFAULT_PLATFORM];
}

export function pickPreferredPlatform(options, requestedPlatform, fallbackPlatform = DEFAULT_PLATFORM) {
  const normalizedOptions = normalizePlatformList(options, { allowEmpty: true });
  const requested = normalizePlatformValue(requestedPlatform);

  if (requested && normalizedOptions.includes(requested)) {
    return requested;
  }

  if (normalizedOptions.length) {
    return normalizedOptions[0];
  }

  return normalizePlatformValue(fallbackPlatform) || DEFAULT_PLATFORM;
}

export function platformMatches(descriptorPlatform, requestedPlatform) {
  const actual = descriptorPlatformValue(descriptorPlatform);
  const expected = normalizePlatformValue(requestedPlatform);
  return Boolean(actual && expected && actual === expected);
}

export function buildRepoDisplayName(registryBaseUrl, repository, reference) {
  const host = stripProtocol(registryBaseUrl).replace(/\/v2$/i, '');
  return `${host}/${repository}:${reference}`;
}

function platformGroupRank(value) {
  const [os] = value.split('/');
  if (os === 'linux') return 0;
  if (os === 'windows') return 1;
  return 2;
}
