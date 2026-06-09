const ALLOWED_METHODS = 'GET,HEAD,OPTIONS';
const ALLOWED_HEADERS = 'Accept, Authorization, Cache-Control, Content-Type, Range';
const EXPOSED_HEADERS =
  'Accept-Ranges, Content-Encoding, Content-Length, Content-Range, Content-Type, Docker-Content-Digest, WWW-Authenticate';
const FORWARDED_HEADERS = ['accept', 'authorization', 'cache-control', 'content-type', 'if-modified-since', 'if-none-match', 'range'];
const CONFIG_CACHE = new Map();

export default {
  async fetch(request, env) {
    const config = getWorkerConfig(env);
    const requestOrigin = normalizeOrigin(request.headers.get('Origin'));

    if (request.method === 'OPTIONS') {
      return buildPreflightResponse(config, requestOrigin);
    }

    if (!['GET', 'HEAD'].includes(request.method)) {
      return buildJsonError(config, requestOrigin, 405, 'Only GET, HEAD, and OPTIONS are supported.');
    }

    const originPolicyError = validateOriginPolicy(config, requestOrigin);
    if (originPolicyError) {
      return buildJsonError(config, requestOrigin, 403, originPolicyError);
    }

    const requestUrl = new URL(request.url);
    const targetParam = requestUrl.searchParams.get('url');
    if (!targetParam) {
      return buildJsonError(config, requestOrigin, 400, 'Missing required ?url= query parameter.');
    }

    let targetUrl;
    try {
      targetUrl = new URL(targetParam);
    } catch {
      return buildJsonError(config, requestOrigin, 400, 'The target url query parameter must be an absolute URL.');
    }

    if (!['http:', 'https:'].includes(targetUrl.protocol)) {
      return buildJsonError(config, requestOrigin, 400, 'Only http:// and https:// target URLs are allowed.');
    }

    if (!config.allowsAnyUpstream && !isAllowedHost(targetUrl.hostname, config.upstreamAllowlist)) {
      return buildJsonError(
        config,
        requestOrigin,
        403,
        `Host ${targetUrl.hostname} is not allowed by the Worker upstream policy. Add it to UPSTREAM_ALLOWLIST or set ALLOW_ANY_UPSTREAMS=true.`
      );
    }

    const upstreamHeaders = new Headers();
    for (const headerName of FORWARDED_HEADERS) {
      const value = request.headers.get(headerName);
      if (value) {
        upstreamHeaders.set(headerName, value);
      }
    }

    let upstreamResponse;
    try {
      upstreamResponse = await fetch(targetUrl.toString(), {
        method: request.method,
        headers: upstreamHeaders,
        redirect: 'follow',
        cf: {
          cacheTtl: 0,
          cacheEverything: false
        }
      });
    } catch (error) {
      return buildJsonError(config, requestOrigin, 502, `Upstream fetch failed: ${error?.message || 'unknown error'}.`);
    }

    const responseHeaders = new Headers(upstreamResponse.headers);
    applyCorsHeaders(responseHeaders, config, requestOrigin);
    responseHeaders.set('Cache-Control', 'no-store');
    responseHeaders.set('Vary', appendVary(responseHeaders.get('Vary'), 'Origin'));

    return new Response(upstreamResponse.body, {
      status: upstreamResponse.status,
      statusText: upstreamResponse.statusText,
      headers: responseHeaders
    });
  }
};

function buildPreflightResponse(config, requestOrigin) {
  const originPolicyError = validateOriginPolicy(config, requestOrigin);
  if (originPolicyError) {
    return buildJsonError(config, requestOrigin, 403, originPolicyError);
  }
  const headers = new Headers();
  applyCorsHeaders(headers, config, requestOrigin);
  headers.set('Access-Control-Max-Age', '86400');
  return new Response(null, {
    status: 204,
    headers
  });
}

function buildJsonError(config, requestOrigin, status, message) {
  const headers = new Headers({
    'Content-Type': 'application/json; charset=utf-8',
    'Cache-Control': 'no-store'
  });
  applyCorsHeaders(headers, config, requestOrigin);
  return new Response(JSON.stringify({ error: message }), {
    status,
    headers
  });
}

function applyCorsHeaders(headers, config, origin) {
  const allowedOrigin = resolveAllowedOrigin(config, origin);
  if (allowedOrigin) {
    headers.set('Access-Control-Allow-Origin', allowedOrigin);
  }
  headers.set('Access-Control-Allow-Methods', ALLOWED_METHODS);
  headers.set('Access-Control-Allow-Headers', ALLOWED_HEADERS);
  headers.set('Access-Control-Expose-Headers', EXPOSED_HEADERS);
}

function appendVary(currentValue, nextValue) {
  const values = String(currentValue || '')
    .split(',')
    .map((value) => value.trim())
    .filter(Boolean);
  if (!values.includes(nextValue)) {
    values.push(nextValue);
  }
  return values.join(', ');
}

function getWorkerConfig(env) {
  const rawAllowedOrigins = String(env.ALLOWED_ORIGINS || '');
  const rawUpstreamAllowlist = String(env.UPSTREAM_ALLOWLIST || '');
  const rawAllowAnyUpstreams = String(env.ALLOW_ANY_UPSTREAMS || '');
  const rawAllowMissingOrigin = String(env.ALLOW_MISSING_ORIGIN || '');
  const cacheKey = [rawAllowedOrigins, rawUpstreamAllowlist, rawAllowAnyUpstreams, rawAllowMissingOrigin].join('\u0000');

  let config = CONFIG_CACHE.get(cacheKey);
  if (config) {
    return config;
  }

  config = {
    originAllowlist: parseOriginAllowlist(rawAllowedOrigins),
    upstreamAllowlist: parseAllowlist(rawUpstreamAllowlist),
    allowsAnyUpstream: parseEnabledFlag(rawAllowAnyUpstreams) || rawUpstreamAllowlist.trim() === '*',
    allowsMissingOrigin: parseEnabledFlag(rawAllowMissingOrigin)
  };

  CONFIG_CACHE.set(cacheKey, config);
  return config;
}

function parseAllowlist(value) {
  return String(value || '')
    .split(',')
    .map((entry) => entry.trim().toLowerCase())
    .filter(Boolean);
}

function parseOriginAllowlist(value) {
  return String(value || '')
    .split(',')
    .map((entry) => normalizeOrigin(entry))
    .filter(Boolean);
}

function parseEnabledFlag(value) {
  const rawValue = String(value || '').trim().toLowerCase();
  return ['1', 'true', 'yes', 'on'].includes(rawValue);
}

function normalizeOrigin(value) {
  const raw = String(value || '').trim();
  if (!raw) return '';
  try {
    return new URL(raw).origin.toLowerCase();
  } catch {
    return '';
  }
}

function resolveAllowedOrigin(config, requestOrigin) {
  if (!config.originAllowlist.length) {
    return requestOrigin || '';
  }
  return config.originAllowlist.includes(requestOrigin) ? requestOrigin : '';
}

function validateOriginPolicy(config, requestOrigin) {
  if (!requestOrigin) {
    return config.allowsMissingOrigin ? '' : 'This proxy only accepts browser requests from approved site origins.';
  }
  if (!config.originAllowlist.length) {
    return '';
  }
  return config.originAllowlist.includes(requestOrigin) ? '' : `Origin ${requestOrigin} is not allowed to use this proxy.`;
}

function isAllowedHost(hostname, allowlist) {
  const host = String(hostname || '').toLowerCase();
  if (!allowlist.length) return false;
  return allowlist.some((entry) => {
    if (entry.startsWith('*.')) {
      const suffix = entry.slice(1);
      return host.endsWith(suffix);
    }
    return host === entry;
  });
}
