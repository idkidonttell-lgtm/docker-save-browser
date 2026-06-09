import {
  CHUNK_SIZE,
  DEFAULT_PLATFORM,
  IMAGE_INDEX_MEDIA_TYPES,
  IMAGE_MANIFEST_MEDIA_TYPES,
  JOB_ID,
  MANIFEST_ACCEPT,
  MAX_PREVIEW_BYTES,
  MAX_RELIABLE_COMPRESSED_SIZE,
  MAX_TEXT_FILE_PREVIEW_SIZE,
  NON_DISTRIBUTABLE_LAYER_MEDIA_TYPES,
  basenameFsPath,
  buildRegistryV2Base,
  descriptorPlatformValue,
  detectLayerCompression,
  formatBytes,
  formatNumber,
  joinFsPath,
  normalizePlatformList,
  normalizePlatformValue as normalizeAnyPlatformValue,
  normalizeFsPath,
  normalizeProxyBaseUrl,
  normalizeRegistryBaseUrl,
  parentFsPath,
  parseImageReference,
  pickPreferredPlatform,
  platformMatches,
  sortEntries,
  splitDigest
} from './shared.js';
import { Sha256 } from './sha256.js';
import { TarWriter, scanTarStream } from './tar.js';
import {
  buildDockerArchive as buildDockerArchiveModule,
  buildSuggestedArchiveName as buildSuggestedArchiveNameModule,
  decodePreviewBytes as decodePreviewBytesModule,
  openLayerContentStream as openLayerContentStreamModule,
  truncateFile as truncateFileModule
} from './worker-archive.js';
import { createWorkerStorage } from './worker-storage.js';

const DB_NAME = 'repoflow-oci-browser';
const DB_VERSION = 1;
const OPFS_ROOT_NAME = 'repoflow-oci-browser';
const ROOT_ENTRY = Object.freeze({
  path: '/',
  name: '/',
  parentPath: null,
  type: 'dir',
  size: 0,
  mode: 0o755,
  uid: 0,
  gid: 0,
  mtime: null,
  linkname: '',
  xattrs: null,
  layerDigest: null,
  layerIndex: -1
});

const runtime = {
  dbPromise: null,
  opfsRootPromise: null,
  job: null,
  entries: new Map(),
  logEntries: [],
  runningTask: null,
  runToken: 0,
  control: 'idle',
  clearingJob: false,
  abortController: null,
  platformProbeAbortController: null,
  authCache: new Map(),
  sessionCredentials: null
};

const {
  clearPersistentState,
  collectStorageEstimate,
  getOpfsRoot,
  getBlobFileHandle,
  getBlobFile,
  removeBlob,
  getJobFileHandle,
  dbGetJob,
  dbPutJob,
  dbGetEntries,
  dbReplaceEntries,
  dbClearJob,
  persistJob,
  persistJobAndEntries
} = createWorkerStorage({
  runtime,
  createError,
  splitDigest,
  JOB_ID,
  DB_NAME,
  DB_VERSION,
  OPFS_ROOT_NAME
});

function decodePreviewBytes(bytes) {
  return decodePreviewBytesModule(bytes, { MAX_PREVIEW_BYTES });
}

function buildDockerArchive(job, signal) {
  return buildDockerArchiveModule(job, signal, {
    TarWriter,
    getJobFileHandle,
    getBlobFile,
    throwIfStopped,
    createError,
    postProgress,
    DEFAULT_PLATFORM,
    OPFS_ROOT_NAME,
    JOB_ID
  });
}

function buildSuggestedArchiveName(job) {
  return buildSuggestedArchiveNameModule(job, { DEFAULT_PLATFORM });
}

function openLayerContentStream(file, mediaType) {
  return openLayerContentStreamModule(file, mediaType, { detectLayerCompression, createError });
}

function truncateFile(fileHandle) {
  return truncateFileModule(fileHandle);
}

self.addEventListener('message', (event) => {
  const data = event.data || {};
  const { id, type, payload } = data;
  if (!id || !type) return;

  Promise.resolve()
    .then(() => handleRequest(type, payload || {}))
    .then((result) => postReply(id, true, result))
    .catch((error) => postReply(id, false, serializeError(error)));
});

async function handleRequest(type, payload) {
  switch (type) {
    case 'bootstrap':
      return bootstrap();
    case 'resolvePlatforms':
      return resolvePlatforms(payload);
    case 'resolveSizeTimeline':
      return resolveSizeTimeline(payload);
    case 'startJob':
      return startJob(payload);
    case 'pauseJob':
      return pauseJob();
    case 'resumeJob':
      return resumeJob();
    case 'cancelJob':
      return cancelJob();
    case 'clearJob':
      return clearJob();
    case 'listDirectory':
      return listDirectory(payload);
    case 'getEntry':
      return getEntry(payload);
    case 'previewText':
      return previewText(payload);
    case 'exportArchive':
      return exportArchive();
    default:
      throw createError('unknown-command', `Unsupported worker command: ${type}`);
  }
}

function postReply(id, ok, payload) {
  self.postMessage(ok ? { replyTo: id, ok: true, result: payload } : { replyTo: id, ok: false, error: payload });
}

function postEvent(event) {
  self.postMessage({ type: 'event', event });
}

function pushLog(level, message) {
  const entry = {
    level,
    message,
    timestamp: new Date().toISOString()
  };
  runtime.logEntries.push(entry);
  if (runtime.logEntries.length > 80) {
    runtime.logEntries = runtime.logEntries.slice(-80);
  }
  postEvent({ kind: 'log', entry });
}

function createError(code, message, extra = {}) {
  const error = new Error(message);
  error.code = code;
  Object.assign(error, extra);
  return error;
}

function serializeError(error) {
  const code = error?.code || 'unknown-error';
  const message = error?.message || 'Unknown error.';
  const details = typeof error?.details === 'string' ? error.details : null;
  return { code, message, details };
}

function structuredCloneSafe(value) {
  return value ? JSON.parse(JSON.stringify(value)) : value;
}

function isRecoverableStop(error) {
  return error?.code === 'job-paused' || error?.code === 'job-cancelled' || error?.name === 'AbortError';
}

async function bootstrap() {
  ensureBrowserSupport();
  await navigator.storage.persist?.().catch(() => false);
  runtime.sessionCredentials = null;
  runtime.authCache.clear();

  const job = await dbGetJob(JOB_ID);
  runtime.entries = new Map();
  if (job) {
    const entries = await dbGetEntries(JOB_ID);
    hydrateEntries(entries);
    runtime.job = normalizeRestoredJob(job);
    if (runtime.job.status === 'resolving' || runtime.job.status === 'downloading' || runtime.job.status === 'indexing') {
      runtime.job.status = 'paused';
      runtime.job.phase = 'paused';
      runtime.job.statusMessage = 'Session restored. Resume to continue the download.';
      runtime.job.restorePrompt = true;
      await dbPutJob(runtime.job);
    }
    ensureRootEntry();
  }

  const snapshot = await buildSnapshot();
  postEvent({ kind: 'snapshot', snapshot });
  return snapshot;
}

async function startJob(payload) {
  ensureBrowserSupport();
  if (runtime.runningTask) {
    throw createError('job-already-running', 'A job is already running. Pause or cancel it before starting a new one.');
  }

  const registryBaseUrl = normalizeRegistryBaseUrl(payload.registryBaseUrl);
  const proxyBaseUrl = normalizeProxyBaseUrl(payload.proxyBaseUrl);
  const initialProxyMode = normalizeProxyMode(payload.proxyMode);
  const parsedReference = parseImageReference(payload.imageRef);
  const requestedPlatform = normalizeRequiredPlatformValue(payload.platform);
  const sessionCredentials = normalizeSessionCredentials(payload.credentials);
  const storage = await collectStorageEstimate();

  await clearPersistentState();
  runtime.sessionCredentials = sessionCredentials;
  runtime.authCache.clear();

  runtime.job = {
    id: JOB_ID,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    sourceImageInput: String(payload.sourceImageInput || `${registryBaseUrl}/${parsedReference.displayName}`).trim(),
    registryBaseUrl,
    proxyBaseUrl: proxyBaseUrl || null,
    proxySource: payload.proxySource || null,
    proxyAuthActive: Boolean(proxyBaseUrl) && (initialProxyMode === 'auth' || initialProxyMode === 'mixed'),
    proxyDataActive:
      Boolean(proxyBaseUrl) &&
      (initialProxyMode === 'data' || initialProxyMode === 'mixed' || (Boolean(payload.proxyActive) && !initialProxyMode)),
    proxyActive: Boolean(
      proxyBaseUrl &&
        (payload.proxyActive || initialProxyMode === 'auth' || initialProxyMode === 'data' || initialProxyMode === 'mixed')
    ),
    skipInitialProbe: Boolean(payload.skipInitialProbe),
    hasCredentials: Boolean(sessionCredentials),
    registryV2Base: buildRegistryV2Base(registryBaseUrl),
    repository: parsedReference.repository,
    imageRef: payload.imageRef,
    reference: parsedReference.reference,
    tag: parsedReference.tag,
    digest: parsedReference.digest,
    repoTag: parsedReference.repoTag,
    selectedPlatform: requestedPlatform,
    platformOptions: normalizePlatformList(payload.platformOptions),
    outputFormat: 'docker-load-tar',
    status: 'queued',
    phase: 'queued',
    statusMessage: 'Waiting to probe the registry.',
    restorePrompt: false,
    storage,
    manifest: null,
    sourceIndex: null,
    config: null,
    layers: [],
    totals: {
      compressedBytes: 0,
      downloadedBytes: 0,
      indexedLayers: 0,
      totalLayers: 0,
      entryCount: 0,
      directoryCount: 1,
      fileCount: 0
    },
    exportInfo: null,
    lastError: null
  };

  runtime.entries = new Map();
  ensureRootEntry();
  runtime.control = 'running';
  pushLog('info', `Prepared job for ${runtime.job.repository} on ${runtime.job.registryBaseUrl}.`);
  if (runtime.job.proxyBaseUrl) {
    pushLog('info', `Cloudflare Worker fallback is configured at ${runtime.job.proxyBaseUrl}.`);
  }
  await persistJob();
  postJobUpdate();
  beginRunLoop();
  return buildSnapshot();
}

async function resolvePlatforms(payload) {
  ensureRegistryClientSupport();
  runtime.platformProbeAbortController?.abort();
  const abortController = new AbortController();
  runtime.platformProbeAbortController = abortController;

  try {
    const registryBaseUrl = normalizeRegistryBaseUrl(payload.registryBaseUrl);
    const proxyBaseUrl = normalizeProxyBaseUrl(payload.proxyBaseUrl);
    const parsedReference = parseImageReference(payload.imageRef);
    const requestedPlatform = normalizeOptionalPlatformValue(payload.platform);
    const credentials = normalizeSessionCredentials(payload.credentials);

    const ephemeralJob = {
      id: 'preview',
      ephemeral: true,
      registryBaseUrl,
      proxyBaseUrl: proxyBaseUrl || null,
      proxySource: payload.proxySource || null,
      proxyAuthActive: false,
      proxyDataActive: false,
      proxyActive: false,
      credentials,
      hasCredentials: Boolean(credentials),
      registryV2Base: buildRegistryV2Base(registryBaseUrl),
      repository: parsedReference.repository,
      imageRef: parsedReference.displayName,
      reference: parsedReference.reference,
      tag: parsedReference.tag,
      digest: parsedReference.digest,
      repoTag: parsedReference.repoTag,
      selectedPlatform: requestedPlatform,
      platformOptions: normalizePlatformList([requestedPlatform])
    };

    const resolved = await fetchManifestDescriptor(ephemeralJob, ephemeralJob.reference, abortController.signal);
    const platformOptions = await resolveAvailablePlatforms(ephemeralJob, resolved, requestedPlatform, abortController.signal);

    return {
      registryBaseUrl,
      imageRef: parsedReference.displayName,
      platformOptions,
      defaultPlatform: pickPreferredPlatform(platformOptions, requestedPlatform),
      proxyActive: Boolean(ephemeralJob.proxyActive),
      proxyMode: getJobProxyMode(ephemeralJob),
      proxySource: ephemeralJob.proxySource || null,
      proxyBaseUrl: ephemeralJob.proxyBaseUrl || null
    };
  } catch (error) {
    if (abortController.signal.aborted || error?.name === 'AbortError') {
      throw createError('platform-probe-aborted', 'Cancelled previous architecture lookup.');
    }
    throw error;
  } finally {
    if (runtime.platformProbeAbortController === abortController) {
      runtime.platformProbeAbortController = null;
    }
  }
}

async function resolveSizeTimeline(payload) {
  ensureRegistryClientSupport();

  const abortController = new AbortController();
  const registryBaseUrl = normalizeRegistryBaseUrl(payload.registryBaseUrl);
  const proxyBaseUrl = normalizeProxyBaseUrl(payload.proxyBaseUrl);
  const parsedReference = parseImageReference(payload.imageRef);
  const requestedPlatform = normalizeRequiredPlatformValue(payload.platform);
  const credentials = normalizeSessionCredentials(payload.credentials);
  const sampleSize = clampNumber(payload.sampleSize, 6, 20, 12);

  if (parsedReference.digest) {
    throw createError('digest-trend-unsupported', 'Size history works with tagged images. Enter a repository and tag instead of a digest.');
  }

  const ephemeralJob = {
    id: 'timeline',
    ephemeral: true,
    registryBaseUrl,
    proxyBaseUrl: proxyBaseUrl || null,
    proxySource: payload.proxySource || null,
    proxyAuthActive: false,
    proxyDataActive: false,
    proxyActive: false,
    credentials,
    hasCredentials: Boolean(credentials),
    registryV2Base: buildRegistryV2Base(registryBaseUrl),
    repository: parsedReference.repository,
    imageRef: parsedReference.displayName,
    reference: parsedReference.reference,
    tag: parsedReference.tag,
    digest: parsedReference.digest,
    repoTag: parsedReference.repoTag,
    selectedPlatform: requestedPlatform,
    platformOptions: normalizePlatformList([requestedPlatform])
  };

  const timeline = await buildSizeTimeline(ephemeralJob, {
    currentTag: parsedReference.tag || 'latest',
    sampleSize,
    signal: abortController.signal
  });

  return {
    registryBaseUrl,
    imageRef: parsedReference.displayName,
    repository: parsedReference.repository,
    platform: requestedPlatform,
    proxyActive: Boolean(ephemeralJob.proxyActive),
    proxyMode: getJobProxyMode(ephemeralJob),
    proxySource: ephemeralJob.proxySource || null,
    proxyBaseUrl: ephemeralJob.proxyBaseUrl || null,
    ...timeline
  };
}

async function pauseJob() {
  const job = assertJob();
  if (!runtime.runningTask) {
    if (job.status !== 'paused') {
      job.status = 'paused';
      job.phase = 'paused';
      job.statusMessage = 'Paused.';
      job.updatedAt = new Date().toISOString();
      await persistJob();
      postJobUpdate();
    }
    return buildSnapshot();
  }

  runtime.control = 'pause';
  runtime.abortController?.abort();
  pushLog('info', 'Pause requested.');
  return buildSnapshot();
}

async function resumeJob(payload = {}) {
  const job = assertJob();
  if (runtime.runningTask) {
    throw createError('job-already-running', 'The current job is already running.');
  }
  const sessionCredentials = normalizeSessionCredentials(payload.credentials);
  if (sessionCredentials || !runtime.sessionCredentials) {
    runtime.sessionCredentials = sessionCredentials;
    runtime.authCache.clear();
  }
  if (job.hasCredentials && !getJobCredentials(job)) {
    throw createError('credentials-required', 'Re-enter registry credentials to resume this private image.');
  }
  if (job.status !== 'paused' && job.status !== 'error' && job.status !== 'cancelled') {
    throw createError('job-not-paused', 'Only paused, cancelled, or failed jobs can be resumed.');
  }
  job.status = 'queued';
  job.phase = 'queued';
  job.statusMessage = 'Resuming from the last persisted checkpoint.';
  job.restorePrompt = false;
  job.lastError = null;
  job.updatedAt = new Date().toISOString();
  runtime.control = 'running';
  await persistJob();
  postJobUpdate();
  beginRunLoop();
  return buildSnapshot();
}

async function cancelJob() {
  const job = assertJob();
  runtime.control = 'cancel';
  runtime.abortController?.abort();
  if (!runtime.runningTask) {
    job.status = 'cancelled';
    job.phase = 'cancelled';
    job.statusMessage = 'Cancelled. Cached files are still available until you clear them.';
    job.updatedAt = new Date().toISOString();
    await persistJob();
    postJobUpdate();
  }
  pushLog('warn', 'Cancel requested.');
  return buildSnapshot();
}

async function clearJob() {
  runtime.clearingJob = true;
  runtime.control = 'cancel';
  runtime.abortController?.abort();
  if (runtime.runningTask) {
    await runtime.runningTask.catch(() => undefined);
  }
  await clearPersistentState();
  runtime.job = null;
  runtime.entries = new Map();
  runtime.control = 'idle';
  runtime.authCache.clear();
  runtime.sessionCredentials = null;
  runtime.logEntries = [];
  const snapshot = await buildSnapshot();
  runtime.clearingJob = false;
  postEvent({ kind: 'snapshot', snapshot });
  return snapshot;
}

async function listDirectory(payload) {
  const job = assertJob();
  ensureRootEntry();
  const path = normalizeFsPath(payload.path || '/');
  const entry = runtime.entries.get(path);
  if (!entry || entry.type !== 'dir') {
    throw createError('not-a-directory', `Path ${path} is not a directory.`);
  }

  const children = [];
  for (const candidate of runtime.entries.values()) {
    if (candidate.parentPath === path) {
      children.push(candidate);
    }
  }

  return {
    job: summarizeJob(job),
    path,
    entry,
    children: sortEntries(children),
    breadcrumbs: buildBreadcrumbs(path)
  };
}

async function getEntry(payload) {
  assertJob();
  const path = normalizeFsPath(payload.path || '/');
  const entry = runtime.entries.get(path);
  if (!entry) {
    throw createError('entry-not-found', `Entry ${path} was not found in the merged filesystem view.`);
  }
  return { entry };
}

async function previewText(payload) {
  const job = assertJob();
  const path = normalizeFsPath(payload.path || '/');
  const entry = runtime.entries.get(path);
  if (!entry) {
    throw createError('entry-not-found', `Entry ${path} was not found in the merged filesystem view.`);
  }
  if (entry.type !== 'file') {
    throw createError('preview-unsupported', 'Only regular files support text preview.');
  }
  if (entry.size > MAX_TEXT_FILE_PREVIEW_SIZE) {
    throw createError(
      'preview-too-large',
      `Text preview is limited to files up to ${formatBytes(MAX_TEXT_FILE_PREVIEW_SIZE)} in v1.`
    );
  }

  const layer = job.layers.find((candidate) => candidate.digest === entry.layerDigest);
  if (!layer || !layer.completed) {
    throw createError('layer-missing', 'The backing layer for this file is no longer available in local storage.');
  }

  const file = await getBlobFile(layer.digest);
  const capture = await scanTarStream(await openLayerContentStream(file, layer.mediaType), {
    capturePath: entry.path,
    captureLimit: MAX_PREVIEW_BYTES
  });

  if (!capture || !capture.entry) {
    throw createError('preview-missing', 'The selected file could not be replayed from its layer archive.');
  }

  const decoded = decodePreviewBytes(capture.bytes);
  return {
    entry,
    content: decoded.content,
    truncated: capture.truncated || decoded.truncated,
    isBinary: decoded.isBinary
  };
}

async function exportArchive() {
  const job = assertJob();
  if (job.status !== 'ready') {
    throw createError('job-not-ready', 'The export archive is only available after the image is fully downloaded and indexed.');
  }
  if (runtime.runningTask) {
    throw createError('job-busy', 'Wait for the active operation to finish before exporting.');
  }

  runtime.control = 'running';
  runtime.abortController = new AbortController();
  job.status = 'exporting';
  job.phase = 'export';
  job.statusMessage = 'Building a Docker-compatible image.tar in OPFS.';
  job.updatedAt = new Date().toISOString();
  postProgress('export', 'Assembling image archive.', 0, 1);
  await persistJob();
  postJobUpdate();

  try {
    const exportInfo = await buildDockerArchive(job, runtime.abortController.signal);
    job.exportInfo = exportInfo;
    job.status = 'ready';
    job.phase = 'ready';
    job.statusMessage = '';
    job.updatedAt = new Date().toISOString();
    await persistJob();
    postProgress('export', 'Archive ready.', 1, 1);
    postJobUpdate();
    pushLog('info', `Built ${exportInfo.suggestedName} in OPFS.`);
    return exportInfo;
  } catch (error) {
    if (isRecoverableStop(error)) {
      if (runtime.clearingJob && error.code === 'job-cancelled') {
        throw error;
      }
      job.status = error.code === 'job-cancelled' ? 'cancelled' : 'paused';
      job.phase = job.status;
      job.statusMessage =
        error.code === 'job-cancelled'
          ? 'Export cancelled. The existing cache remains available.'
          : 'Export paused. Resume to rebuild the archive.';
      job.updatedAt = new Date().toISOString();
      await persistJob();
      postJobUpdate();
      throw error;
    }
    job.status = 'error';
    job.phase = 'error';
    job.statusMessage = error.message;
    job.lastError = serializeError(error);
    job.updatedAt = new Date().toISOString();
    await persistJob();
    postJobUpdate();
    throw error;
  } finally {
    runtime.abortController = null;
    runtime.control = 'idle';
  }
}

function beginRunLoop() {
  runtime.runToken += 1;
  const token = runtime.runToken;
  runtime.runningTask = runJobLoop(token)
    .catch(async (error) => {
      if (!runtime.job) return;
      if (runtime.runToken !== token) return;
      if (isRecoverableStop(error)) {
        if (runtime.clearingJob && error.code === 'job-cancelled') {
          return;
        }
      runtime.job.status = error.code === 'job-cancelled' ? 'cancelled' : 'paused';
      runtime.job.phase = runtime.job.status;
      runtime.job.statusMessage =
        error.code === 'job-cancelled'
          ? 'Cancelled. Cached files remain available until you clear them.'
          : 'Paused. Resume to continue.';
      runtime.job.restorePrompt = false;
      runtime.job.updatedAt = new Date().toISOString();
        await persistJob();
        postJobUpdate();
        return;
      }

      runtime.job.status = 'error';
      runtime.job.phase = 'error';
      runtime.job.statusMessage = error.message;
      runtime.job.lastError = serializeError(error);
      runtime.job.updatedAt = new Date().toISOString();
      pushLog('error', error.message);
      await persistJob();
      postJobUpdate();
    })
    .finally(() => {
      if (runtime.runToken === token) {
        runtime.runningTask = null;
        runtime.abortController = null;
        runtime.control = 'idle';
      }
    });
}

async function runJobLoop(runToken) {
  const job = assertJob();
  runtime.control = 'running';

  if (!job.manifest) {
    if (!job.skipInitialProbe) {
      await probeRegistry(job, runToken);
    }
    await resolveJob(job, runToken);
    job.skipInitialProbe = false;
    await persistJob();
    postJobUpdate();
  }

  await preflightQuota(job);

  if (job.config && !job.config.completed) {
    await downloadBlob(job, job.config, 'config', runToken);
    await persistJob();
    postJobUpdate();
  }

  for (let index = 0; index < job.layers.length; index += 1) {
    if (!job.layers[index].completed) {
      await downloadBlob(job, job.layers[index], `layer ${index + 1}/${job.layers.length}`, runToken);
      await persistJob();
      postJobUpdate();
    }
  }

  ensureRootEntry();
  for (let index = 0; index < job.layers.length; index += 1) {
    if (!job.layers[index].indexed) {
      await indexLayer(job, job.layers[index], index, runToken);
      await persistJobAndEntries();
      postJobUpdate();
    }
  }

  job.status = 'ready';
  job.phase = 'ready';
  job.statusMessage = 'Image is ready to browse and export.';
  job.updatedAt = new Date().toISOString();
  job.totals.entryCount = runtime.entries.size;
  job.totals.directoryCount = countEntriesByType('dir');
  job.totals.fileCount = countEntriesByType('file');
  await persistJobAndEntries();
  postJobUpdate();
  pushLog('info', `Ready with ${formatNumber(job.totals.entryCount)} visible paths.`);
}

async function probeRegistry(job, runToken) {
  assertRunActive(runToken);
  setJobPhase(job, 'probing', 'Checking registry access and auth headers.');
  postProgress('resolve', 'Probing registry access and auth support.', 0, 3);
  await persistJob();
  postJobUpdate();

  const pingUrl = `${job.registryV2Base}/`;
  const pingResponse = await registryFetch(job, pingUrl, {
    method: 'GET',
    expectAuthChallenge: true,
    scope: repositoryPullScope(job.repository),
    allowNotFound: false
  });
  if (![200, 401].includes(pingResponse.status)) {
    await throwResponseError(pingResponse, 'Registry probe failed.', { kind: 'probe', job });
  }
  await pingResponse.body?.cancel().catch(() => undefined);
  postProgress('resolve', 'Registry base endpoint responded.', 1, 3);

  const manifestUrl = buildManifestUrl(job, job.reference);
  const manifestResponse = await registryFetch(job, manifestUrl, {
    method: 'GET',
    headers: {
      Accept: MANIFEST_ACCEPT.join(', ')
    },
    scope: repositoryPullScope(job.repository),
    expectAuthChallenge: true
  });
  if (!manifestResponse.ok) {
    await throwResponseError(manifestResponse, 'Unable to read the image manifest.', {
      kind: 'manifest',
      job,
      reference: job.reference,
      isUserReference: true
    });
  }
  await manifestResponse.body?.cancel().catch(() => undefined);
  postProgress('resolve', 'Manifest endpoint is readable.', 2, 3);
}

async function resolveJob(job, runToken) {
  assertRunActive(runToken);
  setJobPhase(job, 'resolving', 'Resolving manifest and platform metadata.');
  pushLog('info', `Resolving ${job.repository}:${job.reference}.`);
  const resolved = await fetchManifestDescriptor(job, job.reference);
  let manifestResource = resolved;

  if (IMAGE_INDEX_MEDIA_TYPES.has(resolved.mediaType)) {
    const availablePlatforms = extractSupportedPlatformsFromIndex(resolved.json.manifests || []);
    job.platformOptions = normalizePlatformList(availablePlatforms);
    job.selectedPlatform = pickPreferredPlatform(job.platformOptions, job.selectedPlatform);

    const descriptor = selectPlatformDescriptor(resolved.json.manifests || [], job.selectedPlatform);
    if (!descriptor) {
      throw createError(
        'platform-not-found',
        `The image manifest list does not contain ${job.selectedPlatform}. Try another architecture from the list for this image.`
      );
    }
    job.sourceIndex = {
      digest: resolved.digest,
      size: resolved.size,
      mediaType: resolved.mediaType
    };
    job.selectedPlatform = descriptorPlatformValue(descriptor.platform) || job.selectedPlatform;
    pushLog('info', `Selected ${job.selectedPlatform} from the multi-platform index.`);
    manifestResource = await fetchManifestDescriptor(job, descriptor.digest);
  }

  if (!IMAGE_MANIFEST_MEDIA_TYPES.has(manifestResource.mediaType)) {
    throw createError('unsupported-manifest', `Unsupported manifest media type: ${manifestResource.mediaType}`);
  }

  const manifestJson = manifestResource.json;
  job.platformOptions = await resolveAvailablePlatforms(job, manifestResource, job.selectedPlatform);
  job.selectedPlatform = pickPreferredPlatform(job.platformOptions, job.selectedPlatform);
  const configDescriptor = manifestJson.config;
  const layerDescriptors = manifestJson.layers || [];
  if (!configDescriptor || !splitDigest(configDescriptor.digest)) {
    throw createError('invalid-config', 'Manifest config descriptor is missing or invalid.');
  }

  const foreignLayer = layerDescriptors.find((descriptor) => NON_DISTRIBUTABLE_LAYER_MEDIA_TYPES.has(descriptor.mediaType));
  if (foreignLayer) {
    throw createError(
      'foreign-layer',
      `The image uses a nondistributable or foreign layer (${foreignLayer.mediaType}), which v1 refuses to export browser-side.`
    );
  }

  job.manifest = {
    digest: manifestResource.digest,
    hex: splitDigest(manifestResource.digest).hex,
    mediaType: manifestResource.mediaType,
    size: manifestResource.size,
    completed: true
  };

  await writeBlobBytes(job.manifest.digest, manifestResource.bytes);

  job.config = buildBlobState(configDescriptor, 0);
  job.layers = layerDescriptors.map((descriptor, index) => buildBlobState(descriptor, index + 1));
  job.totals.totalLayers = job.layers.length;
  job.totals.compressedBytes =
    job.config.size + job.layers.reduce((total, descriptor) => total + descriptor.size, 0) + job.manifest.size;
  job.manifestPlatform = {
    os: manifestJson.os || manifestJson.config?.os || manifestJson.platform?.os || job.selectedPlatform.split('/')[0],
    architecture:
      manifestJson.architecture || manifestJson.config?.architecture || manifestJson.platform?.architecture || job.selectedPlatform.split('/')[1],
    variant: manifestJson.variant || manifestJson.config?.variant || manifestJson.platform?.variant || job.selectedPlatform.split('/')[2] || ''
  };
  job.statusMessage = `Resolved ${job.layers.length} layer${job.layers.length === 1 ? '' : 's'} for ${job.selectedPlatform}.`;
  job.updatedAt = new Date().toISOString();

  const blobProbeResponse = await fetchBlobChunk(job, job.config.digest, 0, 0);
  if (![200, 206].includes(blobProbeResponse.status)) {
    await throwResponseError(blobProbeResponse, 'The config blob could not be fetched after manifest resolution.', {
      kind: 'config',
      job
    });
  }
  await blobProbeResponse.body?.cancel().catch(() => undefined);
  postProgress('resolve', 'Blob endpoint is readable.', 3, 3);

  if (job.totals.compressedBytes > MAX_RELIABLE_COMPRESSED_SIZE) {
    postProgress(
      'quota-warning',
      `This image is ${formatBytes(job.totals.compressedBytes)} compressed. v1 is only guaranteed up to ${formatBytes(
        MAX_RELIABLE_COMPRESSED_SIZE
      )}; continuing in best-effort mode.`,
      job.totals.compressedBytes,
      MAX_RELIABLE_COMPRESSED_SIZE
    );
  }
}

function buildBlobState(descriptor, order) {
  const digest = splitDigest(descriptor.digest);
  if (!digest) {
    throw createError('invalid-digest', `Unsupported descriptor digest: ${descriptor.digest}`);
  }
  return {
    digest: digest.digest,
    hex: digest.hex,
    algorithm: digest.algorithm,
    mediaType: descriptor.mediaType || '',
    size: Number(descriptor.size || 0),
    completed: false,
    downloadedBytes: 0,
    rangeSupported: null,
    indexed: false,
    order
  };
}

async function preflightQuota(job) {
  const storage = await collectStorageEstimate();
  job.storage = storage;
  const requiredBytes = job.totals.compressedBytes;
  const availableBytes = Math.max(0, storage.quota - storage.usage);
  if (requiredBytes > 0 && availableBytes > 0 && requiredBytes > availableBytes) {
    throw createError(
      'quota-insufficient',
      `This image needs ${formatBytes(requiredBytes)} but the browser only reports ${formatBytes(
        availableBytes
      )} free. Clear storage or use a smaller image.`
    );
  }
  await persistJob();
  postJobUpdate();
}

async function downloadBlob(job, blobState, label, runToken) {
  assertRunActive(runToken);
  const descriptorDigest = splitDigest(blobState.digest);
  if (!descriptorDigest || descriptorDigest.algorithm !== 'sha256') {
    throw createError('unsupported-digest', `Only sha256 descriptors are supported. Received ${blobState.digest}.`);
  }

  const fileHandle = await getBlobFileHandle(blobState.digest, true);
  let downloadedBytes = blobState.downloadedBytes || 0;
  let rangeSupported = blobState.rangeSupported;
  const hasher = new Sha256();

  if (downloadedBytes > 0) {
    if (rangeSupported === false) {
      await restartBlobDownloadFromScratch(job, blobState, fileHandle, label);
      downloadedBytes = 0;
      rangeSupported = null;
    }
    if (downloadedBytes > 0) {
      const existingFile = await fileHandle.getFile();
      if (existingFile.size !== downloadedBytes) {
        downloadedBytes = 0;
        blobState.downloadedBytes = 0;
        rangeSupported = null;
        await truncateFile(fileHandle);
      } else {
        await hashExistingFile(existingFile, hasher);
        pushLog('info', `Resuming ${label} from ${formatBytes(downloadedBytes)}.`);
      }
    }
  }

  setJobPhase(job, 'downloading', `Downloading ${label}.`);
  postProgress(
    'download',
    `Downloading ${label}.`,
    job.totals.downloadedBytes,
    job.totals.compressedBytes,
    buildDownloadProgressMeta(job, blobState, downloadedBytes)
  );

  const writable = await fileHandle.createWritable({ keepExistingData: true });
  try {
    while (downloadedBytes < blobState.size) {
      assertRunActive(runToken);
      throwIfStopped();
      runtime.abortController = new AbortController();

      const chunkEnd = Math.min(blobState.size - 1, downloadedBytes + CHUNK_SIZE - 1);
      const response = await fetchBlobChunk(job, blobState.digest, downloadedBytes, chunkEnd, runtime.abortController.signal);
      if (response.status === 200 && downloadedBytes > 0) {
        await writable.close().catch(() => undefined);
        await restartBlobDownloadFromScratch(job, blobState, fileHandle, label);
        return downloadBlob(job, blobState, label, runToken);
      }
      if (![200, 206].includes(response.status)) {
        await throwResponseError(response, `Failed to download ${label}.`, {
          kind: 'blob',
          job,
          label
        });
      }

      rangeSupported = response.status === 206;
      const reader = response.body?.getReader();
      if (!reader) {
        throw createError('missing-body', `The registry returned no body for ${label}.`);
      }

      while (true) {
        assertRunActive(runToken);
        throwIfStopped();
        const next = await reader.read();
        if (next.done) break;
        const chunk = next.value;
        hasher.update(chunk);
        await writable.write({
          type: 'write',
          position: downloadedBytes,
          data: chunk
        });
        downloadedBytes += chunk.byteLength;
        blobState.downloadedBytes = downloadedBytes;
        blobState.rangeSupported = rangeSupported;
        blobState.completed = false;
        updateDownloadedBytes(job);
        job.updatedAt = new Date().toISOString();
        await persistJob();
        postProgress(
          'download',
          `Downloading ${label}.`,
          job.totals.downloadedBytes,
          job.totals.compressedBytes,
          buildDownloadProgressMeta(job, blobState, downloadedBytes)
        );
      }

      if (!rangeSupported) {
        break;
      }
    }
  } catch (error) {
    await writable.close().catch(() => undefined);
    throw error;
  }

  await writable.close();

  if (downloadedBytes !== blobState.size) {
    throw createError(
      'size-mismatch',
      `Downloaded ${label} size mismatch. Expected ${formatBytes(blobState.size)} but stored ${formatBytes(downloadedBytes)}.`
    );
  }

  const actualDigest = hasher.hex();
  if (actualDigest !== descriptorDigest.hex) {
    await removeBlob(blobState.digest);
    blobState.downloadedBytes = 0;
    blobState.completed = false;
    throw createError('digest-mismatch', `Digest mismatch while downloading ${label}. Expected ${blobState.digest}.`);
  }

  blobState.completed = true;
  blobState.downloadedBytes = downloadedBytes;
  blobState.rangeSupported = rangeSupported ?? false;
  updateDownloadedBytes(job);
  job.updatedAt = new Date().toISOString();
  pushLog('info', `Verified ${label} (${formatBytes(downloadedBytes)}).`);
}

async function restartBlobDownloadFromScratch(job, blobState, fileHandle, label) {
  await truncateFile(fileHandle);
  blobState.downloadedBytes = 0;
  blobState.completed = false;
  blobState.rangeSupported = null;
  updateDownloadedBytes(job);
  job.updatedAt = new Date().toISOString();
  pushLog('warn', `The registry does not support resuming ${label}. Restarting this blob from the beginning.`);
  await persistJob();
  postProgress(
    'download',
    `Downloading ${label}.`,
    job.totals.downloadedBytes,
    job.totals.compressedBytes,
    buildDownloadProgressMeta(job, blobState, 0)
  );
}

async function indexLayer(job, blobState, index, runToken) {
  assertRunActive(runToken);
  throwIfStopped();

  const compression = detectLayerCompression(blobState.mediaType);
  if (compression === 'zstd') {
    throw createError(
      'zstd-unsupported',
      'This image uses zstd-compressed layers. v1 detects them and fails clearly because no browser-native zstd decoder is bundled yet.'
    );
  }
  if (compression === 'unknown') {
    throw createError('layer-media-type-unsupported', `Unsupported layer media type: ${blobState.mediaType}`);
  }

  setJobPhase(job, 'indexing', `Indexing layer ${index + 1} of ${job.layers.length}.`);
  postProgress('index', `Indexing layer ${index + 1} of ${job.layers.length}.`, index, job.layers.length);

  const layerFile = await getBlobFile(blobState.digest);
  const layerRecords = {
    opaqueDirs: new Set(),
    whiteouts: new Set(),
    entries: []
  };

  await scanTarStream(await openLayerContentStream(layerFile, blobState.mediaType), {
    onEntry(entry) {
      const normalizedPath = normalizeFsPath(entry.path);
      const name = basenameFsPath(normalizedPath);
      const parentPath = parentFsPath(normalizedPath);

      if (name === '.wh..wh..opq') {
        if (parentPath) {
          layerRecords.opaqueDirs.add(parentPath);
        }
        return;
      }

      if (name.startsWith('.wh.')) {
        const target = joinFsPath(parentPath || '/', name.slice(4));
        layerRecords.whiteouts.add(target);
        return;
      }

      if (entry.type === 'other') return;

      layerRecords.entries.push({
        path: normalizedPath,
        name: basenameFsPath(normalizedPath),
        parentPath,
        type: entry.type,
        size: entry.size,
        mode: entry.mode,
        uid: entry.uid,
        gid: entry.gid,
        mtime: entry.mtime ? new Date(entry.mtime * 1000).toISOString() : null,
        linkname: entry.linkname || '',
        xattrs: entry.xattrs && Object.keys(entry.xattrs).length ? entry.xattrs : null,
        layerDigest: blobState.digest,
        layerIndex: index
      });
    }
  });

  for (const opaqueDir of layerRecords.opaqueDirs) {
    removeDescendants(opaqueDir);
  }

  for (const whiteoutPath of layerRecords.whiteouts) {
    removePathAndDescendants(whiteoutPath);
  }

  for (const entry of layerRecords.entries) {
    if (entry.path === '/') continue;
    runtime.entries.set(entry.path, entry);
    ensureParentDirectories(entry.parentPath);
  }

  blobState.indexed = true;
  job.totals.indexedLayers = job.layers.filter((layer) => layer.indexed).length;
  job.totals.entryCount = runtime.entries.size;
  job.totals.directoryCount = countEntriesByType('dir');
  job.totals.fileCount = countEntriesByType('file');
  job.updatedAt = new Date().toISOString();
}

function removeDescendants(parentPath) {
  const normalizedParent = normalizeFsPath(parentPath);
  for (const key of [...runtime.entries.keys()]) {
    if (key.startsWith(`${normalizedParent}/`)) {
      runtime.entries.delete(key);
    }
  }
  ensureParentDirectories(normalizedParent);
}

function removePathAndDescendants(targetPath) {
  const normalizedTarget = normalizeFsPath(targetPath);
  for (const key of [...runtime.entries.keys()]) {
    if (key === normalizedTarget || key.startsWith(`${normalizedTarget}/`)) {
      runtime.entries.delete(key);
    }
  }
}

function ensureParentDirectories(parentPath) {
  let current = normalizeFsPath(parentPath || '/');
  while (current && !runtime.entries.has(current)) {
    runtime.entries.set(current, {
      ...structuredCloneSafe(ROOT_ENTRY),
      path: current,
      name: basenameFsPath(current),
      parentPath: parentFsPath(current)
    });
    current = parentFsPath(current);
  }
}

function ensureRootEntry() {
  if (!runtime.entries.has('/')) {
    runtime.entries.set('/', structuredCloneSafe(ROOT_ENTRY));
  }
}

function countEntriesByType(type) {
  let total = 0;
  for (const entry of runtime.entries.values()) {
    if (entry.type === type) total += 1;
  }
  return total;
}

async function fetchManifestDescriptor(job, reference, signal) {
  const url = buildManifestUrl(job, reference);
  let response = await registryFetch(job, url, {
    method: 'GET',
    headers: {
      Accept: MANIFEST_ACCEPT.join(', ')
    },
    scope: repositoryPullScope(job.repository),
    expectAuthChallenge: true,
    signal
  });
  if (!response.ok) {
    await throwResponseError(response, `Unable to read manifest ${reference}.`, {
      kind: 'manifest',
      job,
      reference,
      isUserReference: reference === job.reference
    });
  }

  const bytes = new Uint8Array(await response.arrayBuffer());
  const digest = `sha256:${new Sha256().update(bytes).hex()}`;
  if (splitDigest(reference) && digest !== reference.toLowerCase()) {
    throw createError('manifest-digest-mismatch', `Resolved manifest did not match requested digest ${reference}.`);
  }

  let json;
  try {
    json = JSON.parse(new TextDecoder().decode(bytes));
  } catch {
    throw createError('manifest-parse-error', 'The registry returned a manifest body that is not valid JSON.');
  }

  const mediaType = String(response.headers.get('content-type') || json.mediaType || '').split(';')[0].trim();
  return {
    digest,
    size: bytes.byteLength,
    mediaType,
    json,
    bytes
  };
}

function selectPlatformDescriptor(manifests, requestedPlatform) {
  const preferred = manifests.find((descriptor) => platformMatches(descriptor.platform, requestedPlatform));
  if (preferred) return preferred;

  const fallbackPlatform = pickPreferredPlatform(extractSupportedPlatformsFromIndex(manifests), '', DEFAULT_PLATFORM);
  if (fallbackPlatform) {
    const fallbackDescriptor = manifests.find((descriptor) => platformMatches(descriptor.platform, fallbackPlatform));
    if (fallbackDescriptor) return fallbackDescriptor;
  }

  return manifests.find((descriptor) => Boolean(descriptorPlatformValue(descriptor.platform))) || manifests[0] || null;
}

async function registryFetch(job, url, options = {}) {
  const method = options.method || 'GET';
  const scope = options.scope || repositoryPullScope(job.repository);
  const headers = new Headers(options.headers || {});
  const cachedToken = getCachedAuthToken(job, scope);
  if (cachedToken) {
    headers.set('Authorization', `Bearer ${cachedToken}`);
  } else {
    applyBasicCredentials(headers, job);
  }
  const requestInit = {
    method,
    headers,
    signal: options.signal,
    mode: 'cors',
    credentials: 'omit',
    cache: 'no-store'
  };

  let response;
  try {
    response = await fetchWithProxyFallback(
      job,
      url,
      requestInit,
      {
        kind: 'data',
        reason: `Direct registry access to ${describeHost(url)} was blocked. Retrying through Cloudflare Worker proxy.`
      }
    );
  } catch (error) {
    if (isAbortLikeError(error, options.signal)) {
      throw error;
    }
    throw createError('cors-probe-failed', buildRegistryCorsFailureMessage(job, url));
  }

  if (response.status !== 401) {
    return response;
  }

  let challengeHeader = response.headers.get('www-authenticate');
  if (!challengeHeader) {
    const canUseAuthProxy =
      Boolean(job?.proxyBaseUrl && job?.proxyAuthActive) ||
      (await activateProxyIfNeeded(job, 'auth', 'Registry auth challenge headers were hidden from the browser. Retrying through Cloudflare Worker proxy.'));
    if (canUseAuthProxy) {
      const authResponse = await fetchWithOptionalProxy(job, url, requestInit, 'auth');
      if (authResponse.status !== 401) {
        return authResponse;
      }
      response = authResponse;
      challengeHeader = response.headers.get('www-authenticate');
    }
    if (!challengeHeader) {
      throw createError(
        'auth-header-hidden',
        job?.proxyAuthActive
          ? 'The registry returned 401 but the proxy could not read a WWW-Authenticate header from the upstream response.'
          : 'The registry returned 401 but did not expose the WWW-Authenticate header over CORS, so a browser client cannot complete the Bearer flow.'
      );
    }
  }

  const challenge = parseWwwAuthenticate(challengeHeader);
  if (challenge.scheme !== 'bearer') {
    return response;
  }

  const nextScope = challenge.params.scope || scope;
  const authToken = await fetchAuthToken(job, challenge.params.realm, challenge.params.service, nextScope, options.signal);
  runtime.authCache.set(cacheKey(job, challenge.params.realm, challenge.params.service, nextScope), authToken);

  const retryHeaders = new Headers(options.headers || {});
  retryHeaders.set('Authorization', `Bearer ${authToken}`);

  try {
    return await fetchWithProxyFallback(
      job,
      url,
      {
        ...requestInit,
        headers: retryHeaders
      },
      {
        kind: 'data',
        reason: `Authorized registry access to ${describeHost(url)} was blocked. Retrying through Cloudflare Worker proxy.`
      }
    );
  } catch (error) {
    if (isAbortLikeError(error, options.signal)) {
      throw error;
    }
    throw createError(
      'cors-probe-failed',
      job?.proxyBaseUrl && job?.proxyDataActive
        ? `The Cloudflare Worker proxy at ${job.proxyBaseUrl} could not forward the authorized request to ${url}.`
        : `The authorized registry request to ${url} failed due to CORS or network restrictions.`
    );
  }
}

function buildRegistryCorsFailureMessage(job, url) {
  if (job?.proxyBaseUrl && job?.proxyDataActive) {
    return `The Cloudflare Worker proxy at ${job.proxyBaseUrl} could not fetch ${url}. Check the Worker deployment and its upstream policy.`;
  }
  try {
    const target = new URL(url);
    if (target.hostname.toLowerCase() === 'mirror.gcr.io') {
      return `The registry request to ${url} failed before the browser could read it. mirror.gcr.io is not exposing the CORS headers this client needs for browser pulls.`;
    }
  } catch {
    // Ignore URL parsing errors and fall back to the generic message.
  }
  return `The registry request to ${url} failed before the browser could read it. This usually means CORS is not enabled for browser pulls.`;
}

function getCachedAuthToken(job, scope) {
  for (const [key, value] of runtime.authCache.entries()) {
    if (key.startsWith(`${buildCredentialCacheIdentity(job)}|`) && (key.endsWith(`|${scope}`) || key.endsWith('|'))) {
      return value;
    }
  }
  return null;
}

async function fetchAuthToken(job, realm, service, scope, signal) {
  if (!realm) {
    throw createError('auth-realm-missing', 'Registry auth challenge was missing a token realm URL.');
  }

  const key = cacheKey(job, realm, service, scope);
  if (runtime.authCache.has(key)) {
    return runtime.authCache.get(key);
  }

  const tokenUrl = new URL(realm);
  if (service) tokenUrl.searchParams.set('service', service);
  if (scope) tokenUrl.searchParams.set('scope', scope);

  let response;
  try {
    response = await fetchWithProxyFallback(
      job,
      tokenUrl,
      {
        method: 'GET',
        headers: buildBasicAuthHeaders(job),
        mode: 'cors',
        credentials: 'omit',
        cache: 'no-store',
        signal
      },
      {
        kind: 'auth',
        reason: `The auth token request for ${describeHost(tokenUrl)} was blocked. Retrying through Cloudflare Worker proxy.`
      }
    );
  } catch (error) {
    if (isAbortLikeError(error, signal)) {
      throw error;
    }
    throw createError(
      'token-cors-failed',
      job?.proxyBaseUrl && job?.proxyAuthActive
        ? `The Cloudflare Worker proxy at ${job.proxyBaseUrl} could not reach the auth token URL ${tokenUrl}.`
        : `The auth token request to ${tokenUrl} failed due to CORS or network restrictions.`
    );
  }

  if (!response.ok) {
    await throwResponseError(response, 'The registry token endpoint rejected the browser auth flow.', {
      kind: 'auth',
      job
    });
  }

  let json;
  try {
    json = await response.json();
  } catch {
    throw createError('token-parse-failed', 'The auth token endpoint returned invalid JSON.');
  }

  const token = json.token || json.access_token;
  if (!token) {
    throw createError('token-missing', 'The auth token response did not include token or access_token.');
  }
  runtime.authCache.set(key, token);
  return token;
}

async function fetchWithProxyFallback(job, url, init, options = {}) {
  const kind = options.kind === 'auth' ? 'auth' : 'data';
  try {
    return await fetchWithOptionalProxy(job, url, init, kind);
  } catch (error) {
    if (isAbortLikeError(error, init?.signal)) {
      throw error;
    }
    if (await activateProxyIfNeeded(job, kind, options.reason || 'Retrying through Cloudflare Worker proxy.')) {
      return fetchWithOptionalProxy(job, url, init, kind);
    }
    throw error;
  }
}

async function activateProxyIfNeeded(job, kind, reason) {
  if (!job?.proxyBaseUrl) {
    return false;
  }
  if (kind === 'auth') {
    if (job.proxyAuthActive) {
      return false;
    }
    job.proxyAuthActive = true;
  } else {
    if (job.proxyDataActive) {
      return false;
    }
    job.proxyDataActive = true;
  }
  job.proxyActive = Boolean(job.proxyAuthActive || job.proxyDataActive);
  job.updatedAt = new Date().toISOString();
  if (!job.ephemeral) {
    pushLog('warn', reason);
    await persistJob().catch(() => undefined);
    postJobUpdate();
  }
  return true;
}

async function fetchWithOptionalProxy(job, url, init, kind = 'data') {
  const proxyEnabled = kind === 'auth' ? Boolean(job?.proxyAuthActive) : Boolean(job?.proxyDataActive);
  const target = buildFetchTargetUrl(job?.proxyBaseUrl && proxyEnabled ? job.proxyBaseUrl : '', url);
  return fetch(target, init);
}

function buildFetchTargetUrl(proxyBaseUrl, url) {
  if (!proxyBaseUrl) {
    return url;
  }
  const proxyUrl = new URL(proxyBaseUrl);
  proxyUrl.searchParams.set('url', String(url));
  return proxyUrl.toString();
}

function getJobProxyMode(job) {
  const authProxy = Boolean(job?.proxyAuthActive);
  const dataProxy = Boolean(job?.proxyDataActive);
  if (authProxy && dataProxy) return 'mixed';
  if (dataProxy) return 'data';
  if (authProxy) return 'auth';
  return null;
}

function describeHost(url) {
  try {
    return new URL(String(url)).hostname;
  } catch {
    return String(url);
  }
}

function parseWwwAuthenticate(headerValue) {
  const raw = String(headerValue || '').trim();
  const [schemePart, ...restParts] = raw.split(/\s+/);
  const params = {};
  const rest = restParts.join(' ');
  const matches = rest.matchAll(/([a-zA-Z][a-zA-Z0-9_-]*)="([^"]*)"/g);
  for (const match of matches) {
    params[match[1].toLowerCase()] = match[2];
  }
  return {
    scheme: String(schemePart || '').toLowerCase(),
    params
  };
}

function normalizeSessionCredentials(credentials) {
  if (!credentials || typeof credentials !== 'object') {
    return null;
  }
  const username = String(credentials.username ?? '').trim();
  const password = String(credentials.password ?? '');
  if (!username && !password) {
    return null;
  }
  return { username, password };
}

function getJobCredentials(job) {
  return normalizeSessionCredentials(job?.credentials || runtime.sessionCredentials);
}

function buildCredentialCacheIdentity(job) {
  const credentials = getJobCredentials(job);
  if (!credentials) {
    return 'anonymous';
  }
  return `basic:${encodeBase64Utf8(`${credentials.username}:${credentials.password}`)}`;
}

function cacheKey(job, realm, service, scope) {
  return `${buildCredentialCacheIdentity(job)}|${realm || ''}|${service || ''}|${scope || ''}`;
}

function buildBasicAuthHeaders(job) {
  const headers = new Headers();
  applyBasicCredentials(headers, job);
  return headers;
}

function applyBasicCredentials(headers, job) {
  const credentials = getJobCredentials(job);
  if (!credentials) return;
  headers.set('Authorization', `Basic ${encodeBase64Utf8(`${credentials.username}:${credentials.password}`)}`);
}

function encodeBase64Utf8(value) {
  const bytes = new TextEncoder().encode(String(value || ''));
  let binary = '';
  const chunkSize = 0x8000;
  for (let index = 0; index < bytes.length; index += chunkSize) {
    binary += String.fromCharCode(...bytes.subarray(index, index + chunkSize));
  }
  return btoa(binary);
}

function isAbortLikeError(error, signal) {
  return Boolean(signal?.aborted || error?.name === 'AbortError');
}

function repositoryPullScope(repository) {
  return `repository:${repository}:pull`;
}

function buildManifestUrl(job, reference) {
  return `${job.registryV2Base}/${job.repository}/manifests/${reference}`;
}

function buildBlobUrl(job, digest) {
  return `${job.registryV2Base}/${job.repository}/blobs/${digest}`;
}

async function fetchBlobChunk(job, digest, start, end, signal) {
  const headers = start === 0 && end === 0 ? { Range: 'bytes=0-0' } : { Range: `bytes=${start}-${end}` };
  return registryFetch(job, buildBlobUrl(job, digest), {
    method: 'GET',
    headers,
    scope: repositoryPullScope(job.repository),
    signal
  });
}

async function throwResponseError(response, fallbackMessage, context = {}) {
  let details = '';
  try {
    details = await response.text();
  } catch {
    details = '';
  }
  const errorInfo = extractRegistryErrorInfo(details, response.headers.get('content-type'));
  const friendly = buildFriendlyRegistryError(response.status, errorInfo, context);
  if (response.status === 429) {
    throw createError(
      'rate-limited',
      context?.kind === 'tags'
        ? 'The registry returned HTTP 429 while sampling multiple tags for this chart. Downloading one image can still work because it uses fewer registry requests.'
        : 'The registry returned HTTP 429. Public registries often enforce pull rate limits for anonymous browser traffic.'
    );
  }
  if (friendly) {
    throw createError(friendly.code, friendly.message, {
      details,
      upstreamStatus: response.status,
      upstreamCode: errorInfo.primaryCode || null
    });
  }
  const detailMessage = errorInfo.summary || extractResponseDetailMessage(details, response.headers.get('content-type'));
  throw createError(
    'registry-http-error',
    `${fallbackMessage} HTTP ${response.status}.${detailMessage ? ` ${detailMessage}` : ''}`,
    { details }
  );
}

function extractResponseDetailMessage(details, contentType) {
  const raw = String(details || '').trim();
  if (!raw) return '';

  if (String(contentType || '').toLowerCase().includes('application/json')) {
    try {
      const parsed = JSON.parse(raw);
      if (typeof parsed?.error === 'string' && parsed.error.trim()) {
        return parsed.error.trim();
      }
      if (typeof parsed?.message === 'string' && parsed.message.trim()) {
        return parsed.message.trim();
      }
    } catch {
      // Fall through and try to use the raw text.
    }
  }

  if (raw.startsWith('<!doctype html') || raw.startsWith('<html') || raw.startsWith('{') || raw.length > 240) {
    return '';
  }

  return raw;
}

function extractRegistryErrorInfo(details, contentType) {
  const raw = String(details || '').trim();
  const info = {
    raw,
    primaryCode: '',
    primaryMessage: '',
    summary: ''
  };
  if (!raw) return info;

  if (String(contentType || '').toLowerCase().includes('application/json')) {
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed?.errors) && parsed.errors.length) {
        const first = parsed.errors[0] || {};
        info.primaryCode = String(first.code || '').trim().toUpperCase();
        info.primaryMessage = String(first.message || '').trim();
        info.summary = parsed.errors
          .map((entry) => [entry?.message, simplifyErrorDetail(entry?.detail)].filter(Boolean).join(': '))
          .filter(Boolean)
          .join(' ');
        return info;
      }
      if (typeof parsed?.error === 'string' && parsed.error.trim()) {
        info.primaryMessage = parsed.error.trim();
        info.summary = info.primaryMessage;
        return info;
      }
      if (typeof parsed?.message === 'string' && parsed.message.trim()) {
        info.primaryMessage = parsed.message.trim();
        info.summary = info.primaryMessage;
        return info;
      }
    } catch {
      // Fall through to raw-text handling.
    }
  }

  info.summary = extractResponseDetailMessage(raw, contentType);
  return info;
}

function simplifyErrorDetail(detail) {
  if (!detail) return '';
  if (typeof detail === 'string') return detail.trim();
  if (typeof detail === 'number' || typeof detail === 'boolean') return String(detail);
  if (Array.isArray(detail)) {
    return detail.map((entry) => simplifyErrorDetail(entry)).filter(Boolean).join(', ');
  }
  if (typeof detail === 'object') {
    return Object.values(detail)
      .map((entry) => simplifyErrorDetail(entry))
      .filter(Boolean)
      .join(', ');
  }
  return '';
}

function buildFriendlyRegistryError(status, errorInfo, context = {}) {
  const primaryCode = String(errorInfo?.primaryCode || '').toUpperCase();

  if (status === 404) {
    if (primaryCode === 'MANIFEST_UNKNOWN' || context.kind === 'manifest') {
      return buildFriendlyManifestMissingError(context);
    }
    if (primaryCode === 'NAME_UNKNOWN' || primaryCode === 'REPOSITORY_UNKNOWN') {
      return {
        code: 'image-not-found',
        message: `Couldn't find ${formatUserFacingImageTarget(context.job)}. Check the image name and registry and try again.`
      };
    }
    if (context.kind === 'config' || context.kind === 'blob') {
      return {
        code: 'image-files-missing',
        message: 'The registry is missing one of the image files, so this export cannot continue.'
      };
    }
  }

  if (status === 401 || status === 403) {
    if (isDockerHubRegistry(context.job)) {
      return {
        code: 'dockerhub-image-unavailable',
        message: `Couldn't access ${formatUserFacingImageTarget(context.job)} on Docker Hub. The image name may be wrong, the repository may be private, or the credentials may be invalid.`
      };
    }
    return {
      code: 'registry-access-denied',
      message: 'This image may be private, blocked, or temporarily unavailable. Add registry credentials if you have access.'
    };
  }

  if (status === 400 && ['NAME_INVALID', 'TAG_INVALID', 'MANIFEST_INVALID'].includes(primaryCode)) {
    return {
      code: 'invalid-image-reference',
      message: 'That image name or tag is not valid. Check the image path and try again.'
    };
  }

  return null;
}

function buildFriendlyManifestMissingError(context = {}) {
  const reference = String(context.reference || '').trim();
  const imageTarget = formatUserFacingImageTarget(context.job, reference);

  if (reference.startsWith('sha256:')) {
    return {
      code: 'image-digest-not-found',
      message: `Couldn't find that digest for ${imageTarget}. Check the image name and digest and try again.`
    };
  }

  if (context.isUserReference === false) {
    return {
      code: 'platform-manifest-missing',
      message: 'The registry could not resolve the selected platform manifest for this image. Try another architecture or tag.'
    };
  }

  return {
    code: 'image-tag-not-found',
    message: `Couldn't find ${imageTarget}. Check the image name and tag and try again.`
  };
}

function formatUserFacingImageTarget(job, reference = '') {
  const repository = String(job?.repository || 'image').trim();
  const fallbackTag = String(reference || job?.reference || job?.tag || 'latest').trim();
  const sourceImageInput = String(job?.sourceImageInput || '').trim();
  if (sourceImageInput) {
    if (fallbackTag.startsWith('sha256:')) {
      return sourceImageInput.includes('@') ? sourceImageInput : `${sourceImageInput}@${fallbackTag}`;
    }
    if (sourceImageInput.includes('@')) {
      return sourceImageInput;
    }
    const lastSlash = sourceImageInput.lastIndexOf('/');
    const lastColon = sourceImageInput.lastIndexOf(':');
    const hasTag = lastColon > lastSlash;
    return hasTag ? sourceImageInput : `${sourceImageInput}:${fallbackTag || 'latest'}`;
  }
  if (!repository) return 'that image';
  if (fallbackTag.startsWith('sha256:')) {
    return `${repository}@${fallbackTag}`;
  }
  return `${repository}:${fallbackTag || 'latest'}`;
}

function isDockerHubRegistry(job) {
  const base = String(job?.registryBaseUrl || '').toLowerCase();
  return base.includes('registry-1.docker.io') || base.includes('docker.io');
}

async function writeBlobBytes(digest, bytes) {
  const fileHandle = await getBlobFileHandle(digest, true);
  const writable = await fileHandle.createWritable();
  await writable.write(bytes);
  await writable.close();
}

async function hashExistingFile(file, hasher) {
  const reader = file.stream().getReader();
  while (true) {
    const next = await reader.read();
    if (next.done) break;
    hasher.update(next.value);
  }
}

function updateDownloadedBytes(job) {
  job.totals.downloadedBytes =
    job.manifest.size +
    (job.config?.downloadedBytes || 0) +
    job.layers.reduce((total, layer) => total + (layer.downloadedBytes || 0), 0);
}

function setJobPhase(job, phase, message) {
  job.phase = phase;
  job.status = phase;
  job.statusMessage = message;
  job.updatedAt = new Date().toISOString();
}

function postProgress(phase, message, completed, total, extra = {}) {
  postEvent({
    kind: 'progress',
    progress: {
      phase,
      message,
      completed,
      total,
      ...extra
    }
  });
}

function buildDownloadProgressMeta(job, blobState, downloadedBytes) {
  const totalFiles = 1 + Number(job?.layers?.length || 0);
  return {
    itemIndex: Number(blobState?.order || 0) + 1,
    itemCount: totalFiles,
    itemCompleted: Number(downloadedBytes || 0),
    itemTotal: Number(blobState?.size || 0)
  };
}

async function buildSizeTimeline(job, { currentTag, sampleSize, signal }) {
  let rawCandidates = [];
  let strategy = 'registry-sampling';

  if (isDockerHubRegistry(job)) {
    try {
      rawCandidates = await collectDockerHubTimelineCandidates(job, { signal, currentTag, sampleSize });
      if (rawCandidates.length) {
        strategy = 'docker-hub-history';
      }
    } catch (error) {
      if (!isTimelineMetadataFallbackError(error)) {
        throw error;
      }
    }
  }

  if (!rawCandidates.length) {
    rawCandidates = await collectRegistryTimelineCandidates(job, { signal, currentTag, sampleSize });
  }

  const selectionAttempts = buildTimelineSelectionAttempts(rawCandidates, { currentTag, sampleSize });
  const initialSelection = selectionAttempts[0] || { candidates: [], mode: 'default' };
  if (!initialSelection.candidates.length) {
    throw createError(
      'timeline-insufficient',
      'Not enough comparable tags were found for this repository. Try an image with versioned tags.'
    );
  }

  let appliedSelection = initialSelection;
  let timelineResolution = null;
  let points = [];

  for (const attempt of selectionAttempts) {
    if (!attempt?.candidates?.length) continue;
    const resolved = await resolveTimelinePoints(job, attempt.candidates, { signal });
    if (resolved.points.length >= 2) {
      appliedSelection = attempt;
      timelineResolution = resolved;
      points = resolved.points;
      break;
    }
    if (!timelineResolution || resolved.points.length > points.length) {
      appliedSelection = attempt;
      timelineResolution = resolved;
      points = resolved.points;
    }
  }

  if (points.length < 2) {
    throw createError(
      'timeline-insufficient',
      'Not enough tags for the selected architecture could be resolved into a useful size trend.'
    );
  }

  points.sort(compareTimelinePointOrder);
  const firstPoint = points[0];
  const latestPoint = points[points.length - 1];
  const deltaBytes = latestPoint.sizeBytes - firstPoint.sizeBytes;
  const deltaPercent = firstPoint.sizeBytes > 0 ? (deltaBytes / firstPoint.sizeBytes) * 100 : 0;
  const minSizeBytes = Math.min(...points.map((point) => point.sizeBytes));
  const maxSizeBytes = Math.max(...points.map((point) => point.sizeBytes));

  return {
    strategy,
    axis: points.some((point) => point.timestamp) ? 'time' : 'tag',
    selectionMode: appliedSelection.mode,
    points: points.map((point) => ({
      tag: point.tag,
      label: point.label,
      timestamp: point.timestamp || '',
      sizeBytes: point.sizeBytes,
      digest: point.digest || '',
      platform: point.platform || job.selectedPlatform
    })),
    stats: {
      currentTag: currentTag || '',
      selectionMode: appliedSelection.mode,
      consideredCount: rawCandidates.length,
      sampledCount: appliedSelection.candidates.length,
      plottedCount: points.length,
      rateLimited: Boolean(timelineResolution.rateLimited),
      latestSizeBytes: latestPoint.sizeBytes,
      firstSizeBytes: firstPoint.sizeBytes,
      deltaBytes,
      deltaPercent,
      minSizeBytes,
      maxSizeBytes
    }
  };
}

function buildTimelineSelectionAttempts(rawCandidates, { currentTag, sampleSize }) {
  const attempts = [];
  const seen = new Set();

  const pushAttempt = (selection) => {
    const candidates = Array.isArray(selection?.candidates) ? selection.candidates : [];
    if (!candidates.length) return;
    const key = `${selection.mode}::${candidates.map((candidate) => candidate?.tag || '').join('|')}`;
    if (seen.has(key)) return;
    seen.add(key);
    attempts.push(selection);
  };

  pushAttempt(selectTimelineCandidatesWithMeta(rawCandidates, { currentTag, sampleSize }));
  pushAttempt(
    selectTimelineCandidatesWithMeta(rawCandidates, {
      currentTag,
      sampleSize,
      allowPlainSemverFilter: false
    })
  );
  pushAttempt(
    selectTimelineCandidatesWithMeta(rawCandidates, {
      currentTag,
      sampleSize,
      allowFamilyMatch: false
    })
  );
  pushAttempt(
    selectTimelineCandidatesWithMeta(rawCandidates, {
      currentTag,
      sampleSize,
      allowPlainSemverFilter: false,
      allowFamilyMatch: false
    })
  );

  return attempts;
}

async function collectDockerHubTimelineCandidates(job, { signal, currentTag, sampleSize }) {
  const candidates = [];
  let nextUrl = buildDockerHubTagsApiUrl(job, 1, 100);
  let pageCount = 0;

  while (nextUrl && pageCount < 5 && candidates.length < 320) {
    pageCount += 1;
    let response;
    try {
      response = await fetchWithProxyFallback(
        job,
        nextUrl,
        {
          method: 'GET',
          headers: buildBasicAuthHeaders(job),
          mode: 'cors',
          credentials: 'omit',
          cache: 'no-store',
          signal
        },
        {
          kind: 'data',
          reason: `Docker Hub tag metadata for ${job.repository} was blocked. Retrying through Cloudflare Worker proxy.`
        }
      );
    } catch (error) {
      if (error?.code === 'rate-limited' && hasEnoughTimelineCandidates(candidates, { currentTag, sampleSize, minimumSelected: 2 })) {
        break;
      }
      throw error;
    }

    if (!response.ok) {
      if (response.status === 429 && hasEnoughTimelineCandidates(candidates, { currentTag, sampleSize, minimumSelected: 2 })) {
        break;
      }
      await throwResponseError(response, 'Unable to read Docker Hub tag metadata for this repository.', {
        kind: 'tags',
        job
      });
    }

    let payload;
    try {
      payload = await response.json();
    } catch {
      throw createError('tags-parse-failed', 'Docker Hub tag metadata returned invalid JSON.');
    }

    for (const result of payload?.results || []) {
      const candidate = buildDockerHubTimelineCandidate(result, job.selectedPlatform);
      if (candidate) {
        candidates.push(candidate);
      }
    }

    if (hasEnoughTimelineCandidates(candidates, { currentTag, sampleSize })) {
      break;
    }

    nextUrl = typeof payload?.next === 'string' && payload.next.trim() ? payload.next.trim() : '';
  }

  return dedupeTimelineCandidates(candidates);
}

async function collectRegistryTimelineCandidates(job, { signal, currentTag, sampleSize }) {
  const tags = [];
  let nextUrl = buildTagsListUrl(job, 100);
  let pageCount = 0;

  while (nextUrl && pageCount < 5 && tags.length < 500) {
    pageCount += 1;
    let response;
    try {
      response = await registryFetch(job, nextUrl, {
        method: 'GET',
        scope: repositoryPullScope(job.repository),
        expectAuthChallenge: true,
        signal
      });
    } catch (error) {
      if (
        error?.code === 'rate-limited' &&
        hasEnoughTimelineCandidates(tags.map((tag) => buildGenericTimelineCandidate(tag)), {
          currentTag,
          sampleSize,
          minimumSelected: 2
        })
      ) {
        break;
      }
      throw error;
    }
    if (!response.ok) {
      if (
        response.status === 429 &&
        hasEnoughTimelineCandidates(tags.map((tag) => buildGenericTimelineCandidate(tag)), {
          currentTag,
          sampleSize,
          minimumSelected: 2
        })
      ) {
        break;
      }
      await throwResponseError(response, 'Unable to list repository tags for this registry.', {
        kind: 'tags',
        job
      });
    }

    let payload;
    try {
      payload = await response.json();
    } catch {
      throw createError('tags-parse-failed', 'The registry tag list returned invalid JSON.');
    }

    for (const tag of payload?.tags || []) {
      const normalizedTag = String(tag || '').trim();
      if (normalizedTag) {
        tags.push(normalizedTag);
      }
    }

    if (hasEnoughTimelineCandidates(tags.map((tag) => buildGenericTimelineCandidate(tag)), { currentTag, sampleSize })) {
      break;
    }

    nextUrl = parseRegistryNextLink(response.headers.get('link'), nextUrl);
  }

  return dedupeTimelineCandidates(tags.map((tag) => buildGenericTimelineCandidate(tag)));
}

function buildDockerHubTimelineCandidate(result, platform) {
  const tag = String(result?.name || '').trim();
  if (!tag) return null;

  const images = Array.isArray(result?.images) ? result.images : [];
  const preferredImage =
    images.find((image) =>
      platformMatches(
        { os: image?.os, architecture: image?.architecture, variant: image?.variant },
        platform
      )
    ) || null;

  const fallbackImage = images[0] || null;
  const sizeBytes = Number(preferredImage?.size ?? result?.full_size ?? fallbackImage?.size ?? 0);
  const timestamp = String(preferredImage?.last_pushed || result?.last_updated || fallbackImage?.last_pushed || '').trim();
  const digest = String(preferredImage?.digest || fallbackImage?.digest || '').trim().toLowerCase();
  const resolvedPlatform = descriptorPlatformValue({
    os: preferredImage?.os || fallbackImage?.os,
    architecture: preferredImage?.architecture || fallbackImage?.architecture,
    variant: preferredImage?.variant || fallbackImage?.variant
  });

  return {
    tag,
    label: tag,
    timestamp: timestamp || '',
    sizeBytes: Number.isFinite(sizeBytes) && sizeBytes > 0 ? sizeBytes : 0,
    digest: splitDigest(digest)?.digest || '',
    platform: resolvedPlatform || '',
    hasComparableSize: Boolean(preferredImage && sizeBytes > 0),
    semver: parseTimelineVersionTag(tag),
    sortDate: parseTimelineDate(tag)
  };
}

function buildGenericTimelineCandidate(tag) {
  return {
    tag,
    label: tag,
    timestamp: '',
    sizeBytes: 0,
    digest: '',
    platform: '',
    hasComparableSize: false,
    semver: parseTimelineVersionTag(tag),
    sortDate: parseTimelineDate(tag)
  };
}

function dedupeTimelineCandidates(candidates) {
  const seen = new Set();
  const deduped = [];
  for (const candidate of candidates || []) {
    const tag = String(candidate?.tag || '').trim();
    if (!tag || seen.has(tag)) continue;
    seen.add(tag);
    deduped.push(candidate);
  }
  return deduped;
}

function selectTimelineCandidates(candidates, { currentTag, sampleSize }) {
  return selectTimelineCandidatesWithMeta(candidates, { currentTag, sampleSize }).candidates;
}

function selectTimelineCandidatesWithMeta(
  candidates,
  { currentTag, sampleSize, allowPlainSemverFilter = true, allowFamilyMatch = true } = {}
) {
  const comparable = (candidates || []).filter((candidate) => isComparableTimelineCandidate(candidate));
  if (!comparable.length) {
    return {
      candidates: [],
      mode: 'default'
    };
  }

  const preference = deriveTimelinePreference(currentTag);
  let filtered = comparable;
  let mode = 'default';
  const minimumFilteredCount = Math.max(2, Math.min(sampleSize, 4));

  if (allowPlainSemverFilter && isPlainSemverTimelineTag(currentTag)) {
    const plainSemverOnly = filtered.filter((candidate) => isPlainSemverTimelineTag(candidate?.tag));
    if (plainSemverOnly.length >= minimumFilteredCount) {
      filtered = plainSemverOnly;
      mode = 'semver-only';
    }
  }

  if (allowFamilyMatch && preference.tokens.length) {
    const familyMatches = filtered.filter((candidate) => matchesTimelinePreference(candidate.tag, preference.tokens));
    if (familyMatches.length >= minimumFilteredCount) {
      filtered = familyMatches;
      mode = 'family-match';
    }
  }

  if (!preference.allowPrerelease) {
    const stableOnly = filtered.filter((candidate) => !isPrereleaseTimelineTag(candidate.tag));
    if (stableOnly.length >= Math.min(sampleSize, 5)) {
      filtered = stableOnly;
    }
  }

  const directSizeCandidates = filtered.filter(
    (candidate) => candidate.hasComparableSize || candidate.tag === currentTag
  );
  if (directSizeCandidates.length >= Math.min(sampleSize, 4)) {
    filtered = directSizeCandidates;
  }

  if (filtered.some((candidate) => candidate.timestamp)) {
    filtered = collapseTimelineCandidatesByMonth(filtered);
  }

  filtered.sort(compareTimelineCandidateOrder);
  return {
    candidates: sampleTimelineCandidates(filtered, sampleSize, currentTag),
    mode
  };
}

function collapseTimelineCandidatesByMonth(candidates) {
  const sorted = [...candidates].sort(compareTimelineCandidateOrder);
  const buckets = new Map();
  for (const candidate of sorted) {
    if (!candidate.timestamp) continue;
    const monthKey = candidate.timestamp.slice(0, 7);
    buckets.set(monthKey, candidate);
  }
  return buckets.size ? [...buckets.values()] : sorted;
}

function sampleTimelineCandidates(candidates, sampleSize, currentTag) {
  if (candidates.length <= sampleSize) return candidates;

  const protectedIndices = new Set([0, candidates.length - 1]);
  const currentIndex = candidates.findIndex((candidate) => candidate.tag === currentTag);
  if (currentIndex >= 0) {
    protectedIndices.add(currentIndex);
  }

  const indices = new Set(protectedIndices);
  const span = candidates.length - 1;
  for (let index = 1; index < sampleSize - 1; index += 1) {
    indices.add(Math.round((index * span) / Math.max(1, sampleSize - 1)));
  }

  while (indices.size > sampleSize) {
    const removable = [...indices]
      .filter((index) => !protectedIndices.has(index))
      .sort((left, right) => left - right);
    if (!removable.length) break;
    indices.delete(removable[Math.floor(removable.length / 2)]);
  }

  return [...indices]
    .sort((left, right) => left - right)
    .map((index) => candidates[index])
    .filter(Boolean);
}

function hasEnoughTimelineCandidates(candidates, { currentTag, sampleSize, minimumSelected } = {}) {
  const selected = selectTimelineCandidates(candidates, { currentTag, sampleSize });
  return selected.length >= Math.max(2, Number(minimumSelected || sampleSize || 0));
}

async function resolveTimelinePoints(job, candidates, { signal }) {
  const points = [];
  let rateLimited = false;
  for (const candidate of candidates) {
    try {
      const resolved = await resolveTimelinePoint(job, candidate, signal);
      if (resolved) {
        points.push(resolved);
      }
    } catch (error) {
      if (error?.code === 'rate-limited') {
        rateLimited = true;
        if (points.length >= 2) {
          break;
        }
      }
      if (!shouldSkipTimelineCandidateError(error)) {
        throw error;
      }
    }
  }
  return { points, rateLimited };
}

async function resolveTimelinePoint(job, candidate, signal) {
  let sizeBytes = Number(candidate.sizeBytes || 0);
  let digest = candidate.digest || '';
  let platform = candidate.platform || job.selectedPlatform;

  if (!(sizeBytes > 0 && digest && platform === job.selectedPlatform)) {
    const resolved = await resolveManifestSizeForReference(job, candidate.tag, job.selectedPlatform, signal);
    if (!resolved) return null;
    sizeBytes = resolved.sizeBytes;
    digest = resolved.digest;
    platform = resolved.platform;
  }

  if (!(sizeBytes > 0)) {
    return null;
  }

  return {
    tag: candidate.tag,
    label: candidate.timestamp ? formatTimelineLabel(candidate) : candidate.tag,
    timestamp: candidate.timestamp || '',
    sizeBytes,
    digest,
    platform
  };
}

async function resolveManifestSizeForReference(job, reference, platform, signal) {
  const resolved = await fetchManifestDescriptor(job, reference, signal);
  let manifestResource = resolved;
  let selectedPlatform = platform;

  if (IMAGE_INDEX_MEDIA_TYPES.has(resolved.mediaType)) {
    const descriptor = selectPlatformDescriptor(resolved.json.manifests || [], platform);
    if (!descriptor) {
      throw createError('platform-manifest-missing', 'The selected platform is not available for this tag.');
    }
    selectedPlatform = descriptorPlatformValue(descriptor.platform) || platform;
    manifestResource = await fetchManifestDescriptor(job, descriptor.digest, signal);
  }

  if (!IMAGE_MANIFEST_MEDIA_TYPES.has(manifestResource.mediaType)) {
    throw createError('unsupported-manifest', `Unsupported manifest media type: ${manifestResource.mediaType}`);
  }

  const manifestJson = manifestResource.json || {};
  const configSize = Number(manifestJson?.config?.size || 0);
  const layerBytes = Array.isArray(manifestJson?.layers)
    ? manifestJson.layers.reduce((total, layer) => total + Number(layer?.size || 0), 0)
    : 0;

  return {
    sizeBytes: configSize + layerBytes,
    digest: manifestResource.digest,
    platform: selectedPlatform
  };
}

function shouldSkipTimelineCandidateError(error) {
  return [
    'platform-not-found',
    'platform-manifest-missing',
    'image-tag-not-found',
    'image-not-found',
    'image-files-missing',
    'unsupported-manifest'
  ].includes(error?.code);
}

function isTimelineMetadataFallbackError(error) {
  return ['registry-access-denied', 'auth-header-hidden', 'token-cors-failed', 'cors-probe-failed', 'tags-parse-failed'].includes(
    error?.code
  );
}

function isComparableTimelineCandidate(candidate) {
  return Boolean(candidate?.timestamp || candidate?.sortDate || candidate?.semver);
}

function compareTimelineCandidateOrder(left, right) {
  const leftSemver = left?.semver;
  const rightSemver = right?.semver;
  if (leftSemver && rightSemver) {
    return compareTimelineSemver(leftSemver, rightSemver);
  }

  const leftDate = left?.sortDate || left?.timestamp || '';
  const rightDate = right?.sortDate || right?.timestamp || '';
  if (leftDate && rightDate) {
    return leftDate.localeCompare(rightDate);
  }

  return String(left?.tag || '').localeCompare(String(right?.tag || ''), undefined, {
    numeric: true,
    sensitivity: 'base'
  });
}

function compareTimelinePointOrder(left, right) {
  if (left?.timestamp && right?.timestamp) {
    return left.timestamp.localeCompare(right.timestamp);
  }
  return compareTimelineCandidateOrder(left, right);
}

function deriveTimelinePreference(currentTag) {
  const raw = String(currentTag || '').trim().toLowerCase();
  const tokens = raw
    .split(/[^a-z0-9]+/g)
    .map((token) => token.trim())
    .filter(
      (token) =>
        token &&
        !/^\d+$/.test(token) &&
        !['latest', 'stable', 'current', 'edge', 'main', 'master'].includes(token)
    );

  return {
    tokens: [...new Set(tokens)],
    allowPrerelease: tokens.some((token) => /^(?:alpha|beta|rc|preview)/.test(token))
  };
}

function matchesTimelinePreference(tag, tokens) {
  const haystack = ` ${String(tag || '').toLowerCase()} `;
  return tokens.every((token) => haystack.includes(token));
}

function isPlainSemverTimelineTag(tag) {
  const raw = String(tag || '').trim();
  const match = /^v?(\d+)(?:[._-](\d+))(?:[._-](\d+))?(?:[._-](\d+))?$/i.exec(raw);
  if (!match) return false;
  if (parseTimelineDate(raw) && String(match[1] || '').length === 4) return false;
  return true;
}

function isPrereleaseTimelineTag(tag) {
  return /(?:^|[-_.])(alpha|beta|rc|preview)(?:[-_.]?\d*)/i.test(String(tag || ''));
}

function parseTimelineVersionTag(tag) {
  const match = /^v?(\d+)(?:[._-](\d+))?(?:[._-](\d+))?(?:[._-](\d+))?/i.exec(String(tag || '').trim());
  if (!match) return null;
  return [match[1], match[2], match[3], match[4]].map((segment) => Number(segment || 0));
}

function compareTimelineSemver(left, right) {
  const length = Math.max(left.length, right.length);
  for (let index = 0; index < length; index += 1) {
    const diff = Number(left[index] || 0) - Number(right[index] || 0);
    if (diff !== 0) return diff;
  }
  return 0;
}

function parseTimelineDate(tag) {
  const raw = String(tag || '').trim();
  const match = raw.match(/\b(\d{4})[._-](\d{1,2})[._-](\d{1,2})\b/);
  if (!match) return '';
  const year = match[1];
  const month = match[2].padStart(2, '0');
  const day = match[3].padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function formatTimelineLabel(candidate) {
  const tag = String(candidate?.tag || '').trim();
  const timestamp = String(candidate?.timestamp || '').trim();
  if (!timestamp) return tag;
  return `${timestamp.slice(0, 10)} · ${tag}`;
}

function buildTagsListUrl(job, pageSize, last = '') {
  const url = new URL(`${job.registryV2Base}/${job.repository}/tags/list`);
  url.searchParams.set('n', String(pageSize));
  if (last) {
    url.searchParams.set('last', String(last));
  }
  return url.toString();
}

function parseRegistryNextLink(linkHeader, currentUrl) {
  const raw = String(linkHeader || '').trim();
  if (!raw) return '';
  const match = raw.match(/<([^>]+)>/);
  if (!match) return '';
  try {
    return new URL(match[1], currentUrl).toString();
  } catch {
    return '';
  }
}

function buildDockerHubTagsApiUrl(job, page, pageSize) {
  const parts = String(job?.repository || '')
    .split('/')
    .map((segment) => segment.trim())
    .filter(Boolean);
  if (!parts.length) {
    throw createError('invalid-image-reference', 'Repository path is missing.');
  }
  const namespace = parts.length === 1 ? 'library' : parts[0];
  const repository = parts.length === 1 ? parts[0] : parts.slice(1).join('/');
  const url = new URL(
    `https://hub.docker.com/v2/namespaces/${encodeURIComponent(namespace)}/repositories/${repository
      .split('/')
      .map((segment) => encodeURIComponent(segment))
      .join('/')}/tags`
  );
  url.searchParams.set('page', String(page));
  url.searchParams.set('page_size', String(pageSize));
  url.searchParams.set('ordering', 'last_updated');
  return url.toString();
}

function clampNumber(input, min, max, fallback) {
  const value = Number(input);
  if (!Number.isFinite(value)) return fallback;
  return Math.max(min, Math.min(max, Math.round(value)));
}

function assertRunActive(runToken) {
  if (runtime.runToken !== runToken) {
    throw createError('run-superseded', 'A newer job run replaced the current task.');
  }
}

function throwIfStopped() {
  if (runtime.control === 'pause') {
    throw createError('job-paused', 'Paused.');
  }
  if (runtime.control === 'cancel') {
    throw createError('job-cancelled', 'Cancelled.');
  }
}

function assertJob() {
  if (!runtime.job) {
    throw createError('no-job', 'There is no active image job yet.');
  }
  return runtime.job;
}

function normalizeRequiredPlatformValue(value) {
  const normalized = normalizeAnyPlatformValue(value);
  if (!normalized) {
    throw createError('unsupported-platform', 'Select an available architecture for this image.');
  }
  return normalized;
}

function normalizeOptionalPlatformValue(value) {
  return normalizeAnyPlatformValue(value) || DEFAULT_PLATFORM;
}

function normalizeProxyMode(value) {
  const normalized = String(value || '').trim().toLowerCase();
  return ['auth', 'data', 'mixed'].includes(normalized) ? normalized : '';
}

function ensureRegistryClientSupport() {
  if (typeof fetch === 'undefined') {
    throw createError('fetch-unsupported', 'This browser cannot fetch registry data.');
  }
}

function ensureBrowserSupport() {
  ensureRegistryClientSupport();
  if (!navigator?.storage?.getDirectory) {
    throw createError('opfs-unsupported', 'This browser does not support the Origin Private File System APIs required by the exporter.');
  }
  if (typeof indexedDB === 'undefined') {
    throw createError('indexeddb-unsupported', 'This browser does not expose IndexedDB, so resumable browser-only pulls are unavailable.');
  }
  if (typeof DecompressionStream === 'undefined') {
    throw createError('compression-streams-unsupported', 'This browser does not support DecompressionStream, so gzip layer replay is unavailable.');
  }
}

async function buildSnapshot() {
  return {
    job: summarizeJob(runtime.job),
    logs: runtime.logEntries.slice(-40),
    storage: await collectStorageEstimate()
  };
}

function summarizeJob(job) {
  if (!job) return null;
  return structuredCloneSafe({
    ...job,
    totals: {
      ...job.totals,
      entryCount: runtime.entries.size || job.totals.entryCount || 0,
      directoryCount: countEntriesByType('dir') || job.totals.directoryCount || 0,
      fileCount: countEntriesByType('file') || job.totals.fileCount || 0
    }
  });
}

function postJobUpdate() {
  postEvent({
    kind: 'job',
    job: summarizeJob(runtime.job),
    logs: runtime.logEntries.slice(-40)
  });
}

function hydrateEntries(entries) {
  runtime.entries = new Map();
  for (const entry of entries || []) {
    runtime.entries.set(entry.path, entry);
  }
  ensureRootEntry();
}

function normalizeRestoredJob(job) {
  const hasSplitProxyState =
    Object.prototype.hasOwnProperty.call(job || {}, 'proxyAuthActive') ||
    Object.prototype.hasOwnProperty.call(job || {}, 'proxyDataActive');
  const legacyProxyActive = Boolean(job?.proxyActive);
  const proxyAuthActive = hasSplitProxyState ? Boolean(job?.proxyAuthActive) : legacyProxyActive;
  const proxyDataActive = hasSplitProxyState ? Boolean(job?.proxyDataActive) : legacyProxyActive;
  return {
    ...job,
    sourceImageInput: job.sourceImageInput || null,
    storage: job.storage || null,
    proxyBaseUrl: job.proxyBaseUrl || null,
    proxySource: job.proxySource || null,
    proxyAuthActive,
    proxyDataActive,
    proxyActive: Boolean(proxyAuthActive || proxyDataActive),
    skipInitialProbe: Boolean(job.skipInitialProbe),
    hasCredentials: Boolean(job.hasCredentials),
    restorePrompt: Boolean(job.restorePrompt),
    platformOptions: normalizePlatformList(job.platformOptions),
    lastError: job.lastError || null,
    totals: {
      compressedBytes: 0,
      downloadedBytes: 0,
      indexedLayers: 0,
      totalLayers: 0,
      entryCount: 0,
      directoryCount: 1,
      fileCount: 0,
      ...(job.totals || {})
    }
  };
}

function buildBreadcrumbs(path) {
  const breadcrumbs = [{ path: '/', label: '/' }];
  if (path === '/') return breadcrumbs;
  const parts = path.replace(/^\/+/, '').split('/');
  let current = '';
  for (const part of parts) {
    current += `/${part}`;
    breadcrumbs.push({ path: current, label: part });
  }
  return breadcrumbs;
}

async function resolveAvailablePlatforms(job, manifestResource, requestedPlatform, signal) {
  if (IMAGE_INDEX_MEDIA_TYPES.has(manifestResource.mediaType)) {
    return normalizePlatformList(extractSupportedPlatformsFromIndex(manifestResource.json.manifests || []));
  }

  if (IMAGE_MANIFEST_MEDIA_TYPES.has(manifestResource.mediaType)) {
    return normalizePlatformList(await resolveSingleManifestPlatforms(job, manifestResource.json, requestedPlatform, signal));
  }

  return normalizePlatformList([requestedPlatform]);
}

async function resolveSingleManifestPlatforms(job, manifestJson, requestedPlatform, signal) {
  const directPlatform = descriptorPlatformValue({
    os: manifestJson.os || manifestJson.config?.os || manifestJson.platform?.os,
    architecture: manifestJson.architecture || manifestJson.config?.architecture || manifestJson.platform?.architecture
  });
  if (directPlatform) {
    return [directPlatform];
  }

  const configDigest = splitDigest(manifestJson.config?.digest || '');
  if (configDigest) {
    try {
      const configJson = await fetchJsonBlob(job, configDigest.digest, 'The image config could not be read while detecting architectures.', signal);
      const configPlatform = descriptorPlatformValue({
        os: configJson?.os,
        architecture: configJson?.architecture
      });
      if (configPlatform) {
        return [configPlatform];
      }
    } catch (error) {
      if (!job?.ephemeral) {
        throw error;
      }
    }
  }

  return [pickPreferredPlatform([], requestedPlatform)];
}

async function fetchJsonBlob(job, digest, fallbackMessage, signal) {
  const response = await registryFetch(job, buildBlobUrl(job, digest), {
    method: 'GET',
    scope: repositoryPullScope(job.repository),
    signal
  });
  if (!response.ok) {
    await throwResponseError(response, fallbackMessage, {
      kind: 'config',
      job
    });
  }

  let json;
  try {
    json = await response.json();
  } catch {
    throw createError('config-parse-failed', 'The registry returned invalid config JSON while detecting architectures.');
  }
  return json;
}

function extractSupportedPlatformsFromIndex(manifests) {
  const values = [];
  const seen = new Set();
  for (const descriptor of manifests || []) {
    const value = descriptorPlatformValue(descriptor.platform);
    if (!value || seen.has(value)) continue;
    seen.add(value);
    values.push(value);
  }
  return values;
}
