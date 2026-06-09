import {
  DEFAULT_PLATFORM,
  formatBytes,
  normalizePlatformList,
  normalizePastedDockerPullInput,
  normalizeProxyBaseUrl,
  normalizePlatformValue as normalizeAnyPlatformValue,
  parseImageSourceInput,
  pickPreferredPlatform,
  stripProtocol
} from './shared.js';

const PLATFORM_PROBE_INPUT_DELAY_MS = 900;
const PLATFORM_PROBE_CACHE_TTL_MS = 20_000;
const MAX_PLATFORM_PROBE_CACHE_ENTRIES = 8;

const STORAGE_KEYS = {
  imageInput: 'rf-oci-browser:last-image-input',
  platform: 'rf-oci-browser:last-platform'
};

const FLOW_PROGRESS_WEIGHTS = Object.freeze({
  resolve: 5,
  download: 75,
  index: 15,
  export: 3,
  save: 2
});

const state = {
  worker: null,
  pending: new Map(),
  requestId: 0,
  snapshot: null,
  job: null,
  logs: [],
  progress: null,
  support: null,
  saving: false,
  defaultProxyBaseUrl: '',
  exportFlowActive: false,
  platformProbePending: false,
  platformProbeTimer: null,
  platformProbeRequestToken: 0,
  platformProbeKey: '',
  platformProbeError: '',
  platformProbeCache: new Map(),
  platformProbeProxyActive: false,
  platformProbeProxySource: null,
  platformOptionsResolved: false,
  openPanel: null,
  jobWaiters: [],
  recentExport: null,
  deliveringFallback: false,
  transferRateBps: 0,
  progressSample: null,
  smoothedEtaSeconds: null,
  etaPhase: '',
  selectOpen: false,
  clearingCache: false,
  suppressClearedJobError: false,
  controlPanelHeight: 0,
  copyResetTimers: new WeakMap()
};

const dom = {};

document.addEventListener('DOMContentLoaded', async () => {
  cacheDom();
  renderStaticPlatformOptions();
  restoreRememberedFormState();
  state.support = detectSupport();
  renderSupportStatus();
  bindEvents();

  if (!state.support.ready) {
    renderStatusOnly();
    renderStageMode();
    renderButtonStates();
    return;
  }

  try {
    state.worker = new Worker(new URL('./worker.js', import.meta.url), { type: 'module' });
    state.worker.addEventListener('message', handleWorkerMessage);
  } catch (error) {
    state.support.ready = false;
    state.support.workerApi = false;
    state.support.issues.push(`Worker initialization failed: ${error.message || 'unknown error'}`);
    renderSupportStatus();
    renderStatusOnly();
    renderStageMode();
    renderButtonStates();
    return;
  }

  try {
    const snapshot = await callWorker('bootstrap');
    applySnapshot(snapshot);
    if (state.job) {
      syncFormFromJob();
    } else if (dom.imageUrlInput?.value) {
      schedulePlatformDetection(true);
    }
    renderAll();
  } catch (error) {
    setInlineError(error.message || 'Failed to start the browser worker.');
  }
});

function cacheDom() {
  dom.form = document.querySelector('[data-oci-form]');
  dom.inputSection = document.querySelector('[data-input-section]');
  dom.controlPanel = document.querySelector('[data-control-panel]');
  dom.panelHead = document.querySelector('[data-panel-head]');
  dom.formStage = document.querySelector('[data-form-stage]');
  dom.imageUrlInput = dom.form?.querySelector('[name="imageUrl"]');
  dom.credentialsToggle = document.querySelector('[data-credentials-toggle]');
  dom.credentialsPanel = document.querySelector('[data-credentials-panel]');
  dom.registryUsernameInput = document.querySelector('[name="registryUsername"]');
  dom.registryPasswordInput = document.querySelector('[name="registryPassword"]');
  dom.checkIconTemplate = document.querySelector('[data-check-icon-template]');
  dom.proxyInput = document.querySelector('[name="proxyBaseUrl"]');
  dom.cacheSize = document.querySelector('[data-cache-size]');
  dom.customSelect = document.querySelector('[data-general-select]');
  dom.platformSelect = dom.form?.querySelector('[data-general-select-native]');
  dom.customSelectTrigger = document.querySelector('[data-general-select-trigger]');
  dom.customSelectValue = document.querySelector('[data-general-select-value]');
  dom.customSelectMenu = document.querySelector('[data-general-select-menu]');
  dom.clearButton = document.querySelector('[data-command="clear"]');
  dom.clearLabel = document.querySelector('[data-clear-label]');
  dom.clearLoader = document.querySelector('[data-clear-loader]');
  dom.exportButton = document.querySelector('[data-command="export"]');
  dom.exportLabel = document.querySelector('[data-export-label]');
  dom.exportLoader = document.querySelector('[data-export-loader]');
  dom.exportLoaderValue = document.querySelector('[data-export-loader-value]');
  dom.settingsToggle = document.querySelector('[data-settings-toggle]');
  dom.settingsPanel = document.querySelector('[data-settings-panel]');
  dom.debugToggle = document.querySelector('[data-debug-toggle]');
  dom.debugPanel = document.querySelector('[data-debug-panel]');
  dom.platformNote = document.querySelector('[data-platform-note]');
  dom.imageNote = document.querySelector('[data-image-note]');
  dom.panelTools = document.querySelector('[data-panel-tools]');
  dom.supportStatus = document.querySelector('[data-support-status]');
  dom.supportHint = document.querySelector('[data-support-hint]');
  dom.supportDetails = document.querySelector('[data-support-details]');
  dom.supportList = document.querySelector('[data-support-list]');
  dom.supportPanel = document.querySelector('[data-support-panel]');
  dom.contextNote = document.querySelector('[data-context-note]');
  dom.contextNoteTitle = document.querySelector('[data-context-note-title]');
  dom.contextNoteTrigger = document.querySelector('[data-context-note-trigger]');
  dom.contextNoteTooltip = document.querySelector('[data-context-note-tooltip]');
  dom.contextNoteBody = document.querySelector('[data-context-note-body]');
  dom.logList = document.querySelector('[data-log-list]');
  dom.logEmpty = document.querySelector('[data-log-empty]');
  dom.copyButtons = Array.from(document.querySelectorAll('[data-copy-text]'));
  state.defaultProxyBaseUrl = readDefaultProxyBaseUrl();
}

function detectSupport() {
  const support = {
    secureContext: window.isSecureContext === true,
    workerApi: typeof Worker !== 'undefined',
    opfs: Boolean(navigator?.storage?.getDirectory),
    indexedDb: typeof indexedDB !== 'undefined',
    compressionStreams: typeof DecompressionStream !== 'undefined'
  };

  support.issues = [];
  if (!support.secureContext) support.issues.push('secure context (open this on localhost or HTTPS)');
  if (!support.workerApi) support.issues.push('Web Workers');
  if (!support.opfs) support.issues.push('Origin Private File System');
  if (!support.indexedDb) support.issues.push('IndexedDB');
  if (!support.compressionStreams) support.issues.push('Compression Streams');
  support.ready = support.secureContext && support.workerApi && support.opfs && support.indexedDb && support.compressionStreams;
  return support;
}

function renderStaticPlatformOptions() {
  renderPlatformPlaceholder();
}

function renderPlatformOptions(values, preferredPlatform) {
  if (!dom.platformSelect) return;
  const options = normalizePlatformOptions(values, { allowEmpty: true });
  if (!options.length) {
    renderPlatformPlaceholder();
    return;
  }
  const currentValue = String(dom.platformSelect.value || '').trim().toLowerCase();
  const selectedValue = options.includes(currentValue) ? currentValue : choosePreferredPlatform(options, preferredPlatform);
  dom.platformSelect.innerHTML = options.map((value) => `<option value="${value}">${value}</option>`).join('');
  dom.platformSelect.value = selectedValue;
  state.platformOptionsResolved = true;
  renderCustomSelectOptions(options, selectedValue);
}

function renderPlatformPlaceholder(label = 'Choose architecture') {
  if (!dom.platformSelect) return;
  dom.platformSelect.innerHTML = '';
  state.platformOptionsResolved = false;
  renderCustomSelectOptions([], '');
  updateCustomSelectValue(label);
}

function normalizePlatformOptions(values, { allowEmpty = false } = {}) {
  return normalizePlatformList(values, { allowEmpty });
}

function choosePreferredPlatform(options, requestedPlatform) {
  return pickPreferredPlatform(options, requestedPlatform, DEFAULT_PLATFORM);
}

function renderCustomSelectOptions(options, selectedValue) {
  if (!dom.customSelectMenu) return;
  const checkIcon = dom.checkIconTemplate?.innerHTML || '';
  dom.customSelectMenu.innerHTML = options
    .map((value) => {
      const isSelected = value === selectedValue;
      return `<div
        class="rf-general-select__option${isSelected ? ' is-selected' : ''}"
        role="option"
        tabindex="0"
        aria-selected="${String(isSelected)}"
        data-select-option
        data-value="${escapeAttribute(value)}"
      >
        <span class="rf-general-select__option-label">${escapeHtml(value)}</span>
        <span class="rf-general-select__option-spacer"></span>
        <span class="rf-general-select__option-check" aria-hidden="true">${checkIcon}</span>
      </div>`;
    })
    .join('');
  updateCustomSelectValue();
}

function updateCustomSelectValue(placeholder = '') {
  if (!dom.customSelectValue || !dom.platformSelect) return;
  const currentValue = String(dom.platformSelect.value || '').trim().toLowerCase();
  const hasValue = Boolean(currentValue);
  const fallbackPlaceholder = placeholder || (state.platformProbePending ? 'Loading architectures' : 'Choose architecture');
  dom.customSelectValue.textContent = hasValue ? currentValue : fallbackPlaceholder;
  dom.customSelectValue.classList.toggle('is-placeholder', !hasValue);
  const optionButtons = dom.customSelectMenu?.querySelectorAll('[data-select-option]') || [];
  for (const button of optionButtons) {
    const selected = String(button.dataset.value || '') === currentValue;
    button.classList.toggle('is-selected', selected);
    button.setAttribute('aria-selected', String(selected));
  }
}

function toggleCustomSelect() {
  if (dom.customSelectTrigger?.disabled) return;
  setCustomSelectOpen(!state.selectOpen);
}

function setCustomSelectOpen(isOpen) {
  state.selectOpen = Boolean(isOpen);
  if (dom.customSelect) {
    dom.customSelect.classList.toggle('is-open', state.selectOpen);
  }
  if (dom.customSelectMenu) {
    dom.customSelectMenu.hidden = !state.selectOpen;
  }
  if (dom.customSelectTrigger) {
    dom.customSelectTrigger.setAttribute('aria-expanded', String(state.selectOpen));
  }
}

function handleCustomSelectMenuKeydown(event) {
  if (event.key === 'Escape') {
    setCustomSelectOpen(false);
    dom.customSelectTrigger?.focus();
    return;
  }

  const optionButton = event.target.closest('[data-select-option]');
  if (!optionButton || !dom.platformSelect) return;

  if (event.key === 'Enter' || event.key === ' ') {
    event.preventDefault();
    const value = String(optionButton.dataset.value || '').trim().toLowerCase();
    if (!value) return;
    dom.platformSelect.value = value;
    dom.platformSelect.dispatchEvent(new Event('change', { bubbles: true }));
    setCustomSelectOpen(false);
    dom.customSelectTrigger?.focus();
  }
}

function syncInputControlState(input) {
  const control = input?.closest('.rf-general-input__control');
  if (!control) return;
  control.classList.toggle('is-filled', Boolean(String(input.value || '').trim()));
}

function setInputErrorState(input, isError) {
  const control = input?.closest('.rf-general-input__control');
  if (!control) return;
  control.classList.toggle('is-error', Boolean(isError));
}

function syncAllInputControlStates() {
  syncInputControlState(dom.imageUrlInput);
  syncInputControlState(dom.registryUsernameInput);
  syncInputControlState(dom.registryPasswordInput);
  syncInputControlState(dom.proxyInput);
}

function readRegistryCredentials() {
  const username = String(dom.registryUsernameInput?.value || '').trim();
  const password = String(dom.registryPasswordInput?.value || '');
  if (!username && !password) {
    return null;
  }
  return { username, password };
}

function hasRegistryCredentials() {
  return Boolean(readRegistryCredentials());
}

function buildCredentialCacheKey(credentials) {
  if (!credentials) return '';
  return `${credentials.username || ''}\u0000${credentials.password || ''}`;
}

function restoreRememberedFormState() {
  try {
    // Clean up any older remembered form state. This tool no longer restores
    // the last typed image or platform from localStorage.
    window.localStorage.removeItem(STORAGE_KEYS.imageInput);
    window.localStorage.removeItem(STORAGE_KEYS.platform);
  } catch {
    return;
  }

  syncAllInputControlStates();
  updateCustomSelectValue();
}

function rememberFormState({ imageUrl = dom.imageUrlInput?.value || '', platform = dom.platformSelect?.value || DEFAULT_PLATFORM } = {}) {
  return;
}

function clearRememberedFormState() {
  try {
    window.localStorage.removeItem(STORAGE_KEYS.imageInput);
    window.localStorage.removeItem(STORAGE_KEYS.platform);
  } catch {
    return;
  }
}

function resetClearedUiState({ preserveImageInput = false } = {}) {
  if (dom.imageUrlInput && !preserveImageInput) {
    dom.imageUrlInput.value = '';
  }
  state.platformProbeCache.clear();
  state.platformProbeProxyActive = false;
  state.platformProbeProxySource = null;
  renderStaticPlatformOptions();
  updateCustomSelectValue();
  syncAllInputControlStates();
  state.platformProbePending = false;
  state.platformProbeError = '';
  state.platformProbeKey = '';
  state.platformOptionsResolved = false;
  state.progress = null;
  state.transferRateBps = 0;
  state.progressSample = null;
  state.smoothedEtaSeconds = null;
  state.etaPhase = '';
  state.logs = [];
}

function bindEvents() {
  dom.imageUrlInput?.addEventListener('input', (event) => {
    const input = event.currentTarget;
    syncInputControlState(input);
    rememberFormState();
    handleImageInputChanged();
  });
  dom.imageUrlInput?.addEventListener('paste', (event) => {
    const pastedText = event.clipboardData?.getData('text');
    if (!pastedText) return;
    const normalized = normalizePastedDockerPullInput(pastedText);
    if (!normalized || normalized === pastedText.trim()) return;
    event.preventDefault();
    if (dom.imageUrlInput) {
      dom.imageUrlInput.value = normalized;
      syncInputControlState(dom.imageUrlInput);
    }
    rememberFormState();
    handleImageInputChanged();
  });
  dom.imageUrlInput?.addEventListener('change', () => {
    rememberFormState();
    schedulePlatformDetection(true);
  });
  dom.platformSelect?.addEventListener('change', () => {
    rememberFormState();
    updateCustomSelectValue();
    renderPlatformNote();
  });
  dom.clearButton?.addEventListener('click', handleClearCache);
  dom.exportButton?.addEventListener('click', handlePrimaryAction);
  dom.credentialsToggle?.addEventListener('click', (event) => {
    event.stopPropagation();
    togglePanel('credentials');
  });
  dom.credentialsPanel?.addEventListener('click', (event) => event.stopPropagation());
  dom.registryUsernameInput?.addEventListener('input', (event) => {
    syncInputControlState(event.currentTarget);
    clearRecentExportFeedback({ render: false });
    renderFieldNotes();
    renderPanels();
    schedulePlatformDetection(false);
  });
  dom.registryPasswordInput?.addEventListener('input', (event) => {
    syncInputControlState(event.currentTarget);
    clearRecentExportFeedback({ render: false });
    renderFieldNotes();
    renderPanels();
    schedulePlatformDetection(false);
  });
  dom.registryUsernameInput?.addEventListener('change', () => schedulePlatformDetection(true));
  dom.registryPasswordInput?.addEventListener('change', () => schedulePlatformDetection(true));
  dom.proxyInput?.addEventListener('input', (event) => {
    syncInputControlState(event.currentTarget);
    renderContextNote();
  });
  dom.proxyInput?.addEventListener('change', () => schedulePlatformDetection(true));
  dom.settingsToggle?.addEventListener('click', (event) => {
    event.stopPropagation();
    togglePanel('settings');
  });
  dom.debugToggle?.addEventListener('click', (event) => {
    event.stopPropagation();
    togglePanel('debug');
  });
  dom.customSelectTrigger?.addEventListener('click', (event) => {
    event.stopPropagation();
    toggleCustomSelect();
  });
  dom.customSelectTrigger?.addEventListener('keydown', (event) => {
    if (event.key === 'ArrowDown' || event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      event.stopPropagation();
      setCustomSelectOpen(true);
      dom.customSelectMenu?.querySelector('[data-select-option]')?.focus();
    } else if (event.key === 'Escape') {
      setCustomSelectOpen(false);
    }
  });
  dom.customSelectMenu?.addEventListener('click', (event) => {
    event.stopPropagation();
    const optionButton = event.target.closest('[data-select-option]');
    if (!optionButton || !dom.platformSelect) return;
    const value = String(optionButton.dataset.value || '').trim().toLowerCase();
    if (!value) return;
    dom.platformSelect.value = value;
    dom.platformSelect.dispatchEvent(new Event('change', { bubbles: true }));
    setCustomSelectOpen(false);
  });
  dom.customSelectMenu?.addEventListener('keydown', handleCustomSelectMenuKeydown);
  dom.settingsPanel?.addEventListener('click', (event) => event.stopPropagation());
  dom.debugPanel?.addEventListener('click', (event) => event.stopPropagation());
  for (const button of dom.copyButtons || []) {
    button.addEventListener('click', handleCopyButtonClick);
  }
  document.addEventListener('click', () => {
    closeOpenPanel();
    setCustomSelectOpen(false);
  });
  document.addEventListener('keydown', handleDocumentKeydown);
  window.addEventListener('resize', handleViewportPanelChange, { passive: true });
  window.addEventListener('scroll', handleViewportPanelChange, true);
  syncAllInputControlStates();
}

async function handleCopyButtonClick(event) {
  const button = event.currentTarget;
  const text = String(button?.dataset.copyText || '');
  if (!text) return;

  try {
    await navigator.clipboard.writeText(text);
    setCopyButtonState(button, 'Copied');
  } catch {
    setCopyButtonState(button, 'Failed');
  }
}

function handlePrimaryAction(event) {
  if (state.recentExport?.pendingDelivery) {
    return handleDeliverFallbackExport();
  }
  return handleExportFlow(event);
}

function setCopyButtonState(button, label) {
  const labelNode = button?.querySelector('[data-copy-label]');
  if (!button || !labelNode) return;

  labelNode.textContent = label;
  const existingTimer = state.copyResetTimers.get(button);
  if (existingTimer) {
    window.clearTimeout(existingTimer);
  }

  const resetTimer = window.setTimeout(() => {
    labelNode.textContent = 'Copy';
    state.copyResetTimers.delete(button);
  }, 1600);

  state.copyResetTimers.set(button, resetTimer);
}

function handleImageInputChanged() {
  clearRecentExportFeedback({ render: false });
  setCustomSelectOpen(false);
  state.platformProbeRequestToken += 1;
  state.platformProbePending = false;
  state.platformProbeError = '';
  state.platformProbeKey = '';
  state.platformProbeProxyActive = false;
  state.platformProbeProxySource = null;
  renderPlatformPlaceholder();
  schedulePlatformDetection(false);
  renderFieldNotes();
  renderContextNote();
  renderStatusOnly();
  renderButtonStates();
}

async function handleClearCache() {
  if (state.clearingCache || state.exportFlowActive || state.saving) {
    return;
  }

  clearInlineError();
  clearRecentExportFeedback({ render: false });
  state.clearingCache = true;
  renderAll();

  try {
    clearRememberedFormState();
    if (state.worker) {
      const snapshot = await callWorker('clearJob');
      applySnapshot(snapshot);
    } else {
      state.job = null;
      state.snapshot = null;
    }
    resetClearedUiState();
  } catch (error) {
    setInlineError(error.message || 'Failed to clear local browser data.');
  } finally {
    state.clearingCache = false;
    renderAll();
    if (state.worker && String(dom.imageUrlInput?.value || '').trim()) {
      schedulePlatformDetection(true);
    }
  }
}

async function handleCancelCurrentJob() {
  if (!state.worker || state.clearingCache) {
    return;
  }

  clearInlineError();
  clearRecentExportFeedback({ render: false });
  setCustomSelectOpen(false);
  state.suppressClearedJobError = true;

  try {
    const snapshot = await callWorker('clearJob');
    applySnapshot(snapshot);
    resetClearedUiState({ preserveImageInput: true });
    renderAll();
    if (state.worker && String(dom.imageUrlInput?.value || '').trim()) {
      schedulePlatformDetection(true);
    }
  } catch (error) {
    state.suppressClearedJobError = false;
    setInlineError(error.message || 'Failed to cancel the current download.');
  }
}

async function handleResumeRestoredJob() {
  if (!state.worker || !state.job || state.exportFlowActive || state.saving || state.clearingCache) {
    return;
  }

  clearInlineError();
  clearRecentExportFeedback({ render: false });
  setCustomSelectOpen(false);

  try {
    const snapshot = await callWorker('resumeJob', {
      credentials: readRegistryCredentials()
    });
    applySnapshot(snapshot);
    syncFormFromJob();
    renderAll();
  } catch (error) {
    setInlineError(error.message || 'Failed to resume the restored download.');
  }
}

async function handleIgnoreRestoredJob() {
  if (!state.worker || !state.job || state.exportFlowActive || state.saving || state.clearingCache) {
    return;
  }

  clearInlineError();
  clearRecentExportFeedback({ render: false });
  setCustomSelectOpen(false);
  state.clearingCache = true;
  renderAll();

  try {
    const snapshot = await callWorker('clearJob');
    applySnapshot(snapshot);
    resetClearedUiState({ preserveImageInput: true });
  } catch (error) {
    setInlineError(error.message || 'Failed to clear the restored download.');
  } finally {
    state.clearingCache = false;
    renderAll();
    if (state.worker && String(dom.imageUrlInput?.value || '').trim()) {
      schedulePlatformDetection(true);
    }
  }
}

function schedulePlatformDetection(immediate) {
  if (state.exportFlowActive) {
    return;
  }

  if (state.platformProbeTimer) {
    window.clearTimeout(state.platformProbeTimer);
    state.platformProbeTimer = null;
  }

  const delay = immediate ? 0 : PLATFORM_PROBE_INPUT_DELAY_MS;
  state.platformProbeTimer = window.setTimeout(() => {
    state.platformProbeTimer = null;
    detectPlatformOptions().catch(() => undefined);
  }, delay);
}

async function handleExportFlow(event) {
  event.preventDefault();
  clearInlineError();
  clearRecentExportFeedback({ render: false });
  setCustomSelectOpen(false);

  if (!state.worker) {
    setInlineError(getUnsupportedMessage());
    return;
  }
  if (state.exportFlowActive) {
    return;
  }

  let parsedImage;
  try {
    parsedImage = parseImageSourceInput(dom.imageUrlInput?.value || '');
  } catch (error) {
    setInlineError(error.message || 'The image input is invalid.');
    return;
  }

  const provisionalRequest = {
    registryBaseUrl: parsedImage.registryBaseUrl,
    imageRef: parsedImage.imageRef,
    platform: dom.platformSelect?.value || DEFAULT_PLATFORM
  };

  const exportTarget = determineExportTarget(
    state.job?.exportInfo && jobMatchesRequest(state.job, provisionalRequest)
      ? state.job.exportInfo.suggestedName
      : buildSuggestedArchiveName(provisionalRequest)
  );

  let request;
  try {
    request = await buildRequestedImagePayload({ forcePlatformRefresh: true });
  } catch (error) {
    setInlineError(error.message || 'The image input is invalid.');
    return;
  }

  if (hasConflictingRunningJob(request)) {
    setInlineError('Another image is already being prepared. Cancel it from settings or wait for it to finish.');
    return;
  }

  try {
    state.exportFlowActive = true;
    state.saving = true;
    renderButtonStates();

    if (shouldStartFreshJob(request)) {
      renderAll();

      const snapshot = await callWorker('startJob', request);
      applySnapshot(snapshot);
      renderAll();
    } else if (shouldResumeCurrentJob(request)) {
      const snapshot = await callWorker('resumeJob', {
        credentials: request.credentials || null
      });
      applySnapshot(snapshot);
      renderAll();
    }

    await waitForJobReady();

    const exportInfo =
      state.job?.status === 'ready' && state.job?.exportInfo && jobMatchesRequest(state.job, request)
        ? state.job.exportInfo
        : await callWorker('exportArchive');
    if (state.job) {
      state.job.exportInfo = exportInfo;
    }
    if (exportTarget.type === 'browser-open') {
      markRecentExport(exportInfo, request, {
        delivery: null,
        pendingDelivery: 'open',
        opfsPath: exportInfo.opfsPath
      });
      renderAll();
      return;
    }
    rememberFormState({
      imageUrl: request.sourceImageInput,
      platform: request.platform
    });
    markRecentExport(exportInfo, request, {
      delivery: null,
      opfsPath: exportInfo.opfsPath
    });
    state.exportFlowActive = false;
    state.saving = false;
    renderAll();
    await waitForUiCompletionFrame();
    await deliverExportThroughBrowser(exportInfo, {
      suggestedName: exportTarget.suggestedName,
      mode: 'download'
    });
    markRecentExport(exportInfo, request, {
      delivery: 'downloaded',
      opfsPath: exportInfo.opfsPath
    });
    await clearWorkerCacheAfterSave();
    renderAll();
  } catch (error) {
    if (error?.name !== 'AbortError' && !shouldSuppressClearedJobError(error)) {
      setInlineError(error.message || 'Failed to export the archive.');
    }
  } finally {
    state.exportFlowActive = false;
    state.saving = false;
    state.suppressClearedJobError = false;
    renderAll();
  }
}

function determineExportTarget(suggestedName) {
  return {
    type: isAppleMobileBrowser() ? 'browser-open' : 'browser-download',
    suggestedName: suggestedName || 'image.tar'
  };
}

function buildSuggestedArchiveName(request) {
  const imageRef = String(request?.imageRef || '').trim();
  const repositoryPart = imageRef.includes('@') ? imageRef.slice(0, imageRef.indexOf('@')) : imageRef.split(':')[0];
  const repoName = String(repositoryPart || 'image').replace(/[\/:@]+/g, '-');
  const hasDigest = imageRef.includes('@');
  const platformPart = String(request?.platform || DEFAULT_PLATFORM).replace(/[^a-zA-Z0-9._-]+/g, '-');
  const tagPart = hasDigest
    ? platformPart
    : String(imageRef.split(':').slice(1).join(':') || 'latest').replace(/[^a-zA-Z0-9._-]+/g, '-');
  return `${repoName}-${tagPart}-${platformPart}.image.tar`;
}

async function buildRequestedImagePayload({ forcePlatformRefresh = false } = {}) {
  const parsedImage = parseImageSourceInput(dom.imageUrlInput?.value || '');
  const { proxyBaseUrl, proxySource } = resolveRequestedProxyConfig();
  const credentials = readRegistryCredentials();
  const platformInfo = await detectPlatformOptions({
    force: forcePlatformRefresh,
    requested: parsedImage,
    proxyBaseUrl,
    proxySource,
    credentials
  });

  const platform = choosePreferredPlatform(
    platformInfo?.platformOptions || [dom.platformSelect?.value || DEFAULT_PLATFORM],
    dom.platformSelect?.value || platformInfo?.defaultPlatform || DEFAULT_PLATFORM
  );

  renderPlatformOptions(platformInfo?.platformOptions || [platform], platform);
    renderFieldNotes();

  return {
    registryBaseUrl: parsedImage.registryBaseUrl,
    proxyBaseUrl,
    proxySource,
    proxyActive: Boolean(platformInfo?.proxyActive),
    proxyMode: platformInfo?.proxyMode || null,
    credentials,
    imageRef: parsedImage.imageRef,
    platform,
    platformOptions: platformInfo?.platformOptions || [platform],
    sourceImageInput: parsedImage.sourceImageInput,
    skipInitialProbe: true
  };
}

async function detectPlatformOptions({ force = false, requested = null, proxyBaseUrl = null, proxySource = null, credentials = undefined } = {}) {
  if (!state.worker) return null;
  if (state.exportFlowActive && !force) return null;

  const rawImageInput = String(requested?.sourceImageInput || dom.imageUrlInput?.value || '').trim();

  let parsedImage;
  try {
    parsedImage = requested || parseImageSourceInput(dom.imageUrlInput?.value || '');
  } catch (error) {
    state.platformProbePending = false;
    state.platformProbeError = rawImageInput ? String(error?.message || 'Enter an image like alpine:latest.') : '';
    state.platformProbeKey = '';
    state.platformProbeProxyActive = false;
    state.platformProbeProxySource = null;
    state.platformOptionsResolved = false;
    renderStaticPlatformOptions();
    renderFieldNotes();
    renderContextNote();
    renderStatusOnly();
    renderButtonStates();
    return null;
  }

  const proxyConfig = proxyBaseUrl === null && proxySource === null ? resolveRequestedProxyConfig() : { proxyBaseUrl, proxySource };
  const resolvedCredentials = credentials === undefined ? readRegistryCredentials() : credentials;
  const probeKey = [
    parsedImage.registryBaseUrl,
    parsedImage.imageRef,
    proxyConfig.proxyBaseUrl || '',
    proxyConfig.proxySource || '',
    buildCredentialCacheKey(resolvedCredentials)
  ].join('|');
  const requestToken = ++state.platformProbeRequestToken;

  const cachedResult = getCachedPlatformProbe(probeKey);
  if (cachedResult) {
    state.platformProbeKey = probeKey;
    state.platformProbeError = '';
    state.platformProbePending = false;
    state.platformProbeProxyActive = Boolean(cachedResult.proxyActive);
    state.platformProbeProxySource = cachedResult.proxySource || null;
    renderPlatformOptions(cachedResult.platformOptions, cachedResult.defaultPlatform);
    renderFieldNotes();
    renderContextNote();
    renderStatusOnly();
    renderButtonStates();
    renderProgress();
    return cachedResult;
  }

  if (!force && state.platformProbeKey === probeKey && !state.platformProbeError) {
    return {
      platformOptions: normalizePlatformOptions(Array.from(dom.platformSelect?.options || []).map((option) => option.value)),
      defaultPlatform: dom.platformSelect?.value || DEFAULT_PLATFORM
    };
  }

  state.platformProbePending = true;
  state.platformOptionsResolved = false;
  renderPlatformPlaceholder('Loading architectures');
  renderFieldNotes();
  renderButtonStates();
  renderProgress();

  try {
    const result = await callWorker('resolvePlatforms', {
      registryBaseUrl: parsedImage.registryBaseUrl,
      proxyBaseUrl: proxyConfig.proxyBaseUrl,
      proxySource: proxyConfig.proxySource,
      credentials: resolvedCredentials,
      imageRef: parsedImage.imageRef,
      platform: normalizeAnyPlatformValue(dom.platformSelect?.value) || DEFAULT_PLATFORM
    });

    if (requestToken !== state.platformProbeRequestToken) {
      return null;
    }

    state.platformProbeKey = probeKey;
    state.platformProbeError = '';
    state.platformProbeProxyActive = Boolean(result.proxyActive);
    state.platformProbeProxySource = result.proxySource || null;
    cachePlatformProbeResult(probeKey, result);
    renderPlatformOptions(result.platformOptions, result.defaultPlatform);
    renderFieldNotes();
    renderContextNote();
    renderStatusOnly();
    return result;
  } catch (error) {
    if (requestToken !== state.platformProbeRequestToken || error?.code === 'platform-probe-aborted') {
      return null;
    }
    state.platformProbeKey = '';
    state.platformProbeError = formatPlatformProbeError(error, parsedImage);
    state.platformProbeProxyActive = false;
    state.platformProbeProxySource = null;
    state.platformOptionsResolved = false;
    renderStaticPlatformOptions();
    renderFieldNotes();
    renderContextNote();
    renderStatusOnly();
    if (force) {
      throw error;
    }
    return null;
  } finally {
    if (requestToken === state.platformProbeRequestToken) {
      state.platformProbePending = false;
      renderFieldNotes();
      renderButtonStates();
      renderProgress();
    }
  }
}

function getCachedPlatformProbe(probeKey) {
  if (!probeKey) return null;
  const cached = state.platformProbeCache.get(probeKey);
  if (!cached) return null;
  if (cached.expiresAt <= Date.now()) {
    state.platformProbeCache.delete(probeKey);
    return null;
  }
  return cached.result;
}

function cachePlatformProbeResult(probeKey, result) {
  if (!probeKey || !result) return;
  pruneExpiredPlatformProbeCache();
  const platformOptions = normalizePlatformOptions(result.platformOptions || []);
  const defaultPlatform = choosePreferredPlatform(platformOptions, result.defaultPlatform || DEFAULT_PLATFORM);
  state.platformProbeCache.delete(probeKey);
  state.platformProbeCache.set(probeKey, {
    expiresAt: Date.now() + PLATFORM_PROBE_CACHE_TTL_MS,
    result: {
      platformOptions,
      defaultPlatform,
      proxyActive: Boolean(result.proxyActive),
      proxySource: result.proxySource || null
    }
  });

  while (state.platformProbeCache.size > MAX_PLATFORM_PROBE_CACHE_ENTRIES) {
    const oldestKey = state.platformProbeCache.keys().next().value;
    if (!oldestKey) break;
    state.platformProbeCache.delete(oldestKey);
  }
}

function pruneExpiredPlatformProbeCache() {
  const now = Date.now();
  for (const [key, value] of state.platformProbeCache.entries()) {
    if (value.expiresAt <= now) {
      state.platformProbeCache.delete(key);
    }
  }
}

function formatPlatformProbeError(error, parsedImage) {
  const imageLabel = String(parsedImage?.sourceImageInput || parsedImage?.displayName || parsedImage?.imageRef || 'that image').trim();
  const hasCredentials = hasRegistryCredentials();

  if (error?.code === 'image-tag-not-found' || error?.code === 'image-not-found' || error?.code === 'image-digest-not-found') {
    return `Couldn't find ${imageLabel}. Check the image name and tag and try again.`;
  }

  if (error?.code === 'dockerhub-image-unavailable') {
    return hasCredentials
      ? `Couldn't access ${imageLabel} on Docker Hub. Check the image name or credentials and try again.`
      : `Couldn't find ${imageLabel}, or it is private on Docker Hub. Check the image name or add credentials.`;
  }

  if (error?.code === 'registry-access-denied') {
    return hasCredentials
      ? 'The registry rejected these credentials, or this image is still not available for browser download.'
      : 'This image may be private. Add registry credentials if you have access.';
  }

  if (error?.code === 'platform-manifest-missing') {
    return 'This image does not have a manifest for that architecture. Try another architecture or tag.';
  }

  return error?.message || 'Could not load architecture options yet.';
}

function shouldStartFreshJob(request) {
  if (!state.job) return true;
  if (jobMatchesRequest(state.job, request)) {
    return false;
  }
  return !['probing', 'resolving', 'downloading', 'indexing', 'exporting'].includes(state.job.status);
}

function hasConflictingRunningJob(request) {
  if (!state.job || jobMatchesRequest(state.job, request)) return false;
  return ['probing', 'resolving', 'downloading', 'indexing', 'exporting'].includes(state.job.status);
}

function shouldResumeCurrentJob(request) {
  if (!state.job || !jobMatchesRequest(state.job, request)) return false;
  return ['paused', 'error', 'cancelled'].includes(state.job.status);
}

function jobMatchesRequest(job, request) {
  if (!job || !request) return false;
  return (
    String(job.registryBaseUrl || '') === String(request.registryBaseUrl || '') &&
    String(job.imageRef || '') === String(request.imageRef || '') &&
    String(job.selectedPlatform || '') === String(request.platform || '')
  );
}

function waitForJobReady() {
  if (!state.job) {
    return Promise.reject(new Error('No image job is active.'));
  }
  if (state.job.status === 'ready') {
    return Promise.resolve(state.job);
  }
  if (['error', 'cancelled', 'paused'].includes(state.job.status)) {
    return Promise.reject(new Error(state.job.statusMessage || 'The image job stopped before export was ready.'));
  }

  return new Promise((resolve, reject) => {
    state.jobWaiters.push({ resolve, reject });
  });
}

function shouldSuppressClearedJobError(error) {
  if (!state.suppressClearedJobError) return false;
  const message = String(error?.message || '').trim();
  return (
    message === 'The current image job was cleared.' ||
    message === 'No image job is active.' ||
    message === 'No image job is active'
  );
}

function flushJobWaiters() {
  if (!state.jobWaiters.length) return;
  if (!state.job) {
    const waiters = state.jobWaiters.splice(0);
    for (const waiter of waiters) {
      waiter.reject(new Error('The current image job was cleared.'));
    }
    return;
  }

  if (state.job.status === 'ready') {
    const waiters = state.jobWaiters.splice(0);
    for (const waiter of waiters) {
      waiter.resolve(state.job);
    }
    return;
  }

  if (['error', 'cancelled', 'paused'].includes(state.job.status)) {
    const waiters = state.jobWaiters.splice(0);
    const error = new Error(state.job.statusMessage || 'The image job stopped before export completed.');
    for (const waiter of waiters) {
      waiter.reject(error);
    }
  }
}

function renderPlatformNote() {
  if (!dom.platformNote) return;
  dom.platformNote.hidden = true;
  dom.platformNote.textContent = '';
  dom.platformNote.classList.remove('is-error');
}

function renderImageNote() {
  if (!dom.imageNote) return;

  const hasInput = Boolean(String(dom.imageUrlInput?.value || '').trim());
  const validationError = hasInput && !state.platformProbePending ? String(state.platformProbeError || '').trim() : '';
  const inlineError = validationError || (hasInput ? getFieldRowErrorMessage() : '');

  dom.imageNote.hidden = !inlineError;
  dom.imageNote.textContent = inlineError;
  dom.imageNote.classList.toggle('is-error', Boolean(inlineError));
  setInputErrorState(dom.imageUrlInput, Boolean(validationError));
}

function getRestorePromptModel() {
  if (!state.job || state.job.status !== 'paused' || !state.job.restorePrompt) {
    return null;
  }

  const imageLabel = String(state.job.sourceImageInput || '').trim();
  if (!imageLabel) {
    return null;
  }

  return {
    text:
      state.job.hasCredentials && !hasRegistryCredentials()
        ? `Resume downloading ${imageLabel}. Re-enter registry credentials first, or ignore it and clear the local cache.`
        : `Resume downloading ${imageLabel}, or ignore it and clear the local cache.`
  };
}

function renderRestoreNote() {
  return;
}

function renderFieldNotes() {
  renderImageNote();
  renderPlatformNote();
  renderRestoreNote();
}

async function deliverExportThroughBrowser(exportInfo, { suggestedName, mode = 'download' } = {}) {
  const file = await readOpfsFile(exportInfo.opfsPath);
  const objectUrl = URL.createObjectURL(file);

  try {
    if (mode === 'open') {
      const popup = window.open(objectUrl, '_blank', 'noopener');
      if (!popup) {
        window.location.assign(objectUrl);
      }
      return;
    }

    const anchor = document.createElement('a');
    anchor.href = objectUrl;
    anchor.download = suggestedName || exportInfo.suggestedName || 'image.tar';
    anchor.rel = 'noopener';
    anchor.style.display = 'none';
    document.body.append(anchor);
    anchor.click();
    anchor.remove();
  } finally {
    window.setTimeout(() => URL.revokeObjectURL(objectUrl), 60_000);
  }
}

function waitForUiCompletionFrame(delayMs = 900) {
  return new Promise((resolve) => {
    window.requestAnimationFrame(() => {
      window.requestAnimationFrame(() => {
        window.setTimeout(resolve, delayMs);
      });
    });
  });
}

async function handleDeliverFallbackExport() {
  const exportInfo = state.recentExport;
  if (!exportInfo?.pendingDelivery || state.deliveringFallback) {
    return;
  }

  try {
    clearInlineError();
    state.deliveringFallback = true;
    renderButtonStates();
    await deliverExportThroughBrowser(exportInfo, {
      suggestedName: exportInfo.suggestedName,
      mode: exportInfo.pendingDelivery
    });
    state.recentExport = {
      ...state.recentExport,
      pendingDelivery: null,
      delivery: exportInfo.pendingDelivery === 'open' ? 'opened' : 'downloaded'
    };
    await clearWorkerCacheAfterSave();
    renderAll();
  } catch (error) {
    setInlineError(error.message || 'Could not open the archive in this browser.');
  } finally {
    state.deliveringFallback = false;
    renderButtonStates();
  }
}

async function readOpfsFile(opfsPath) {
  const root = await navigator.storage.getDirectory();
  const segments = String(opfsPath || '')
    .split('/')
    .filter(Boolean);
  let current = root;
  for (let index = 0; index < segments.length - 1; index += 1) {
    current = await current.getDirectoryHandle(segments[index], { create: false });
  }
  const fileHandle = await current.getFileHandle(segments[segments.length - 1], { create: false });
  return fileHandle.getFile();
}

async function clearWorkerCacheAfterSave() {
  if (!state.worker) return;
  try {
    const snapshot = await callWorker('clearJob');
    applySnapshot(snapshot);
  } catch {
    return;
  }
}

async function callAndRefresh(command, payload) {
  if (!state.worker) return null;
  try {
    const snapshot = await callWorker(command, payload);
    applySnapshot(snapshot);
    renderAll();
    return snapshot;
  } catch (error) {
    setInlineError(error.message || `Failed to ${command}.`);
    return null;
  }
}

function handleWorkerMessage(event) {
  const data = event.data || {};
  if (data.replyTo) {
    const pending = state.pending.get(data.replyTo);
    if (!pending) return;
    state.pending.delete(data.replyTo);
    if (data.ok) {
      pending.resolve(data.result);
    } else {
      pending.reject(data.error || new Error('Unknown worker error.'));
    }
    return;
  }

  if (data.type !== 'event') return;
  handleWorkerEvent(data.event);
}

function handleWorkerEvent(event) {
  if (!event) return;

  if (event.kind === 'snapshot') {
    applySnapshot(event.snapshot);
  } else if (event.kind === 'job') {
    state.job = sanitizeJob(event.job);
    state.logs = event.logs || state.logs;
  } else if (event.kind === 'progress') {
    updateTransferRate(event.progress);
    state.progress = event.progress;
  } else if (event.kind === 'log') {
    state.logs = [...state.logs, event.entry].slice(-40);
  }

  flushJobWaiters();
  renderAll();
}

function applySnapshot(snapshot) {
  state.snapshot = snapshot;
  state.job = sanitizeJob(snapshot?.job || null);
  state.logs = snapshot?.logs || [];
  if (!state.job) {
    state.platformProbeKey = '';
    state.transferRateBps = 0;
    state.progressSample = null;
  }
  flushJobWaiters();
}

function callWorker(type, payload = {}) {
  return new Promise((resolve, reject) => {
    const id = `job-${Date.now()}-${++state.requestId}`;
    state.pending.set(id, {
      resolve,
      reject
    });
    state.worker.postMessage({ id, type, payload });
  }).catch((error) => {
    if (error?.message) {
      throw error;
    }
    throw new Error(error?.message || error?.details || 'Unknown worker error.');
  });
}

function renderAll() {
  syncAllInputControlStates();
  updateCustomSelectValue();
  renderCommandExample();
  renderStatusOnly();
  renderContextNote();
  renderFieldNotes();
  renderCacheSummary();
  renderProgress();
  renderPanels();
  renderLogs();
  renderButtonStates();
  renderControlPanelHeight();
}

function renderStatusOnly() {
  return;
}

function getFieldRowErrorMessage() {
  const explainCorsProxy = shouldExplainCorsProxyRequirement();
  const validationError = String(state.platformProbeError || '').trim();
  if (validationError && !state.platformProbePending) {
    return explainCorsProxy ? `${validationError} Add a proxy in Settings and try again.` : validationError;
  }

  if (state.progress?.phase === 'error') {
    const message = String(state.progress.message || '').trim();
    if (isHiddenRecoverableMessage(message)) return '';
    return explainCorsProxy ? `${message} Add a proxy in Settings and try again.` : message;
  }

  if (state.job?.status === 'error') {
    const message = String(state.job.statusMessage || '').trim();
    if (isHiddenRecoverableMessage(message)) return '';
    return explainCorsProxy ? `${message} Add a proxy in Settings and try again.` : message;
  }

  return '';
}

function isHiddenRecoverableMessage(message) {
  const value = String(message || '').trim();
  if (!value) return false;
  return (
    value.includes('does not support HTTP Range') ||
    value.includes('returned a full 200 response instead of 206') ||
    value.startsWith('Restarting layer ') ||
    value.startsWith('Restarting config ')
  );
}

function renderCommandExample() {}

function getCommandExampleArchiveName() {
  const recentName = String(state.recentExport?.suggestedName || '').trim();
  if (recentName) return recentName;

  const exportInfoName = String(state.job?.exportInfo?.suggestedName || '').trim();
  if (exportInfoName) return exportInfoName;

  const sourceInput = String(dom.imageUrlInput?.value || '').trim();
  if (!sourceInput) return 'image.tar';

  try {
    const parsedImage = parseImageSourceInput(sourceInput);
    return buildSuggestedArchiveName({
      imageRef: parsedImage.imageRef,
      platform: dom.platformSelect?.value || DEFAULT_PLATFORM
    });
  } catch {
    return 'image.tar';
  }
}

function sanitizeJob(job) {
  if (!job) return null;
  if (job.status === 'ready' || isHiddenStatusMessage(job.statusMessage)) {
    return {
      ...job,
      statusMessage: ''
    };
  }
  return job;
}

function isHiddenStatusMessage(message) {
  const value = String(message || '').trim();
  return value === 'Export archive is ready to save.' || value === 'Image is ready to browse and export.';
}

function renderSupportStatus() {
  if (!state.support) return;
  const diagnostics = getSupportDiagnostics();
  if (dom.supportPanel) {
    dom.supportPanel.hidden = diagnostics.level === 'ready';
  }
  if (dom.panelTools) {
    dom.panelTools.hidden = !state.support.ready;
  }
  dom.supportStatus.textContent = diagnostics.status;
  dom.supportStatus.dataset.support = diagnostics.level;
  if (dom.supportHint) {
    dom.supportHint.textContent = diagnostics.hint;
  }
  if (dom.supportDetails && dom.supportList) {
    const showDetails = diagnostics.details.length > 0;
    dom.supportDetails.hidden = !showDetails;
    if (!showDetails) {
      dom.supportDetails.open = false;
    }
    dom.supportList.innerHTML = showDetails
      ? diagnostics.details
          .map(
            (detail) =>
              `<li><strong>${escapeHtml(detail.label)}:</strong> ${escapeHtml(detail.value)}${detail.note ? ` ${escapeHtml(detail.note)}` : ''}</li>`
          )
          .join('')
      : '';
  }
}

function renderCacheSummary() {
  if (!dom.cacheSize) return;
  dom.cacheSize.textContent = formatBytes(getCurrentCacheBytes(state.job));
}

function renderControlPanelHeight() {
  const panel = dom.controlPanel;
  if (!panel) return;

  const previousMinHeight = panel.style.minHeight;
  panel.style.minHeight = '0px';
  const targetHeight = Math.ceil(panel.scrollHeight);
  panel.style.minHeight = previousMinHeight;

  if (!(targetHeight > 0)) return;
  if (!state.controlPanelHeight) {
    state.controlPanelHeight = targetHeight;
    panel.style.minHeight = `${targetHeight}px`;
    return;
  }
  if (Math.abs(targetHeight - state.controlPanelHeight) < 2) {
    return;
  }
  state.controlPanelHeight = targetHeight;
  panel.style.minHeight = `${targetHeight}px`;
}

function renderProgress() {
  renderExportLoader();
}

function renderContextNote() {
  if (!dom.contextNote || !dom.contextNoteBody) return;

  const note = getContextualNote();
  if (!note) {
    dom.contextNote.hidden = true;
    dom.contextNoteBody.textContent = '';
    if (dom.contextNoteTrigger) dom.contextNoteTrigger.hidden = true;
    if (dom.contextNoteTooltip) dom.contextNoteTooltip.hidden = true;
    return;
  }

  const title = String(note.title || '').trim();
  const body = String(note.body || '').trim();
  const hasTooltip = Boolean(body);

  dom.contextNote.hidden = false;
  if (dom.contextNoteTitle) {
    dom.contextNoteTitle.textContent = title;
  }
  dom.contextNoteBody.textContent = body;
  if (dom.contextNoteTrigger) {
    dom.contextNoteTrigger.hidden = !hasTooltip;
  }
  if (dom.contextNoteTooltip) {
    dom.contextNoteTooltip.hidden = !hasTooltip;
  }
}

function clampPercent(value) {
  return Math.max(0, Math.min(100, Number.isFinite(value) ? value : 0));
}

function getPhaseRatio(completed, total) {
  if (!(total > 0)) return 0;
  return Math.max(0, Math.min(1, completed / total));
}

function getNormalizedFlowPhase(progress, job) {
  const phase = String(progress?.phase || job?.phase || '').trim().toLowerCase();

  if (['queued', 'probing', 'resolving', 'resolve'].includes(phase)) return 'resolve';
  if (['downloading', 'download'].includes(phase)) return 'download';
  if (['indexing', 'index'].includes(phase)) return 'index';
  if (['exporting', 'export'].includes(phase)) return 'export';
  if (['saving', 'save'].includes(phase)) return 'save';
  if (['ready', 'saved'].includes(phase)) return 'done';
  if (phase === 'error') return 'error';
  return 'resolve';
}

function getDownloadStageProgress(progress, job) {
  const phase = String(progress?.phase || '').trim();
  const progressItemCount = Number(progress?.itemCount || 0);
  const progressItemIndex = Number(progress?.itemIndex || 0);
  const progressItemTotal = Number(progress?.itemTotal || 0);
  const progressItemCompleted = Number(progress?.itemCompleted || 0);

  if (phase === 'download' && progressItemCount > 0 && progressItemIndex > 0) {
    const fileFraction =
      progressItemTotal > 0 ? Math.max(0, Math.min(1, progressItemCompleted / progressItemTotal)) : 1;
    const completedUnits = Math.max(0, Math.min(progressItemCount, (progressItemIndex - 1) + fileFraction));

    return {
      completedBytes: Math.max(Number(job?.totals?.downloadedBytes || 0), Number(progress?.completed || 0)),
      totalBytes: Math.max(Number(job?.totals?.compressedBytes || 0), Number(progress?.total || 0)),
      percent: clampPercent((completedUnits / progressItemCount) * 100)
    };
  }

  const trackedBlobs = [job?.config, ...(job?.layers || [])].filter(Boolean);
  const totalFiles = trackedBlobs.length;

  if (totalFiles > 0) {
    const completedUnits = trackedBlobs.reduce((sum, blob) => {
      if (blob.completed) {
        return sum + 1;
      }
      if (blob.size > 0 && blob.downloadedBytes > 0) {
        return sum + Math.max(0, Math.min(1, Number(blob.downloadedBytes) / Number(blob.size)));
      }
      return sum;
    }, 0);

    return {
      completedBytes: Number(job?.totals?.downloadedBytes || 0),
      totalBytes: Number(job?.totals?.compressedBytes || 0),
      percent: clampPercent((completedUnits / totalFiles) * 100)
    };
  }

  const totalBytes = Math.max(Number(job?.totals?.compressedBytes || 0), phase === 'download' ? Number(progress?.total || 0) : 0);
  const completedBytes = Math.max(
    Number(job?.totals?.downloadedBytes || 0),
    phase === 'download' ? Number(progress?.completed || 0) : 0
  );

  if (totalBytes > 0) {
    return {
      completedBytes,
      totalBytes,
      percent: clampPercent((completedBytes / totalBytes) * 100)
    };
  }

  return {
    completedBytes: 0,
    totalBytes: 0,
    percent: 0
  };
}

function getIndexStageProgress(progress, job) {
  const totalLayers = Math.max(Number(job?.totals?.totalLayers || 0), Number(progress?.total || 0));
  if (totalLayers <= 0) {
    return { completedUnits: 0, totalUnits: 0, percent: 100 };
  }

  const completedUnits = Math.max(
    Number(job?.totals?.indexedLayers || 0),
    Number(progress?.completed || 0)
  );

  return {
    completedUnits,
    totalUnits: totalLayers,
    percent: clampPercent(getPhaseRatio(completedUnits, totalLayers) * 100)
  };
}

function getDownloadFileProgress(progress, job) {
  const trackedBlobs = [job?.config, ...(job?.layers || [])].filter(Boolean);
  const totalFiles = Math.max(trackedBlobs.length, Number(progress?.itemCount || 0));
  const completedFiles = trackedBlobs.reduce((sum, blob) => sum + (blob?.completed ? 1 : 0), 0);
  const currentIndex = Math.max(Number(progress?.itemIndex || 0), completedFiles > 0 ? Math.min(totalFiles, completedFiles + 1) : 0);

  return {
    currentIndex: totalFiles > 0 ? Math.min(Math.max(currentIndex, 1), totalFiles) : 0,
    completedFiles: Math.min(completedFiles, totalFiles),
    totalFiles
  };
}

function formatEta(seconds) {
  const safeSeconds = Number(seconds);
  if (!(safeSeconds > 0) || !Number.isFinite(safeSeconds)) return '';
  if (safeSeconds < 90) {
    const rounded = Math.max(5, Math.round(safeSeconds / 5) * 5);
    return `${rounded}s`;
  }
  if (safeSeconds < 3600) {
    return `${Math.max(1, Math.round(safeSeconds / 60))} min`;
  }
  const hours = Math.floor(safeSeconds / 3600);
  const minutes = Math.round((safeSeconds % 3600) / 60);
  if (minutes <= 0) return `${hours} hr`;
  return `${hours} hr ${minutes} min`;
}

function buildDownloadRateLine(progress, job, downloadProgress) {
  const parts = [];
  if (downloadProgress.totalBytes > 0) {
    parts.push(`${formatBytes(downloadProgress.completedBytes)} of ${formatBytes(downloadProgress.totalBytes)}`);
  }
  if (state.transferRateBps > 0) {
    parts.push(`${formatBytes(state.transferRateBps)}/s`);
  }
  const fileProgress = getDownloadFileProgress(progress, job);
  let filePart = '';
  if (fileProgress.totalFiles > 0) {
    filePart = `File ${fileProgress.currentIndex} of ${fileProgress.totalFiles}`;
  }
  const eta = formatEta(state.smoothedEtaSeconds);
  if (eta) {
    parts.push(`ETA ${eta}`);
  }
  const base = parts.join(' · ');
  if (base && filePart) {
    return `${base} ${filePart}`;
  }
  return filePart || base;
}

function getFlowPhaseWeightStart(phase) {
  switch (phase) {
    case 'download':
      return FLOW_PROGRESS_WEIGHTS.resolve;
    case 'index':
      return FLOW_PROGRESS_WEIGHTS.resolve + FLOW_PROGRESS_WEIGHTS.download;
    case 'export':
      return FLOW_PROGRESS_WEIGHTS.resolve + FLOW_PROGRESS_WEIGHTS.download + FLOW_PROGRESS_WEIGHTS.index;
    case 'save':
      return (
        FLOW_PROGRESS_WEIGHTS.resolve +
        FLOW_PROGRESS_WEIGHTS.download +
        FLOW_PROGRESS_WEIGHTS.index +
        FLOW_PROGRESS_WEIGHTS.export
      );
    default:
      return 0;
  }
}

function getOverallProcessProgress(progress, job) {
  const phase = getNormalizedFlowPhase(progress, job);

  if (phase === 'done') {
    return { percent: 100, determinate: true };
  }

  if (phase === 'error') {
    return { percent: 0, determinate: false };
  }

  const phaseStart = getFlowPhaseWeightStart(phase);
  const phaseWeight = FLOW_PROGRESS_WEIGHTS[phase] || 0;

  let fraction = 0;
  let determinate = false;

  if (phase === 'resolve') {
    fraction = getPhaseRatio(Number(progress?.completed || 0), Number(progress?.total || 0));
    determinate = Number(progress?.total || 0) > 0;
  } else if (phase === 'download') {
    const stage = getDownloadStageProgress(progress, job);
    fraction = stage.percent / 100;
    determinate = true;
  } else if (phase === 'index') {
    const stage = getIndexStageProgress(progress, job);
    fraction = stage.percent / 100;
    determinate = true;
  } else if (phase === 'export' || phase === 'save') {
    fraction = getPhaseRatio(Number(progress?.completed || 0), Number(progress?.total || 0));
    determinate = Number(progress?.total || 0) > 0;
  }

  return {
    percent: clampPercent(phaseStart + (phaseWeight * fraction)),
    determinate
  };
}

function renderPanels() {
  const isSettingsOpen = state.openPanel === 'settings';
  const isDebugOpen = state.openPanel === 'debug';
  const isCredentialsOpen = state.openPanel === 'credentials';

  if (dom.settingsPanel) {
    dom.settingsPanel.hidden = !isSettingsOpen;
  }
  if (dom.debugPanel) {
    dom.debugPanel.hidden = !isDebugOpen;
  }
  if (dom.credentialsPanel) {
    dom.credentialsPanel.hidden = !isCredentialsOpen;
  }
  if (dom.settingsToggle) {
    dom.settingsToggle.setAttribute('aria-expanded', String(isSettingsOpen));
  }
  if (dom.debugToggle) {
    dom.debugToggle.setAttribute('aria-expanded', String(isDebugOpen));
  }
  if (dom.credentialsToggle) {
    dom.credentialsToggle.setAttribute('aria-expanded', String(isCredentialsOpen));
    dom.credentialsToggle.dataset.hasCredentials = hasRegistryCredentials() ? 'true' : 'false';
  }
}

function renderLogs() {
  if (dom.debugToggle) {
    dom.debugToggle.dataset.hasLogs = state.logs.length ? 'true' : 'false';
  }
  if (!dom.logList || !dom.logEmpty) return;

  if (!state.logs.length) {
    dom.logEmpty.hidden = false;
    dom.logList.innerHTML = '';
    return;
  }

  dom.logEmpty.hidden = true;
  dom.logList.innerHTML = state.logs
    .slice(-18)
    .reverse()
    .map(
      (entry) => `<li class="rf-oci-browser__log rf-oci-browser__log--${escapeAttribute(entry.level || 'info')}">
        <span class="rf-oci-browser__log-time">${escapeHtml(shortTime(entry.timestamp))}</span>
        <span class="rf-oci-browser__log-message">${escapeHtml(entry.message || '')}</span>
      </li>`
    )
    .join('');
}

function renderButtonStates() {
  const job = state.job;
  const running = ['probing', 'resolving', 'downloading', 'indexing', 'exporting'].includes(job?.status);
  const showComplete = Boolean(state.recentExport) && !state.exportFlowActive && !state.saving && !running;
  const showDeliver = Boolean(state.recentExport?.pendingDelivery) && showComplete;
  const hasImageInput = Boolean(String(dom.imageUrlInput?.value || '').trim());
  const platformReady = Boolean(dom.platformSelect?.value) && state.platformOptionsResolved;
  const selectBusy = state.platformProbePending || state.exportFlowActive || state.saving || running;
  const selectDisabled = !hasImageInput || selectBusy || !platformReady;

  if (dom.customSelect) {
    dom.customSelect.classList.toggle('is-loading', state.platformProbePending);
    dom.customSelect.classList.toggle('is-disabled', selectDisabled);
  }
  if (dom.customSelectTrigger) {
    dom.customSelectTrigger.disabled = selectDisabled;
  }
  if (dom.customSelectValue) {
    if (state.platformProbePending) {
      dom.customSelectValue.textContent = 'Loading…';
    } else if (!hasImageInput) {
      dom.customSelectValue.textContent = 'Choose architecture';
    }
  }

  if (dom.exportButton) {
    const primaryLabel = showDeliver
      ? state.recentExport?.pendingDelivery === 'open'
        ? 'Open to save'
        : 'Download image'
      : showComplete
        ? 'Finished'
        : 'Download image';
    if (dom.exportLabel) {
      dom.exportLabel.textContent = primaryLabel;
    } else {
      dom.exportButton.textContent = primaryLabel;
    }
    dom.exportButton.classList.toggle('is-loading', state.exportFlowActive || state.saving || running);
    dom.exportButton.classList.toggle('is-complete', showComplete);
    dom.exportButton.hidden = false;
    dom.exportButton.disabled =
      !state.support?.ready ||
      state.saving ||
      state.platformProbePending ||
      !platformReady ||
      state.clearingCache ||
      !hasImageInput;
  }

  if (dom.clearButton) {
    dom.clearButton.classList.toggle('is-loading', state.clearingCache);
    dom.clearButton.disabled = state.saving || state.exportFlowActive || running || state.clearingCache;
  }
  if (dom.clearLabel) {
    dom.clearLabel.textContent = 'Clear';
  }
  if (dom.clearLoader) {
    dom.clearLoader.hidden = !state.clearingCache;
  }
}

function syncFormFromJob() {
  if (!state.job) return;
  if (dom.imageUrlInput) {
    dom.imageUrlInput.value = state.job.sourceImageInput || `${stripProtocol(state.job.registryBaseUrl)}/${state.job.imageRef}`;
  }
  if (dom.proxyInput) {
    dom.proxyInput.value = state.job.proxySource === 'custom' ? state.job.proxyBaseUrl || '' : '';
  }
  renderPlatformOptions(state.job.platformOptions || [], state.job.selectedPlatform || dom.platformSelect?.value || DEFAULT_PLATFORM);
  state.platformProbeError = '';
  state.platformProbeKey = [
    state.job.registryBaseUrl,
    state.job.imageRef,
    state.job.proxyBaseUrl || '',
    state.job.proxySource || '',
    buildCredentialCacheKey(readRegistryCredentials())
  ].join('|');
  state.platformOptionsResolved = Boolean(dom.platformSelect?.value);
  syncAllInputControlStates();
  rememberFormState({
    imageUrl: dom.imageUrlInput?.value || '',
    platform: dom.platformSelect?.value || DEFAULT_PLATFORM
  });
}

function setInlineError(message) {
  state.transferRateBps = 0;
  state.progressSample = null;
  state.progress = {
    phase: 'error',
    message,
    completed: 0,
    total: 1
  };
  renderProgress();
  renderStatusOnly();
  renderFieldNotes();
  renderButtonStates();
}

function clearInlineError() {
  if (state.job?.status === 'error') return;
  if (state.progress?.phase === 'error') {
    state.progress = null;
  }
}

function humanizeStatus(status) {
  switch (status) {
    case 'saved':
      return 'Saved';
    case 'queued':
      return 'Queued';
    case 'probing':
      return 'Probing';
    case 'resolving':
      return 'Resolving';
    case 'downloading':
      return 'Downloading';
    case 'indexing':
      return 'Indexing';
    case 'ready':
      return 'Ready';
    case 'paused':
      return 'Paused';
    case 'cancelled':
      return 'Cancelled';
    case 'exporting':
      return 'Exporting';
    case 'error':
      return 'Error';
    default:
      return 'Idle';
  }
}

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function escapeAttribute(value) {
  return escapeHtml(value).replace(/"/g, '&quot;');
}

function shortTime(value) {
  if (!value) return '--:--';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '--:--';
  return new Intl.DateTimeFormat('en-US', {
    hour: '2-digit',
    minute: '2-digit'
  }).format(date);
}

function getUnsupportedMessage() {
  const diagnostics = getSupportDiagnostics();
  return diagnostics.hint;
}

function getSupportDiagnostics() {
  const support = state.support || detectSupport();
  const currentOrigin = window.location.origin || 'unknown origin';
  const localOrigins = getPreferredLocalOrigins();
  const currentHost = window.location.hostname || '';

  let level = 'ready';
  let status = '';
  let hint = '';

  if (!support.ready) {
    level = 'blocked';
    if (!support.secureContext) {
      status = 'Blocked: insecure page';
      hint = `This page is not running in a secure context. Reopen it on ${localOrigins.join(' or ')} or over HTTPS. Current origin: ${currentOrigin}.`;
    } else {
      status = 'Blocked: required APIs missing';
      hint = `This browser is missing the APIs needed to pull and index OCI images: ${support.issues.join(', ')}.`;
    }
  }

  const details = [
    {
      label: 'Secure context',
      value: support.secureContext ? 'Yes' : 'No',
      note: support.secureContext ? '' : `Try ${localOrigins.join(' or ')} instead of ${currentHost || 'this host'}.`
    },
    {
      label: 'Web Workers',
      value: support.workerApi ? 'Available' : 'Missing'
    },
    {
      label: 'OPFS storage',
      value: support.opfs ? 'Available' : 'Missing',
      note: support.opfs ? '' : 'Without OPFS the tool cannot store layers safely in the browser.'
    },
    {
      label: 'IndexedDB',
      value: support.indexedDb ? 'Available' : 'Missing'
    },
    {
      label: 'Compression Streams',
      value: support.compressionStreams ? 'Available' : 'Missing'
    }
  ];

  return {
    level,
    status,
    hint,
    details: level === 'ready' ? [] : details
  };
}

function getPreferredLocalOrigins() {
  const port = window.location.port ? `:${window.location.port}` : '';
  return [`http://localhost${port}`, `http://127.0.0.1${port}`];
}

function shouldExplainCorsProxyRequirement() {
  const lastErrorCode = state.job?.lastError?.code || '';
  if (!['cors-probe-failed', 'auth-header-hidden', 'token-cors-failed'].includes(lastErrorCode)) {
    return false;
  }
  return !String(dom.proxyInput?.value || '').trim();
}

function getContextualNote() {
  if (state.job?.proxyActive) {
    const usingCustomProxy = state.job.proxySource === 'custom';
    return {
      title: usingCustomProxy ? 'Using custom proxy for this registry' : 'Using proxy for this registry',
      body: usingCustomProxy
        ? 'This registry blocks part of the browser flow, so some requests for this image are going through your custom proxy.'
        : 'This registry blocks part of the browser flow, so some requests for this image are going through the default proxy.'
    };
  }

  if (state.platformProbeProxyActive) {
    const usingCustomProxy = state.platformProbeProxySource === 'custom';
    return {
      title: usingCustomProxy ? 'Using custom proxy for this registry' : 'Using proxy for this registry',
      body: usingCustomProxy
        ? 'This registry blocks part of the browser flow, so some requests for this image are going through your custom proxy.'
        : 'This registry blocks part of the browser flow, so some requests for this image are going through the default proxy.'
    };
  }

  const lastErrorCode = state.job?.lastError?.code || '';
  if (['cors-probe-failed', 'auth-header-hidden', 'token-cors-failed'].includes(lastErrorCode)) {
    return {
      title: 'Direct registry access blocked',
      body: 'This registry does not allow direct browser access. Add or fix a proxy in settings to continue.'
    };
  }

  return null;
}

function getRecentExportMessage(recentExport) {
  if (!recentExport) return '';
  if (recentExport.pendingDelivery === 'download') {
    return `Archive ready. Click “Download image” for ${recentExport.suggestedName}.`;
  }
  if (recentExport.pendingDelivery === 'open') {
    return `Archive ready. Click “Open to save” for ${recentExport.suggestedName}.`;
  }
  if (recentExport.delivery === 'opened') {
    return `Opened ${recentExport.suggestedName} in Safari so you can save it.`;
  }
  if (recentExport.delivery === 'downloaded') {
    return `Downloading ${recentExport.suggestedName}.`;
  }
  return `${recentExport.suggestedName} is ready.`;
}

function isAppleMobileBrowser() {
  const userAgent = String(navigator.userAgent || '');
  const platform = String(navigator.platform || '');
  return /iPhone|iPad|iPod/i.test(userAgent) || (platform === 'MacIntel' && Number(navigator.maxTouchPoints || 0) > 1);
}

function readDefaultProxyBaseUrl() {
  const configured = String(dom.form?.dataset.defaultProxyUrl || '').trim();
  if (!configured) return '';
  try {
    return normalizeProxyBaseUrl(configured);
  } catch {
    return '';
  }
}

function togglePanel(panelName) {
  state.openPanel = state.openPanel === panelName ? null : panelName;
  renderPanels();
}

function closeOpenPanel() {
  if (!state.openPanel) return;
  state.openPanel = null;
  renderPanels();
}

function handleDocumentKeydown(event) {
  if (event.key !== 'Escape') return;
  if (state.selectOpen) {
    setCustomSelectOpen(false);
  }
  if (state.openPanel) {
    closeOpenPanel();
  }
}

function handleViewportPanelChange() {
  return;
}

function updateFloatingPanelPosition() {
  return;
}

function getCurrentCacheBytes(job) {
  if (!job) return 0;
  return Number(job.totals?.downloadedBytes || 0) + Number(job.exportInfo?.size || 0);
}

function renderExportLoader() {
  if (!dom.exportLoader || !dom.exportLoaderValue) return;

  const job = state.job;
  const running = ['probing', 'resolving', 'downloading', 'indexing', 'exporting'].includes(job?.status);
  const showComplete = Boolean(state.recentExport) && !state.exportFlowActive && !state.saving && !running;
  const isActive =
    state.exportFlowActive ||
    state.saving ||
    running ||
    showComplete;

  dom.exportLoader.hidden = !isActive;
  if (!isActive) {
    dom.exportLoaderValue.textContent = '0%';
    return;
  }

  if (showComplete) {
    dom.exportLoaderValue.textContent = '100%';
    return;
  }

  const progress = state.progress;
  const overallProgress = getOverallProcessProgress(progress, state.job);
  const hasPercent = overallProgress.determinate;
  const percent = overallProgress.percent;

  if (hasPercent) {
    dom.exportLoaderValue.textContent = `${Math.round(percent)}%`;
  } else {
    dom.exportLoaderValue.textContent = '0%';
  }
}

function markRecentExport(exportInfo, request = null, options = {}) {
  clearRecentExportFeedback({ render: false });
  const hasDelivery = Object.prototype.hasOwnProperty.call(options, 'delivery');
  const hasPendingDelivery = Object.prototype.hasOwnProperty.call(options, 'pendingDelivery');
  state.recentExport = {
    suggestedName: exportInfo?.suggestedName || 'image.tar',
    size: Number(exportInfo?.size || 0),
    opfsPath: String(options.opfsPath || exportInfo?.opfsPath || ''),
    image: String(request?.sourceImageInput || dom.imageUrlInput?.value || '').trim(),
    platform: String(request?.platform || dom.platformSelect?.value || DEFAULT_PLATFORM).trim(),
    delivery: hasDelivery ? options.delivery : 'saved',
    pendingDelivery: hasPendingDelivery ? options.pendingDelivery : null,
    savedAt: Date.now()
  };
  state.transferRateBps = 0;
  state.progressSample = null;
}

function clearRecentExportFeedback({ render = true } = {}) {
  if (!state.recentExport) {
    if (render) {
      renderAll();
    }
    return;
  }
  state.recentExport = null;
  if (render) {
    renderAll();
  }
}

function updateTransferRate(progress) {
  const phase = String(progress?.phase || '').trim();
  const completed = Number(progress?.completed || 0);
  const now = performance.now();

  if (!['download', 'save'].includes(phase)) {
    state.transferRateBps = 0;
    state.smoothedEtaSeconds = null;
    state.etaPhase = phase;
    state.progressSample = phase ? { phase, completed, timestamp: now } : null;
    return;
  }

  const previous = state.progressSample;
  if (!previous || previous.phase !== phase || completed < previous.completed) {
    state.transferRateBps = 0;
    state.smoothedEtaSeconds = null;
    state.etaPhase = phase;
    state.progressSample = { phase, completed, timestamp: now };
    return;
  }

  const elapsedSeconds = (now - previous.timestamp) / 1000;
  const deltaBytes = completed - previous.completed;

  if (elapsedSeconds <= 0) {
    return;
  }

  if (deltaBytes > 0 && elapsedSeconds >= 0.18) {
    const instantRate = deltaBytes / elapsedSeconds;
    state.transferRateBps = state.transferRateBps ? state.transferRateBps * 0.65 + instantRate * 0.35 : instantRate;
    state.progressSample = { phase, completed, timestamp: now };
    updateSmoothedEta(progress, phase);
    return;
  }

  if (elapsedSeconds >= 1.4) {
    state.progressSample = { phase, completed, timestamp: now };
  }
  updateSmoothedEta(progress, phase);
}

function updateSmoothedEta(progress, phase) {
  if (phase !== 'download') {
    state.smoothedEtaSeconds = null;
    state.etaPhase = phase;
    return;
  }

  const stage = getDownloadStageProgress(progress, state.job);
  const remainingBytes = Math.max(0, Number(stage.totalBytes || 0) - Number(stage.completedBytes || 0));

  if (!(remainingBytes > 0) || !(state.transferRateBps > 0)) {
    if (!(remainingBytes > 0)) {
      state.smoothedEtaSeconds = null;
      state.etaPhase = phase;
    }
    return;
  }

  const rawEtaSeconds = remainingBytes / state.transferRateBps;
  if (state.etaPhase !== phase || !Number.isFinite(state.smoothedEtaSeconds)) {
    state.smoothedEtaSeconds = rawEtaSeconds;
    state.etaPhase = phase;
    return;
  }

  state.smoothedEtaSeconds = state.smoothedEtaSeconds * 0.8 + rawEtaSeconds * 0.2;
  state.etaPhase = phase;
}

function resolveRequestedProxyConfig() {
  const rawCustomProxyBaseUrl = String(dom.proxyInput?.value || '').trim();
  if (rawCustomProxyBaseUrl) {
    const customProxyBaseUrl = normalizeProxyBaseUrl(rawCustomProxyBaseUrl);
    return {
      proxyBaseUrl: customProxyBaseUrl,
      proxySource: customProxyBaseUrl ? 'custom' : null
    };
  }

  return {
    proxyBaseUrl: state.defaultProxyBaseUrl,
    proxySource: state.defaultProxyBaseUrl ? 'default' : null
  };
}
