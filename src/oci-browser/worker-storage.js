export function createWorkerStorage({
  runtime,
  createError,
  splitDigest,
  JOB_ID,
  DB_NAME,
  DB_VERSION,
  OPFS_ROOT_NAME
}) {
  async function clearPersistentState() {
    await dbClearJob(JOB_ID);
    runtime.sessionCredentials = null;
    const opfsRoot = await getOpfsRoot();
    try {
      await opfsRoot.removeEntry('jobs', { recursive: true });
    } catch {}
  }

  async function collectStorageEstimate() {
    const estimate = (await navigator.storage.estimate?.()) || {};
    return {
      usage: Number(estimate.usage || 0),
      quota: Number(estimate.quota || 0),
      available: Math.max(0, Number(estimate.quota || 0) - Number(estimate.usage || 0)),
      persisted: await navigator.storage.persisted?.().catch(() => false)
    };
  }

  async function getOpfsRoot() {
    if (!runtime.opfsRootPromise) {
      runtime.opfsRootPromise = (async () => {
        const root = await navigator.storage.getDirectory();
        try {
          return await root.getDirectoryHandle(OPFS_ROOT_NAME, { create: true });
        } catch {
          throw createError('opfs-unavailable', 'Failed to create the OPFS workspace for the image browser.');
        }
      })();
    }
    return runtime.opfsRootPromise;
  }

  async function getDirectoryHandle(baseHandle, segments, create = true) {
    let handle = baseHandle;
    for (const segment of segments) {
      handle = await handle.getDirectoryHandle(segment, { create });
    }
    return handle;
  }

  async function getBlobFileHandle(digest, create) {
    const parts = ['jobs', JOB_ID, 'blobs', 'sha256'];
    const root = await getOpfsRoot();
    const directory = await getDirectoryHandle(root, parts, create);
    return directory.getFileHandle(splitDigest(digest).hex, { create });
  }

  async function getBlobFile(digest) {
    const handle = await getBlobFileHandle(digest, false);
    return handle.getFile();
  }

  async function removeBlob(digest) {
    const root = await getOpfsRoot();
    const directory = await getDirectoryHandle(root, ['jobs', JOB_ID, 'blobs', 'sha256'], false);
    await directory.removeEntry(splitDigest(digest).hex).catch(() => undefined);
  }

  async function getJobFileHandle(pathSegments, create) {
    const root = await getOpfsRoot();
    const segments = ['jobs', JOB_ID, ...pathSegments];
    const directory = await getDirectoryHandle(root, segments.slice(0, -1), create);
    return directory.getFileHandle(segments[segments.length - 1], { create });
  }

  async function openDatabase() {
    if (!runtime.dbPromise) {
      runtime.dbPromise = new Promise((resolve, reject) => {
        const request = indexedDB.open(DB_NAME, DB_VERSION);
        request.onupgradeneeded = () => {
          const db = request.result;
          if (!db.objectStoreNames.contains('jobs')) {
            db.createObjectStore('jobs', { keyPath: 'id' });
          }
          if (!db.objectStoreNames.contains('entries')) {
            const entries = db.createObjectStore('entries', { keyPath: 'path' });
            entries.createIndex('jobId', 'jobId', { unique: false });
          }
        };
        request.onerror = () => reject(request.error || createError('indexeddb-open-failed', 'Failed to open IndexedDB.'));
        request.onsuccess = () => resolve(request.result);
      });
    }
    return runtime.dbPromise;
  }

  async function dbGetJob(id) {
    const db = await openDatabase();
    return withStore(db, 'jobs', 'readonly', (store) => promisifyRequest(store.get(id)));
  }

  async function dbPutJob(job) {
    const db = await openDatabase();
    return withStore(db, 'jobs', 'readwrite', (store) => promisifyRequest(store.put(job)));
  }

  async function dbGetEntries(jobId) {
    const db = await openDatabase();
    return withStore(db, 'entries', 'readonly', async (store) => {
      const index = store.index('jobId');
      return promisifyRequest(index.getAll(jobId));
    });
  }

  async function dbReplaceEntries(jobId, entries) {
    const db = await openDatabase();
    return withTransaction(db, ['entries'], 'readwrite', async (transaction) => {
      const store = transaction.objectStore('entries');
      const existing = await promisifyRequest(store.index('jobId').getAllKeys(jobId));
      for (const key of existing) {
        store.delete(key);
      }
      for (const entry of entries) {
        store.put({
          ...entry,
          jobId
        });
      }
    });
  }

  async function dbClearJob(jobId) {
    const db = await openDatabase();
    return withTransaction(db, ['jobs', 'entries'], 'readwrite', async (transaction) => {
      transaction.objectStore('jobs').delete(jobId);
      const entriesStore = transaction.objectStore('entries');
      const keys = await promisifyRequest(entriesStore.index('jobId').getAllKeys(jobId));
      for (const key of keys) {
        entriesStore.delete(key);
      }
    });
  }

  function withStore(db, storeName, mode, callback) {
    return withTransaction(db, [storeName], mode, (transaction) => callback(transaction.objectStore(storeName)));
  }

  function withTransaction(db, storeNames, mode, callback) {
    return new Promise((resolve, reject) => {
      const transaction = db.transaction(storeNames, mode);
      let result;
      transaction.oncomplete = () => resolve(result);
      transaction.onerror = () => reject(transaction.error || createError('indexeddb-transaction-failed', 'IndexedDB transaction failed.'));
      transaction.onabort = () => reject(transaction.error || createError('indexeddb-transaction-aborted', 'IndexedDB transaction was aborted.'));

      Promise.resolve()
        .then(() => callback(transaction))
        .then((value) => {
          result = value;
        })
        .catch((error) => {
          reject(error);
          transaction.abort();
        });
    });
  }

  function promisifyRequest(request) {
    return new Promise((resolve, reject) => {
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error || createError('indexeddb-request-failed', 'IndexedDB request failed.'));
    });
  }

  async function persistJob() {
    if (!runtime.job) return;
    runtime.job.updatedAt = new Date().toISOString();
    await dbPutJob(runtime.job);
  }

  async function persistJobAndEntries() {
    if (!runtime.job) return;
    runtime.job.updatedAt = new Date().toISOString();
    await dbPutJob(runtime.job);
    await dbReplaceEntries(
      runtime.job.id,
      [...runtime.entries.values()].map((entry) => ({
        ...entry
      }))
    );
  }

  return {
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
  };
}
