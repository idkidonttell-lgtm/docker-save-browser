export function decodePreviewBytes(bytes, { MAX_PREVIEW_BYTES }) {
  const sample = bytes || new Uint8Array(0);
  let controlCount = 0;
  for (const byte of sample) {
    if (byte === 0) {
      return {
        content: 'Binary content preview is not available.',
        isBinary: true,
        truncated: true
      };
    }
    if (byte < 9 || (byte > 13 && byte < 32)) {
      controlCount += 1;
    }
  }
  if (sample.byteLength > 0 && controlCount / sample.byteLength > 0.12) {
    return {
      content: 'Binary content preview is not available.',
      isBinary: true,
      truncated: true
    };
  }
  return {
    content: new TextDecoder().decode(sample),
    isBinary: false,
    truncated: sample.byteLength >= MAX_PREVIEW_BYTES
  };
}

export async function buildDockerArchive(job, signal, deps) {
  const {
    TarWriter,
    getJobFileHandle,
    getBlobFile,
    throwIfStopped,
    createError,
    postProgress,
    DEFAULT_PLATFORM,
    OPFS_ROOT_NAME,
    JOB_ID
  } = deps;

  throwIfStopped();
  const exportHandle = await getJobFileHandle(['export', 'image.tar'], true);
  const exportWritable = await exportHandle.createWritable();
  const tarWriter = new TarWriter(exportWritable);
  const repoTagList = job.repoTag ? [job.repoTag] : [];
  const compatibilityManifest = JSON.stringify([
    {
      Config: `blobs/sha256/${job.config.hex}`,
      RepoTags: repoTagList,
      Layers: job.layers.map((layer) => `blobs/sha256/${layer.hex}`)
    }
  ]);
  const repositories = JSON.stringify(job.repoTag ? { [job.repository]: { [job.tag]: job.config.hex } } : {});
  const indexJson = JSON.stringify({
    schemaVersion: 2,
    mediaType: 'application/vnd.oci.image.index.v1+json',
    manifests: [
      {
        mediaType: job.manifest.mediaType,
        digest: job.manifest.digest,
        size: job.manifest.size,
        annotations: job.repoTag
          ? {
              'org.opencontainers.image.ref.name': job.tag
            }
          : undefined,
        platform: {
          os: job.manifestPlatform?.os || job.selectedPlatform.split('/')[0],
          architecture: job.manifestPlatform?.architecture || job.selectedPlatform.split('/')[1],
          ...(job.manifestPlatform?.variant || job.selectedPlatform.split('/')[2]
            ? { variant: job.manifestPlatform?.variant || job.selectedPlatform.split('/')[2] }
            : {})
        }
      }
    ]
  });
  const ociLayout = JSON.stringify({ imageLayoutVersion: '1.0.0' });

  const smallFiles = [
    { name: 'oci-layout', body: ociLayout },
    { name: 'index.json', body: indexJson },
    { name: 'manifest.json', body: compatibilityManifest },
    { name: 'repositories', body: repositories }
  ];

  const blobList = [job.manifest, job.config, ...job.layers];
  const totalUnits = smallFiles.length + blobList.length;
  let completedUnits = 0;

  for (const file of smallFiles) {
    throwIfStopped();
    if (signal?.aborted) {
      throw createError('job-cancelled', 'Export was cancelled.');
    }
    await tarWriter.writeBytes(file.name, file.body);
    completedUnits += 1;
    postProgress('export', `Wrote ${file.name}.`, completedUnits, totalUnits);
  }

  for (const blob of blobList) {
    throwIfStopped();
    if (signal?.aborted) {
      throw createError('job-cancelled', 'Export was cancelled.');
    }
    const file = await getBlobFile(blob.digest);
    await tarWriter.writeFile(`blobs/sha256/${blob.hex}`, file, {}, () => undefined);
    completedUnits += 1;
    postProgress('export', `Packed blob ${blob.hex.slice(0, 12)}…`, completedUnits, totalUnits);
  }

  await tarWriter.finish();
  const exportedFile = await exportHandle.getFile();
  return {
    opfsPath: `${OPFS_ROOT_NAME}/jobs/${JOB_ID}/export/image.tar`,
    size: exportedFile.size,
    suggestedName: buildSuggestedArchiveName(job, { DEFAULT_PLATFORM })
  };
}

export function buildSuggestedArchiveName(job, { DEFAULT_PLATFORM }) {
  const repoName = job.repository.replace(/[\/:@]+/g, '-');
  const platformPart = String(job.selectedPlatform || DEFAULT_PLATFORM).replace(/[^a-zA-Z0-9._-]+/g, '-');
  const tagPart = job.tag
    ? job.tag.replace(/[^a-zA-Z0-9._-]+/g, '-')
    : platformPart;
  return `${repoName}-${tagPart}-${platformPart}.image.tar`;
}

export async function openLayerContentStream(file, mediaType, deps) {
  const { detectLayerCompression, createError } = deps;
  const compression = detectLayerCompression(mediaType);
  if (compression === 'identity') {
    return file.stream();
  }
  if (compression === 'gzip') {
    return file.stream().pipeThrough(new DecompressionStream('gzip'));
  }
  if (compression === 'zstd') {
    throw createError(
      'zstd-unsupported',
      'This image uses zstd-compressed layers. v1 detects them and fails clearly because no browser-native zstd decoder is bundled yet.'
    );
  }
  throw createError('layer-media-type-unsupported', `Unsupported layer media type: ${mediaType}`);
}

export async function truncateFile(fileHandle) {
  const writable = await fileHandle.createWritable();
  await writable.truncate(0);
  await writable.close();
}
