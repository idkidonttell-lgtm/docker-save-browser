# Docker Save Browser

Small browser app for exporting a public Docker or OCI image as a `docker save` tar.

Docker Save Browser lets you paste a public image reference, pick an architecture, and download a `docker save`-compatible tar directly from the browser. The app resolves image metadata, downloads the layers, and assembles the final archive client-side.

The hard part is registry access from the browser. Many registries do not allow the cross-origin requests needed for this flow. If a registry does allow browser access, you can talk to it directly. If it does not, you need a small proxy in front of it. You could also loosen CORS on your own registry, or disable browser policy/CORS protections in local settings, but that is usually not recommended.

Live Demo: [repoflow.io/tools/docker-save](https://www.repoflow.io/tools/docker-save)

## What's in this repo

- `src/oci-browser` for the browser app and worker logic
- `proxy/cloudflare-worker` for the optional CORS proxy

## Run the browser app

```bash
npm install
npm run dev
```

## Run the proxy locally

```bash
npm install
npm run proxy:dev
```

## Deploy the proxy

[![Deploy proxy to Cloudflare Workers](https://deploy.workers.cloudflare.com/button)](https://deploy.workers.cloudflare.com/?url=https://github.com/RepoFlow-Package-Management/docker-save-browser)

```bash
npm install
npm run proxy:deploy
```

By default, `proxy/cloudflare-worker/wrangler.jsonc` allows both:

- local dev origins (`localhost:4321`, `127.0.0.1:4321`)
- RepoFlow origins (`https://www.repoflow.io`, `https://repoflow.io`)

If you deploy this for your own site, update `ALLOWED_ORIGINS` to match your own domains.

## License

Apache-2.0. See [LICENSE](./LICENSE) and [NOTICE](./NOTICE).
