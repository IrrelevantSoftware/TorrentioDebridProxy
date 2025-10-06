if (process.env.NODE_ENV !== "production") {
    require("dotenv").config();
}

const express = require("express");
const fetch = require("node-fetch");
const axios = require("axios");

const addon = express();
const PORT = process.env.PORT || 13470;
const API_KEY = process.env.API_KEY;

if (API_KEY) {
    console.log("API_KEY is set. All requests will require it as an 'api_key=your_key' URL query parameter.");
}

// Log all incoming requests
addon.use((req, res, next) => {
    console.log(`[${new Date().toISOString()}] ${req.method} ${req.originalUrl}`);
    next();
});

// Middleware to check for API key if defined
addon.use((req, res, next) => {
    if (API_KEY && req.query.api_key !== API_KEY) {
        console.warn(`Access Denied: Incorrect or missing api_key. Path: ${req.originalUrl}`);
        if (res.socket && !res.socket.destroyed) {
            res.socket.destroy();
        }
        return;
    }
    next();
});

// Normalize and validate TORRENTIO_URL
let rawTorrentioUrl = process.env.TORRENTIO_URL || "";
if (rawTorrentioUrl.startsWith("stremio://")) {
    rawTorrentioUrl = rawTorrentioUrl.replace("stremio://", "https://");
}
rawTorrentioUrl = rawTorrentioUrl.replace(/\/manifest\.json$/, "");
if (!rawTorrentioUrl.startsWith("https://")) {
    console.error("TORRENTIO_URL must be defined and start with https://");
    process.exit(1);
}
const TORRENTIO_URL = rawTorrentioUrl;

// Validate PROXY_SERVER_URL
const PROXY_SERVER_URL = process.env.PROXY_SERVER_URL;
if (!PROXY_SERVER_URL) {
    console.error("PROXY_SERVER_URL must be defined");
    process.exit(1);
}

// Respond helper
const respond = (res, data) => {
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Headers", "*");
    res.setHeader("Content-Type", "application/json");
    res.send(data);
};

// Stremio addon manifest
const MANIFEST = {
    id: "org.custom.torrentio-debrid-proxy",
    version: "1.0.0",
    name: "Torrentio Debrid Proxy",
    description: "Streams via Torrentio with Real-Debrid, proxied through your own server.",
    types: ["movie", "series"],
    resources: ["stream"],
    catalogs: [],
    idPrefixes: ["tt"]
};

// Serve the manifest
addon.get("/manifest.json", (req, res) => {
    respond(res, MANIFEST);
});

// Serve stream metadata and rewrite RD URLs
addon.get("/stream/:type/:id.json", async (req, res) => {
    const { type, id } = req.params;
    console.log("Processing stream request (access-checked):", type, id);

    const apiUrl = `${TORRENTIO_URL}/stream/${type}/${id}.json`;

    try {
        const { data } = await axios.get(apiUrl);

        const streams = (data.streams || []).map(stream => {
            if (!stream.url.includes("/realdebrid/")) return stream;

            let newUrl = stream.url.replace("https://torrentio.strem.fun", PROXY_SERVER_URL);
            if (API_KEY) {
                const sep = newUrl.includes("?") ? "&" : "?";
                newUrl += `${sep}api_key=${encodeURIComponent(API_KEY)}`;
            }

            return { ...stream, url: newUrl };
        });

        respond(res, { streams });
    } catch (err) {
        console.error("Failed to fetch stream data:", err.message);
        respond(res, { streams: [] });
    }
});

// Cache for resolved RD URLs
const resolvedUrlCache = new Map();

// Proxy Real-Debrid streaming URLs
addon.get("/resolve/realdebrid/*", (req, res) => {
    const remotePath = req.params[0];
    const rangeHeader = req.headers["range"];
    tryProxyStreamWithFallback(remotePath, rangeHeader, res);
});

async function tryProxyStreamWithFallback(remotePath, rangeHeader, res) {
    const torrentioUrl = `https://torrentio.strem.fun/resolve/realdebrid/${remotePath}`;

    const tryFetchAndProxy = async (url, isRetry = false) => {
        const controller = new AbortController();
        const signal = controller.signal;

        try {
            const proxyResp = await fetch(url, {
                headers: rangeHeader ? { Range: rangeHeader } : {},
                signal
            });

            if (proxyResp.status === 404 && !isRetry) {
                console.warn("Cached RD URL returned 404. Retrying without cache:", remotePath);
                resolvedUrlCache.delete(remotePath);

                const retryUrl = await resolveRDUrl(torrentioUrl);
                if (!retryUrl) {
                    if (!res.headersSent) res.status(502).send("Failed to resolve stream URL after retry");
                    return;
                }

                return tryFetchAndProxy(retryUrl, true);
            }

            if (!proxyResp.ok) {
                console.error(`Remote fetch failed (${url}):`, proxyResp.status);
                if (!res.headersSent) res.status(proxyResp.status).send("Failed to fetch stream");
                return;
            }

            res.status(proxyResp.status);
            proxyResp.headers.forEach((val, key) => res.setHeader(key, val));

            const TIMEOUT_MS = 5 * 60 * 1000;
            let timeout = setTimeout(() => {
                console.log(`No activity detected for stream ${remotePath}. Aborting.`);
                controller.abort();
            }, TIMEOUT_MS);

            res.on("close", () => {
                clearTimeout(timeout);
                controller.abort();
            });

            proxyResp.body.on("data", () => {
                clearTimeout(timeout);
                timeout = setTimeout(() => {
                    console.log(`Idle timeout for stream ${remotePath}. Aborting.`);
                    controller.abort();
                }, TIMEOUT_MS);
            });

            proxyResp.body.pipe(res);
        } catch (err) {
            if (err.name === "AbortError") {
                console.warn(`Stream aborted: ${remotePath}`);
            } else {
                console.error(`Proxy error for ${remotePath}:`, err.message);
            }

            if (!res.headersSent) res.status(502).send("Proxy failed");
        }
    };

    if (resolvedUrlCache.has(remotePath)) {
        return tryFetchAndProxy(resolvedUrlCache.get(remotePath));
    }

    const newUrl = await resolveRDUrl(torrentioUrl);
    if (!newUrl) {
        if (!res.headersSent) res.status(502).send("Failed to resolve stream URL");
        return;
    }

    return tryFetchAndProxy(newUrl);
}

async function resolveRDUrl(torrentioUrl) {
    try {
        console.log("Resolving redirect:", torrentioUrl);
        const resp = await fetch(torrentioUrl, {
            method: "HEAD",
            redirect: "manual"
        });

        const redirectedUrl = resp.headers.get("location");
        if (!redirectedUrl) {
            console.error("No redirect found for:", torrentioUrl);
            return null;
        }

        const cacheKey = torrentioUrl.split("/realdebrid/")[1];
        if (cacheKey) {
            resolvedUrlCache.set(cacheKey, redirectedUrl);
            console.log("Cached redirect:", redirectedUrl, "for key:", cacheKey);
        }
        return redirectedUrl;
    } catch (err) {
        console.error("Redirect resolve error for:", torrentioUrl, err.message);
        return null;
    }
}

addon.listen(PORT, () => {
    console.log(`Addon server is running on port ${PORT}`);
});
