use crate::cache::FeedCache;
use crate::types::{FeedCacheEntry, HttpSettings, NewsItem, Settings};
use anyhow::Result;
use reqwest::Client;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::sync::Semaphore;

/// HTTP fetch result
pub struct FetchResponse {
    pub status: u16,
    pub headers: reqwest::header::HeaderMap,
    pub body_text: String,
}

pub enum FetchOutcome {
    Cached(FeedCacheEntry),
    Response(FetchResponse, Option<String>, Option<String>), // response, etag, last-modified
}

/// HTTP client with caching, retries, and concurrency limits
pub struct HttpClient {
    client: Client,
    cache: FeedCache,
    settings: HttpSettings,
    semaphore: Semaphore,
}

impl HttpClient {
    pub fn new(settings: &HttpSettings, cache_dir: &Path) -> Self {
        let timeout = Duration::from_millis(settings.timeout_ms);
        let client = Client::builder()
            .user_agent(&settings.user_agent)
            .timeout(timeout)
            .build()
            .expect("Failed to build HTTP client");

        Self {
            cache: FeedCache::new(cache_dir),
            client,
            settings: settings.clone(),
            semaphore: Semaphore::new(settings.concurrency as usize),
        }
    }

    pub async fn fetch(
        &self,
        url: &str,
        extra_headers: Option<&HashMap<String, String>>,
        cache_mode: &str,
    ) -> Result<FetchOutcome> {
        let cached = self.cache.read(url).await.ok().flatten();

        // If cache-only mode, return cached if available
        if cache_mode == "only" {
            if let Some(entry) = cached {
                return Ok(FetchOutcome::Cached(entry));
            }
            return Err(anyhow::anyhow!("Cache miss for {}", url));
        }

        // If prefer mode and cache is valid, return cached
        if cache_mode == "prefer" {
            if let Some(ref entry) = cached {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                // Default 30 min TTL
                if now - entry.fetched_at <= 1_800_000 {
                    return Ok(FetchOutcome::Cached(entry.clone()));
                }
            }
        }

        let _permit = self.semaphore.acquire().await.unwrap();

        let mut req = self.client.get(url);

        // Conditional request headers
        if let Some(ref entry) = cached {
            if let Some(ref etag) = entry.etag {
                req = req.header("if-none-match", etag);
            }
            if let Some(ref lm) = entry.last_modified {
                req = req.header("if-modified-since", lm);
            }
        }

        if let Some(h) = extra_headers {
            for (k, v) in h {
                req = req.header(k.as_str(), v.as_str());
            }
        }

        let res = req.send().await?;
        let status = res.status().as_u16();
        let headers = res.headers().clone();
        let etag = headers
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        let last_modified = headers
            .get("last-modified")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());

        if status == 304 {
            if let Some(entry) = cached {
                return Ok(FetchOutcome::Cached(entry));
            }
            return Ok(FetchOutcome::Cached(FeedCacheEntry {
                url: url.to_string(),
                etag,
                last_modified,
                fetched_at: 0,
                items: vec![],
            }));
        }

        let body_text = res.text().await?;

        Ok(FetchOutcome::Response(
            FetchResponse {
                status,
                headers,
                body_text,
            },
            etag,
            last_modified,
        ))
    }

    pub fn cache(&self) -> &FeedCache {
        &self.cache
    }

    pub async fn write_cache(
        &self,
        url: &str,
        items: Vec<NewsItem>,
        etag: Option<String>,
        last_modified: Option<String>,
    ) -> Result<()> {
        self.cache
            .write(url, etag, last_modified, items)
            .await
    }
}

/// Resolve cache directory: absolute path as-is, relative paths resolved against user config dir
pub fn resolve_cache_dir(settings: &Settings, user_cfg_dir: &Path) -> PathBuf {
    let cache_path = PathBuf::from(&settings.cache.dir);
    if cache_path.is_absolute() {
        cache_path
    } else {
        user_cfg_dir.join(&cache_path)
    }
}
