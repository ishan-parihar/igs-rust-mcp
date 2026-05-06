use crate::config;
use crate::http::{self as http_mod, HttpClient};
use crate::parsers;
use crate::types::*;
use rmcp::{
    Json,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::*,
    tool, tool_handler, tool_router,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use chrono::Datelike;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;

// ─── Internal Server State ──────────────────────────────────────

struct InsightStorage {
    articles: Vec<ArticleInsight>,
}

impl InsightStorage {
    fn new() -> Self {
        Self { articles: vec![] }
    }

    fn add_article(&mut self, article: ArticleInsight) {
        self.articles.push(article);
    }

    fn clear(&mut self) {
        self.articles.clear();
    }

    fn stats(&self) -> InsightStats {
        let total_articles = self.articles.len();
        let mut entities = std::collections::HashSet::new();
        let mut domains = std::collections::HashSet::new();
        for a in &self.articles {
            for e in &a.entities {
                entities.insert(e.name.clone());
            }
            for d in &a.domains {
                domains.insert(d.domain.clone());
            }
        }
        InsightStats {
            total_articles,
            total_entities: entities.len(),
            total_domains: domains.len(),
            avg_entities_per_article: if total_articles > 0 {
                self.articles.iter().map(|a| a.entities.len() as f64).sum::<f64>() / total_articles as f64
            } else {
                0.0
            },
            avg_domains_per_article: if total_articles > 0 {
                self.articles.iter().map(|a| a.domains.len() as f64).sum::<f64>() / total_articles as f64
            } else {
                0.0
            },
        }
    }

    fn find_inter_domain_connections(&self, entity: &str, min_domains: usize) -> Vec<EntityConnection> {
        let mut domain_map: HashMap<String, DomainConnection> = HashMap::new();
        for article in &self.articles {
            let matches_entity = article.entities.iter().any(|e| {
                e.name.to_lowercase() == entity.to_lowercase()
                    || e.normalized_id.as_ref().map_or(false, |id| id.to_lowercase() == entity.to_lowercase())
            });
            if !matches_entity { continue; }

            for d in &article.domains {
                let entry = domain_map.entry(d.domain.clone()).or_insert_with(|| DomainConnection {
                    domain: d.domain.clone(),
                    article_ids: vec![],
                    article_titles: vec![],
                });
                entry.article_ids.push(article.id.clone());
                entry.article_titles.push(article.title.clone());
            }
        }

        let domains_vec: Vec<DomainConnection> = domain_map.into_values().collect();
        let ndomains = domains_vec.len();
        if ndomains >= min_domains {
            let entity_type = self.articles.iter()
                .flat_map(|a| a.entities.iter())
                .find(|e| e.name.to_lowercase() == entity.to_lowercase())
                .map(|e| e.entity_type.clone())
                .unwrap_or_default();
            vec![EntityConnection {
                entity: entity.to_string(),
                entity_type,
                domains: domains_vec,
                connection_strength: ndomains as f64,
            }]
        } else {
            vec![]
        }
    }

    fn find_all_inter_domain_connections(&self, min_domains: usize) -> Vec<EntityConnection> {
        let mut entity_domains: HashMap<String, (String, HashMap<String, DomainConnection>)> = HashMap::new();
        for article in &self.articles {
            for e in &article.entities {
                let key = e.name.to_lowercase();
                let (etype, domain_map) = entity_domains.entry(key).or_insert_with(|| {
                    (e.entity_type.clone(), HashMap::new())
                });
                if etype.is_empty() {
                    *etype = e.entity_type.clone();
                }
                for d in &article.domains {
                    let entry = domain_map.entry(d.domain.clone()).or_insert_with(|| DomainConnection {
                        domain: d.domain.clone(),
                        article_ids: vec![],
                        article_titles: vec![],
                    });
                    entry.article_ids.push(article.id.clone());
                    entry.article_titles.push(article.title.clone());
                }
            }
        }

        entity_domains
            .into_iter()
            .filter_map(|(key, (etype, dm))| {
                let nd = dm.len();
                if nd < min_domains { return None; }
                let d2: Vec<DomainConnection> = dm.into_values().collect();
                Some(EntityConnection {
                    entity: key,
                    entity_type: etype,
                    domains: d2,
                    connection_strength: nd as f64,
                })
            })
            .collect()
    }

    fn detect_trending(&self, time_window_ms: i64, min_growth: f64, min_current: u32) -> Vec<TrendingEntity> {
        let now = chrono::Utc::now().timestamp_millis();
        let cutoff = now - time_window_ms;
        let half_cutoff = now - (time_window_ms * 2);

        let mut current: HashMap<String, (u32, String)> = HashMap::new();
        let mut previous: HashMap<String, u32> = HashMap::new();

        for article in &self.articles {
            let t = chrono::DateTime::parse_from_rfc3339(&article.pub_date)
                .ok()
                .map(|dt| dt.timestamp_millis())
                .unwrap_or(0);

            for e in &article.entities {
                let name = e.name.to_lowercase();
                if t >= cutoff {
                    let (count, _etype) = current.entry(name).or_insert((0, e.entity_type.clone()));
                    *count += 1;
                } else if t >= half_cutoff {
                    *previous.entry(name).or_insert(0) += 1;
                }
            }
        }

        current
            .into_iter()
            .filter_map(|(name, (current_count, etype))| {
                if current_count < min_current { return None; }
                let prev_count = previous.get(&name).copied().unwrap_or(0);
                let growth = if prev_count > 0 {
                    current_count as f64 / prev_count as f64
                } else {
                    current_count as f64
                };
                if growth < min_growth { return None; }
                Some(TrendingEntity {
                    entity: name,
                    entity_type: etype,
                    current_mentions: current_count,
                    previous_mentions: prev_count,
                    growth,
                    normalized_growth: (growth / (1.0 + growth)).min(1.0),
                })
            })
            .collect()
    }
}

// ─── Server State ────────────────────────────────────────────────

#[derive(Clone)]
pub struct IgsMcpServer {
    tool_router: ToolRouter<IgsMcpServer>,
    insights: Arc<Mutex<InsightStorage>>,
}

impl IgsMcpServer {
    fn settings_path() -> PathBuf {
        config::user_config_dir().join("settings.yml")
    }
}

// ─── Tool Input/Output Types ─────────────────────────────────────

// Pools tool types
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct PoolListInput {}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct PoolListOutput {
    pub pools: Vec<Pool>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct PoolUpsertInput {
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub is_active: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct PoolUpsertOutput {
    pub updated: bool,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct PoolDeleteInput {
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct PoolDeleteOutput {
    pub removed: bool,
}

// Sources tool types
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct SourceListInput {
    pub pools: Option<Vec<String>>,
    pub active_only: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct SourceListOutput {
    pub sources: Vec<Source>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct SourceUpsertInput {
    pub id: Option<String>,
    pub name: String,
    #[serde(rename = "type")]
    pub source_type: String,
    pub url: String,
    pub headers: Option<HashMap<String, String>>,
    pub parser: Option<String>,
    pub pools: Option<Vec<String>>,
    pub countries: Option<Vec<String>>,
    pub cities: Option<Vec<String>>,
    pub domains: Option<Vec<String>>,
    pub is_active: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct SourceUpsertOutput {
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct SourceDeleteInput {
    pub id: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct SourceDeleteOutput {
    pub removed: bool,
}

// Parser tool types
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ParserInfo {
    pub key: String,
    pub note: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ParserListOutput {
    pub parsers: Vec<ParserInfo>,
}

// Autodiscover types
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct AutodiscoverInput {
    pub url: String,
    pub pools: Option<Vec<String>>,
    pub name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct AutodiscoverOutput {
    pub kind: String,
    pub url: Option<String>,
    pub sample: Vec<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct EnableScraperInput {
    pub id: String,
    pub list_url: Option<String>,
    pub selectors: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct EnableScraperOutput {
    pub updated: bool,
}

// Country/City/Domain types
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct CountryInfo {
    pub name: String,
    pub code: String,
    pub source_count: usize,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct CountriesOutput {
    pub countries: Vec<CountryInfo>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct CityInfo {
    pub name: String,
    pub source_count: usize,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct CitiesOutput {
    pub cities: Vec<CityInfo>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DomainInfoCount {
    pub name: String,
    pub source_count: usize,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct DomainsOutput {
    pub domains: Vec<DomainInfoCount>,
}

// News fetch types
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct NewsFetchInput {
    pub pools: Option<Vec<String>>,
    pub sources: Option<Vec<String>>,
    pub countries: Option<Vec<String>>,
    pub cities: Option<Vec<String>>,
    pub domains: Option<Vec<String>>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub keywords: Option<serde_json::Value>,
    pub exclude_keywords: Option<Vec<String>>,
    pub match_all: Option<bool>,
    pub discovery_mode: Option<bool>,
    pub limit: Option<i32>,
    pub cache_mode: Option<String>,
    pub urgency: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct NewsFetchMeta {
    pub sources_queried: usize,
    pub sources_succeeded: usize,
    pub sources_failed: usize,
    pub total_sources: usize,
    pub pool_ids: Vec<String>,
    pub keywords: Vec<String>,
    pub count: usize,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct NewsFetchOutput {
    pub items: Vec<NewsItem>,
    pub count: usize,
    pub meta: NewsFetchMeta,
}

// News test source
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct NewsTestInput {
    pub id: String,
    pub cache_mode: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct NewsTestOutput {
    pub items: Vec<NewsItem>,
    pub count: usize,
}

// News enrich
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct EnrichItemInput {
    pub id: String,
    pub title: String,
    pub link: String,
    pub pub_date: String,
    pub source_name: String,
    pub pool_id: String,
    pub content_snippet: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct NewsEnrichInput {
    pub items: Vec<EnrichItemInput>,
    pub extract: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct NewsEnrichOutput {
    pub items: Vec<serde_json::Value>,
    pub meta: serde_json::Value,
}

// Reddit search types
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct RedditSearchInput {
    pub query: String,
    pub subreddits: Option<Vec<String>>,
    pub sort: Option<String>,
    pub time: Option<String>,
    pub limit: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct RedditSearchMeta {
    pub query: String,
    pub subreddits: Option<Vec<String>>,
    pub sort: String,
    pub time: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct RedditSearchOutput {
    pub posts: Vec<NewsItem>,
    pub count: usize,
    pub meta: RedditSearchMeta,
}

// Research types
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ResearchSearchInput {
    pub query: String,
    pub sources: Option<Vec<String>>,
    pub categories: Option<Vec<String>>,
    pub year_from: Option<i32>,
    pub year_to: Option<i32>,
    pub limit: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ResearchSearchMeta {
    pub query: String,
    pub sources: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ResearchSearchOutput {
    pub papers: Vec<ResearchPaper>,
    pub count: usize,
    pub total: usize,
    pub meta: ResearchSearchMeta,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ResearchPaperInput {
    pub paper_id: String,
    pub include_citations: Option<bool>,
    pub include_references: Option<bool>,
    pub extract_pdf: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct PaperDetail {
    pub id: String,
    pub title: String,
    pub authors: Vec<String>,
    pub abstract_text: String,
    pub year: Option<i32>,
    pub citations: Option<i32>,
    pub references: Option<i32>,
    pub pdf_url: Option<String>,
    pub content: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ResearchPaperOutput {
    pub paper: PaperDetail,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ResearchDownloadInput {
    pub paper_id: String,
    pub output_path: Option<String>,
    pub format: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct ResearchDownloadOutput {
    pub pdf_path: Option<String>,
    pub markdown_path: Option<String>,
    pub file_size: u64,
    pub metadata: serde_json::Value,
}

// Web search types
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct WebSearchInput {
    pub query: String,
    pub provider: Option<String>,
    pub max_results: Option<i32>,
    pub topic: Option<String>,
    pub include_domains: Option<Vec<String>>,
    pub exclude_domains: Option<Vec<String>>,
    pub days: Option<i32>,
    pub include_answer: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct WebSearchMeta {
    pub provider: String,
    pub query: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct WebSearchOutput {
    pub results: Vec<serde_json::Value>,
    pub count: usize,
    pub answer: Option<String>,
    pub meta: WebSearchMeta,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct WebScrapeInput {
    pub url: String,
    pub provider: Option<String>,
    pub formats: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct WebScrapeOutput {
    pub success: bool,
    pub url: String,
    pub markdown: Option<String>,
    pub meta: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct WebCrawlInput {
    pub url: String,
    pub provider: Option<String>,
    pub limit: Option<i32>,
    pub max_depth: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct WebCrawlOutput {
    pub success: bool,
    pub url: String,
    pub count: usize,
    pub meta: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct WebMapInput {
    pub url: String,
    pub provider: Option<String>,
    pub limit: Option<i32>,
    pub search: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct WebMapOutput {
    pub success: bool,
    pub url: String,
    pub links: Vec<serde_json::Value>,
    pub count: usize,
    pub meta: serde_json::Value,
}

// Insight types
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct InsightConnectionInput {
    pub entity: String,
    pub min_domains: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct InsightConnectionOutput {
    pub connections: Vec<EntityConnection>,
    pub count: usize,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct InsightAllConnectionsInput {
    pub min_domains: Option<i32>,
    pub limit: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct InsightAllConnectionsOutput {
    pub connections: Vec<EntityConnection>,
    pub total_found: usize,
    pub stats: InsightStats,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct InsightTrendingInput {
    pub time_window_hours: Option<i64>,
    pub min_growth: Option<f64>,
    pub min_current_mentions: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct InsightTrendingOutput {
    pub trending: Vec<TrendingEntity>,
    pub count: usize,
    pub stats: InsightStats,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct InsightIndexArticle {
    pub id: String,
    pub title: String,
    pub pub_date: String,
    pub source_name: String,
    pub domains: Option<Vec<DomainInfo>>,
    pub entities: Option<Vec<EntityInfo>>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct InsightIndexInput {
    pub articles: Vec<InsightIndexArticle>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct InsightIndexOutput {
    pub indexed: usize,
    pub stats: InsightStats,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct InsightStatsOutput {
    pub stats: InsightStats,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct InsightClearOutput {
    pub cleared: bool,
}

// ─── Tool Router ────────────────────────────────────────────────

#[tool_router(router = tool_router)]
impl IgsMcpServer {
    pub fn new() -> Self {
        Self {
            tool_router: Self::tool_router(),
            insights: Arc::new(Mutex::new(InsightStorage::new())),
        }
    }

    // ── Pool Tools ──────────────────────────────────────────────

    #[tool(name = "pools.list", description = "List all configured pools")]
    pub async fn pools_list(&self) -> Result<Json<PoolListOutput>, String> {
        match config::load_pools().await {
            Ok(pf) => Ok(Json(PoolListOutput { pools: pf.pools })),
            Err(e) => Err(format!("Failed to load pools: {}", e)),
        }
    }

    #[tool(name = "pools.upsert", description = "Create or update a pool")]
    pub async fn pools_upsert(
        &self,
        params: Parameters<PoolUpsertInput>,
    ) -> Result<Json<PoolUpsertOutput>, String> {
        let input = params.0;
        match config::load_pools().await {
            Ok(mut pf) => {
                if let Some(idx) = pf.pools.iter().position(|p| p.id == input.id) {
                    pf.pools[idx] = Pool {
                        id: input.id.clone(),
                        name: input.name,
                        description: input.description,
                        is_active: input.is_active,
                    };
                } else {
                    pf.pools.push(Pool {
                        id: input.id,
                        name: input.name,
                        description: input.description,
                        is_active: input.is_active,
                    });
                }
                config::save_pools(&pf).await.map_err(|e| format!("Save failed: {}", e))?;
                Ok(Json(PoolUpsertOutput { updated: true }))
            }
            Err(e) => Err(format!("Failed to load pools: {}", e)),
        }
    }

    #[tool(name = "pools.delete", description = "Delete a pool by id")]
    pub async fn pools_delete(
        &self,
        params: Parameters<PoolDeleteInput>,
    ) -> Result<Json<PoolDeleteOutput>, String> {
        match config::load_pools().await {
            Ok(mut pf) => {
                let before = pf.pools.len();
                pf.pools.retain(|p| p.id != params.0.id);
                let removed = pf.pools.len() < before;
                config::save_pools(&pf).await.map_err(|e| format!("Save failed: {}", e))?;
                Ok(Json(PoolDeleteOutput { removed }))
            }
            Err(e) => Err(format!("Failed to load pools: {}", e)),
        }
    }

    // ── Source Tools ────────────────────────────────────────────

    #[tool(name = "sources.list", description = "List sources with optional pool/active filters")]
    pub async fn sources_list(
        &self,
        params: Parameters<SourceListInput>,
    ) -> Result<Json<SourceListOutput>, String> {
        match config::load_sources().await {
            Ok(sf) => {
                let mut list = sf.sources;
                if let Some(ref pools) = params.0.pools {
                    list.retain(|s| s.pools.iter().any(|p| pools.contains(p)));
                }
                if params.0.active_only.unwrap_or(false) {
                    list.retain(|s| s.is_active.unwrap_or(true));
                }
                Ok(Json(SourceListOutput { sources: list }))
            }
            Err(e) => Err(format!("Failed to load sources: {}", e)),
        }
    }

    #[tool(name = "sources.upsert", description = "Create or update a source")]
    pub async fn sources_upsert(
        &self,
        params: Parameters<SourceUpsertInput>,
    ) -> Result<Json<SourceUpsertOutput>, String> {
        let input = params.0;
        match config::load_sources().await {
            Ok(mut sf) => {
                let id = input.id.unwrap_or_else(|| {
                    input.name.to_lowercase().replace(|c: char| !c.is_alphanumeric() && c != '_', "_")
                });
                let src = Source {
                    id: id.clone(),
                    name: input.name,
                    source_type: input.source_type,
                    url: input.url,
                    headers: input.headers,
                    parser: input.parser,
                    parser_config: None,
                    pools: input.pools.unwrap_or_default(),
                    countries: input.countries.unwrap_or_default(),
                    cities: input.cities.unwrap_or_default(),
                    domains: input.domains.unwrap_or_default(),
                    is_active: input.is_active,
                    platform: None,
                    tier: None,
                    rate_limit: None,
                    source_category: None,
                };
                if let Some(idx) = sf.sources.iter().position(|s| s.id == id) {
                    sf.sources[idx] = src;
                } else {
                    sf.sources.push(src);
                }
                config::save_sources(&sf).await.map_err(|e| format!("Save failed: {}", e))?;
                Ok(Json(SourceUpsertOutput { id }))
            }
            Err(e) => Err(format!("Failed to load sources: {}", e)),
        }
    }

    #[tool(name = "sources.delete", description = "Delete a source by id")]
    pub async fn sources_delete(
        &self,
        params: Parameters<SourceDeleteInput>,
    ) -> Result<Json<SourceDeleteOutput>, String> {
        match config::load_sources().await {
            Ok(mut sf) => {
                let before = sf.sources.len();
                sf.sources.retain(|s| s.id != params.0.id);
                let removed = sf.sources.len() < before;
                config::save_sources(&sf).await.map_err(|e| format!("Save failed: {}", e))?;
                Ok(Json(SourceDeleteOutput { removed }))
            }
            Err(e) => Err(format!("Failed to load sources: {}", e)),
        }
    }

    // ── Parser Tools ────────────────────────────────────────────

    #[tool(name = "parsers.list", description = "List available parser keys")]
    pub async fn parsers_list(&self) -> Result<Json<ParserListOutput>, String> {
        Ok(Json(ParserListOutput {
            parsers: vec![
                ParserInfo { key: "rss".into(), note: "Generic RSS/Atom via feed-rs".into() },
                ParserInfo { key: "ofac".into(), note: "OFAC Recent Actions HTML parser".into() },
                ParserInfo { key: "ussf_cfc".into(), note: "US Space Force CFC News HTML parser".into() },
                ParserInfo { key: "who_dons".into(), note: "WHO Disease Outbreak News JSON parser".into() },
                ParserInfo { key: "newslaundry".into(), note: "Newslaundry list page JSON-in-script parser".into() },
                ParserInfo { key: "generic_html".into(), note: "Generic HTML scraper with auto-detect".into() },
            ],
        }))
    }

    // ── Autodiscover Tools ──────────────────────────────────────

    #[tool(name = "sources.autodiscover", description = "Auto-discover feeds/selectors from a homepage URL")]
    pub async fn sources_autodiscover(
        &self,
        params: Parameters<AutodiscoverInput>,
    ) -> Result<Json<AutodiscoverOutput>, String> {
        let input = params.0;
        match config::load_settings().await {
            Ok(settings) => {
                let cache_dir = http_mod::resolve_cache_dir(&settings, &config::user_config_dir());
                let http = HttpClient::new(&settings.http, &cache_dir);
                match http.fetch(&input.url, None, "bypass").await {
                    Ok(outcome) => {
                        let body = match outcome {
                            http_mod::FetchOutcome::Cached(_) => return Err("Unexpected cache hit".into()),
                            http_mod::FetchOutcome::Response(resp, _, _) => resp.body_text,
                        };
                        // Check for RSS/Atom feed links (sync fn, no Send issues)
                        let feed_url = find_feed_url(&body, &input.url);

                        if let Some(feed) = feed_url {
                            Ok(Json(AutodiscoverOutput {
                                kind: "rss".into(),
                                url: Some(feed),
                                sample: vec![],
                            }))
                        } else {
                            // Try sitemap
                            let sitemap_url = format!("{}/sitemap.xml", input.url.trim_end_matches('/'));
                            match http.fetch(&sitemap_url, None, "bypass").await {
                                Ok(_) => Ok(Json(AutodiscoverOutput {
                                    kind: "sitemap".into(),
                                    url: Some(sitemap_url),
                                    sample: vec![],
                                })),
                                Err(_) => Ok(Json(AutodiscoverOutput {
                                    kind: "none".into(),
                                    url: None,
                                    sample: vec![],
                                })),
                            }
                        }
                    }
                    Err(e) => Err(format!("Fetch failed: {}", e)),
                }
            }
            Err(e) => Err(format!("Settings load failed: {}", e)),
        }
    }

    #[tool(name = "sources.enableGenericScraper", description = "Enable generic HTML scraping for a source")]
    pub async fn sources_enable_scraper(
        &self,
        params: Parameters<EnableScraperInput>,
    ) -> Result<Json<EnableScraperOutput>, String> {
        let input = params.0;
        match config::load_sources().await {
            Ok(mut sf) => {
                if let Some(idx) = sf.sources.iter().position(|s| s.id == input.id) {
                    let s = &mut sf.sources[idx];
                    s.parser = Some("generic_html".into());
                    s.parser_config = Some(SourceParserConfig {
                        list_url: input.list_url,
                        selectors: input.selectors.map(|sel_map| Selectors {
                            item: sel_map.get("item").cloned().unwrap_or_default(),
                            title: sel_map.get("title").cloned(),
                            link: sel_map.get("link").cloned(),
                            date: sel_map.get("date").cloned(),
                            desc: sel_map.get("desc").cloned(),
                        }),
                    });
                    config::save_sources(&sf).await.map_err(|e| format!("Save failed: {}", e))?;
                    Ok(Json(EnableScraperOutput { updated: true }))
                } else {
                    Err(format!("Source not found: {}", input.id))
                }
            }
            Err(e) => Err(format!("Failed to load sources: {}", e)),
        }
    }

    // ── Country/City/Domain Tools ───────────────────────────────

    #[tool(name = "sources.countries", description = "List countries with available source counts")]
    pub async fn sources_countries(&self) -> Result<Json<CountriesOutput>, String> {
        let countries = config::load_countries().await.unwrap_or(serde_json::json!({"countries": []}));
        let sources = config::load_sources().await.unwrap_or(SourcesFile { sources: vec![] });
        let out: Vec<CountryInfo> = countries["countries"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|c| {
                        let name = c["name"].as_str()?.to_string();
                        let code = c["code"].as_str()?.to_string();
                        let count = sources
                            .sources
                            .iter()
                            .filter(|s| {
                                s.is_active.unwrap_or(true)
                                    && s.countries.iter().any(|sc| sc.to_uppercase() == code.to_uppercase())
                            })
                            .count();
                        Some(CountryInfo { name, code, source_count: count })
                    })
                    .collect()
            })
            .unwrap_or_default();
        Ok(Json(CountriesOutput { countries: out }))
    }

    #[tool(name = "sources.cities", description = "List cities with available source counts")]
    pub async fn sources_cities(&self) -> Result<Json<CitiesOutput>, String> {
        let sources = config::load_sources().await.unwrap_or(SourcesFile { sources: vec![] });
        let mut city_map: HashMap<String, usize> = HashMap::new();
        for s in &sources.sources {
            if s.is_active.unwrap_or(true) {
                for c in &s.cities {
                    *city_map.entry(c.clone()).or_default() += 1;
                }
            }
        }
        let mut cities: Vec<CityInfo> = city_map
            .into_iter()
            .map(|(name, count)| CityInfo { name, source_count: count })
            .collect();
        cities.sort_by(|a, b| b.source_count.cmp(&a.source_count));
        Ok(Json(CitiesOutput { cities }))
    }

    #[tool(name = "sources.domains", description = "List domains with available source counts")]
    pub async fn sources_domains(&self) -> Result<Json<DomainsOutput>, String> {
        let sources = config::load_sources().await.unwrap_or(SourcesFile { sources: vec![] });
        let mut domain_map: HashMap<String, usize> = HashMap::new();
        for s in &sources.sources {
            if s.is_active.unwrap_or(true) {
                for d in &s.domains {
                    *domain_map.entry(d.clone()).or_default() += 1;
                }
            }
        }
        let mut domains: Vec<DomainInfoCount> = domain_map
            .into_iter()
            .map(|(name, count)| DomainInfoCount { name, source_count: count })
            .collect();
        domains.sort_by(|a, b| b.source_count.cmp(&a.source_count));
        Ok(Json(DomainsOutput { domains }))
    }

    // ── News Tools ──────────────────────────────────────────────

    #[tool(name = "news.fetch", description = "Fetch normalized news items from configured sources. Supports filtering by pools, sources, countries, cities, domains, time range, and keywords/clusters.")]
    pub async fn news_fetch(
        &self,
        params: Parameters<NewsFetchInput>,
    ) -> Result<Json<NewsFetchOutput>, String> {
        let input = params.0;
        let settings = config::load_settings().await.map_err(|e| format!("Settings: {}", e))?;
        let cache_dir = http_mod::resolve_cache_dir(&settings, &config::user_config_dir());
        let http = std::sync::Arc::new(HttpClient::new(&settings.http, &cache_dir));
        let sf = config::load_sources().await.map_err(|e| format!("Sources: {}", e))?;

        let cache_mode = input.cache_mode.unwrap_or_else(|| "prefer".to_string());
        let limit = input.limit.unwrap_or(100).min(500).max(1) as usize;

        let mut sources = sf.sources;
        sources.retain(|s| s.is_active.unwrap_or(true));

        // Filter sources by pool
        if let Some(ref pool_ids) = input.pools {
            if !pool_ids.is_empty() {
                sources.retain(|s| s.pools.iter().any(|p| pool_ids.contains(p)));
            }
        }

        // Filter by country/city/domain
        if let Some(ref countries) = input.countries {
            if !countries.is_empty() {
                sources.retain(|s| {
                    s.countries.iter().any(|sc| {
                        countries.iter().any(|c| sc.to_uppercase() == c.to_uppercase())
                    })
                });
            }
        }
        if let Some(ref cities) = input.cities {
            if !cities.is_empty() {
                sources.retain(|s| s.cities.iter().any(|c| cities.iter().any(|cc| c.to_lowercase() == cc.to_lowercase())));
            }
        }
        if let Some(ref domains) = input.domains {
            if !domains.is_empty() {
                sources.retain(|s| {
                    s.domains.iter().any(|d| domains.iter().any(|dd| d.to_lowercase() == dd.to_lowercase()))
                });
            }
        }

        let mut all_items = Vec::new();
        let mut succeeded = 0usize;
        let mut failed = 0usize;

        // Use semaphore for concurrency
        let sem = Arc::new(tokio::sync::Semaphore::new(settings.http.concurrency as usize));
        let total = sources.len();

        let mut handles = Vec::new();
        for src in sources.into_iter().take(50) {
            // Limit to 50 sources per request
            let sem = sem.clone();
            let http_ref = http.clone();
            let cm = cache_mode.clone();
            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                match parsers::parse_by_source(&src, &http_ref, &cm, None).await {
                    Ok(items) => (items, true),
                    Err(_) => (vec![], false),
                }
            }));
        }

        for handle in handles {
            match handle.await {
                Ok((items, ok)) => {
                    all_items.extend(items);
                    if ok { succeeded += 1; } else { failed += 1; }
                }
                Err(_) => { failed += 1; }
            }
        }

        // Apply filters
        all_items.sort_by(|a, b| b.pub_date.cmp(&a.pub_date));

        // Time filter
        if input.start.is_some() || input.end.is_some() {
            all_items = parsers::filter_by_time(
                all_items,
                input.start.as_deref(),
                input.end.as_deref(),
            );
        }

        // Keyword filter
        let mut keyword_vec: Vec<String> = Vec::new();
        if let Some(ref kw) = input.keywords {
            if let Some(arr) = kw.as_array() {
                keyword_vec = arr.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
            }
        }
        if !input.discovery_mode.unwrap_or(false) {
            let exclude = input.exclude_keywords.as_ref().cloned().unwrap_or_default();
            all_items = parsers::filter_by_keywords(
                all_items,
                input.keywords.as_ref(),
                &exclude,
                input.match_all.unwrap_or(false),
            );
        }

        all_items.truncate(limit);

        // Deduplicate
        all_items = parsers::batch_similar(all_items, 0.3);

        let count = all_items.len();

        let meta = NewsFetchMeta {
            sources_queried: total,
            sources_succeeded: succeeded,
            sources_failed: failed,
            total_sources: total,
            pool_ids: input.pools.unwrap_or_default(),
            keywords: keyword_vec,
            count,
        };

        Ok(Json(NewsFetchOutput {
            items: all_items,
            count,
            meta,
        }))
    }

    #[tool(name = "news.testSource", description = "Debug helper. Test a single source and return up to 10 items.")]
    pub async fn news_test_source(
        &self,
        params: Parameters<NewsTestInput>,
    ) -> Result<Json<NewsTestOutput>, String> {
        let input = params.0;
        let settings = config::load_settings().await.map_err(|e| format!("Settings: {}", e))?;
        let cache_dir = http_mod::resolve_cache_dir(&settings, &config::user_config_dir());
        let http = HttpClient::new(&settings.http, &cache_dir);
        let sf = config::load_sources().await.map_err(|e| format!("Sources: {}", e))?;

        let src = sf.sources.iter().find(|s| s.id == input.id)
            .ok_or_else(|| format!("Source not found: {}", input.id))?;

        let cache_mode = input.cache_mode.as_deref().unwrap_or("bypass");
        let items = parsers::parse_by_source(src, &http, cache_mode, None)
            .await
            .map_err(|e| format!("Parse error: {}", e))?;

        let items: Vec<NewsItem> = items.into_iter().take(10).collect();
        let count = items.len();
        Ok(Json(NewsTestOutput { items, count }))
    }

    #[tool(name = "news.enrich", description = "NLP enrichment (offline). Adds basic topics, sentiment, and summary to items.")]
    pub async fn news_enrich(
        &self,
        params: Parameters<NewsEnrichInput>,
    ) -> Result<Json<NewsEnrichOutput>, String> {
        let input = params.0;
        let extract = input.extract.unwrap_or_else(|| vec![
            "topics".into(), "entities".into(), "sentiment".into(), "summary".into(),
        ]);
        let want: std::collections::HashSet<String> = extract.into_iter().collect();

        let mut out = Vec::new();
        for item in &input.items {
            let text = format!("{} {}", item.title, item.content_snippet.as_deref().unwrap_or(""));

            let mut enriched = serde_json::json!({
                "id": item.id,
                "title": item.title,
                "link": item.link,
                "pub_date": item.pub_date,
                "source_name": item.source_name,
                "pool_id": item.pool_id,
                "content_snippet": item.content_snippet,
            });

            if want.contains("topics") {
                let topics = extract_topics(&text, 8);
                enriched["topics"] = serde_json::json!(topics);
            }

            if want.contains("entities") {
                let entities = extract_basic_entities(&text);
                enriched["entities"] = serde_json::json!(entities);
            }

            if want.contains("sentiment") {
                let sentiment = basic_sentiment(&text);
                enriched["sentiment"] = serde_json::json!(sentiment);
            }

            if want.contains("summary") {
                let summary = item.content_snippet.as_deref()
                    .and_then(|s| s.split(|c| c == '.' || c == '!' || c == '?')
                        .find(|s| !s.trim().is_empty())
                        .map(|s| s.trim().to_string()))
                    .unwrap_or_else(|| item.title.clone());
                enriched["summary"] = serde_json::json!(summary);
            }

            out.push(enriched);
        }

        Ok(Json(NewsEnrichOutput {
            items: out,
            meta: serde_json::json!({
                "items_enriched": input.items.len(),
                "note": "Basic offline NLP enrichment (no external API calls)"
            }),
        }))
    }

    // ── Reddit Tool ─────────────────────────────────────────────

    #[tool(name = "reddit.search", description = "Search Reddit posts. Uses ophan.herokuapp.com for anonymous Reddit search.")]
    pub async fn reddit_search(
        &self,
        params: Parameters<RedditSearchInput>,
    ) -> Result<Json<RedditSearchOutput>, String> {
        let input = params.0;
        let sort = input.sort.as_deref().unwrap_or("relevance");
        let time = input.time.as_deref().unwrap_or("all");
        let limit = input.limit.unwrap_or(25).min(100).max(1);

        // Use ophan.herokuapp.com as a proxy for anonymous Reddit access
        let query_enc = urlencoding(&input.query);
        let subreddit_filter = input.subreddits.as_ref()
            .map(|sr| sr.join("+"))
            .unwrap_or_default();

        let api_url = if subreddit_filter.is_empty() {
            format!("https://www.reddit.com/search.json?q={}&sort={}&t={}&limit={}",
                query_enc, sort, time, limit)
        } else {
            format!("https://www.reddit.com/r/{}/search.json?q={}&sort={}&t={}&limit={}",
                subreddit_filter, query_enc, sort, time, limit)
        };

        let settings = config::load_settings().await.map_err(|e| format!("Settings: {}", e))?;
        let cache_dir = http_mod::resolve_cache_dir(&settings, &config::user_config_dir());
        let http = HttpClient::new(&settings.http, &cache_dir);

        match http.fetch(&api_url, None, "bypass").await {
            Ok(outcome) => {
                let body = match outcome {
                    http_mod::FetchOutcome::Cached(entry) => {
                        // Return cached results
                        let posts: Vec<NewsItem> = entry.items;
                        return Ok(Json(RedditSearchOutput {
                            count: posts.len(),
                            posts,
                            meta: RedditSearchMeta {
                                query: input.query,
                                subreddits: input.subreddits,
                                sort: sort.to_string(),
                                time: time.to_string(),
                            },
                        }));
                    }
                    http_mod::FetchOutcome::Response(resp, _, _) => resp.body_text,
                };

                let json: serde_json::Value = serde_json::from_str(&body)
                    .map_err(|e| format!("Failed to parse Reddit response: {}", e))?;

                let posts: Vec<NewsItem> = json["data"]["children"]
                    .as_array()
                    .map(|children| {
                        children.iter().filter_map(|child| {
                            let data = &child["data"];
                            let title = data["title"].as_str().unwrap_or("Untitled");
                            let permalink = data["permalink"].as_str().unwrap_or("");
                            let url = format!("https://www.reddit.com{}", permalink);
                            let subreddit = data["subreddit"].as_str().unwrap_or("unknown");
                            let author = data["author"].as_str();
                            let score = data["score"].as_i64().unwrap_or(0);
                            let num_comments = data["num_comments"].as_i64().unwrap_or(0);
                            let created_utc = data["created_utc"].as_f64().unwrap_or(0.0);
                            let selftext = data["selftext"].as_str().unwrap_or("");
                            let created = chrono::DateTime::from_timestamp(created_utc as i64, 0)
                                .map(|dt| dt.to_rfc3339())
                                .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());

                            let item_id = parsers::make_item_id(
                                title,
                                &url,
                                &created,
                                &format!("reddit_{}", subreddit),
                            );

                            Some(NewsItem {
                                id: item_id,
                                title: title.to_string(),
                                link: url,
                                pub_date: created,
                                source_name: format!("Reddit r/{}", subreddit),
                                pool_id: "REDDIT".to_string(),
                                content_snippet: format!("Score: {} | Comments: {} | {}", score, num_comments,
                                    selftext.chars().take(300).collect::<String>()),
                                author: author.map(|a| a.to_string()),
                                media_url: None,
                            })
                        }).collect()
                    })
                    .unwrap_or_default();

                Ok(Json(RedditSearchOutput {
                    count: posts.len(),
                    posts,
                    meta: RedditSearchMeta {
                        query: input.query,
                        subreddits: input.subreddits,
                        sort: sort.to_string(),
                        time: time.to_string(),
                    },
                }))
            }
            Err(e) => Err(format!("Reddit search failed: {}", e)),
        }
    }

    // ── Research Tools ──────────────────────────────────────────

    #[tool(name = "research.search", description = "Search academic papers across arXiv and Semantic Scholar")]
    pub async fn research_search(
        &self,
        params: Parameters<ResearchSearchInput>,
    ) -> Result<Json<ResearchSearchOutput>, String> {
        let input = params.0;
        let sources = input.sources.unwrap_or_else(|| vec!["arxiv".into(), "semanticscholar".into()]);
        let limit = input.limit.unwrap_or(25).min(100).max(1);
        let query_enc = urlencoding(&input.query);

        let settings = config::load_settings().await.map_err(|e| format!("Settings: {}", e))?;
        let cache_dir = http_mod::resolve_cache_dir(&settings, &config::user_config_dir());
        let http = HttpClient::new(&settings.http, &cache_dir);

        let mut all_papers: Vec<ResearchPaper> = Vec::new();
        let mut total = 0usize;

        // Search arXiv
        if sources.contains(&"arxiv".to_string()) {
            let cat_filter = input.categories.as_ref()
                .map(|cats| cats.iter().map(|c| format!("cat:{}", c)).collect::<Vec<_>>().join("+OR+"))
                .unwrap_or_default();
            let arxiv_query = if cat_filter.is_empty() {
                format!("search_query=all:{}&start=0&max_results={}", query_enc, limit)
            } else {
                format!("search_query=(all:{})+AND+({})&start=0&max_results={}", query_enc, cat_filter, limit)
            };
            let arxiv_url = format!("http://export.arxiv.org/api/query?{}", arxiv_query);

            match http.fetch(&arxiv_url, None, "bypass").await {
                Ok(outcome) => {
                    if let http_mod::FetchOutcome::Response(resp, _, _) = outcome {
                        let body = resp.body_text;
                        // Parse arXiv Atom feed
                        if let Ok(feed) = feed_rs::parser::parse(body.as_bytes()) {
                            for entry in &feed.entries {
                                let arxiv_id = entry.id.trim_start_matches("http://arxiv.org/abs/").to_string();
                                let pdf_url = format!("https://arxiv.org/pdf/{}.pdf", arxiv_id);
                                let title = entry.title.as_ref().map(|t| t.content.clone()).unwrap_or_default();
                                let abstract_text = entry.summary.as_ref().map(|s| s.content.clone()).unwrap_or_default();
                                let authors: Vec<String> = entry.authors.iter()
                                    .map(|a| a.name.clone())
                                    .collect();
                                let year = entry.published.map(|d| d.year() as i32);

                                all_papers.push(ResearchPaper {
                                    id: format!("arxiv:{}", arxiv_id),
                                    title: title.clone(),
                                    authors: authors.clone(),
                                    abstract_text: abstract_text.clone(),
                                    year,
                                    citation_count: None,
                                    pdf_url: Some(pdf_url),
                                    source: "arXiv".into(),
                                    link: Some(entry.links.first().map(|l| l.href.clone()).unwrap_or_default()),
                                });
                            }
                        }

                        // Count total results from atom:totalResults
                        if let Some(total_str) = body.split("<opensearch:totalResults").nth(1)
                            .and_then(|s| s.split('>').nth(1))
                            .and_then(|s| s.split('<').next())
                        {
                            total += total_str.parse::<usize>().unwrap_or(0);
                        } else {
                            total += all_papers.len();
                        }
                    }
                }
                Err(e) => tracing::warn!("arXiv search failed: {}", e),
            }
        }

        // Search Semantic Scholar
        if sources.contains(&"semanticscholar".to_string()) {
            let ss_query = format!(
                "https://api.semanticscholar.org/graph/v1/paper/search?query={}&limit={}&fields=title,authors,abstract,year,citationCount,openAccessPdf,externalIds",
                query_enc, limit.min(100)
            );
            match http.fetch(&ss_query, None, "bypass").await {
                Ok(outcome) => {
                    if let http_mod::FetchOutcome::Response(resp, _, _) = outcome {
                        if let Ok(json) = serde_json::from_str::<serde_json::Value>(&resp.body_text) {
                            if let Some(papers) = json["data"].as_array() {
                                for paper in papers {
                                    let paper_id = paper["paperId"].as_str().unwrap_or("");
                                    let title = paper["title"].as_str().unwrap_or("");
                                    let abstract_text = paper["abstract"].as_str().unwrap_or("");
                                    let year = paper["year"].as_i64();
                                    let citations = paper["citationCount"].as_i64();
                                    let pdf_url = paper["openAccessPdf"]["url"].as_str().map(|s| s.to_string());
                                    let authors: Vec<String> = paper["authors"]
                                        .as_array()
                                        .map(|a| {
                                            a.iter()
                                                .filter_map(|author| author["name"].as_str().map(|n| n.to_string()))
                                                .collect()
                                        })
                                        .unwrap_or_default();

                                    all_papers.push(ResearchPaper {
                                        id: format!("semanticscholar:{}", paper_id),
                                        title: title.to_string(),
                                        authors,
                                        abstract_text: abstract_text.to_string(),
                                        year: year.map(|y| y as i32),
                                        citation_count: citations.map(|c| c as i32),
                                        pdf_url,
                                        source: "Semantic Scholar".into(),
                                        link: Some(format!("https://api.semanticscholar.org/{}/{}", paper_id, "CorpusId")),
                                    });
                                }
                            }
                            total += json["total"].as_i64().unwrap_or(0) as usize;
                        }
                    }
                }
                Err(e) => tracing::warn!("Semantic Scholar search failed: {}", e),
            }
        }

        // Sort by year descending, limit
        all_papers.sort_by(|a, b| b.year.unwrap_or(0).cmp(&a.year.unwrap_or(0)));
        all_papers.truncate(limit as usize);

        let count = all_papers.len();
        Ok(Json(ResearchSearchOutput {
            papers: all_papers,
            count,
            total,
            meta: ResearchSearchMeta {
                query: input.query,
                sources,
            },
        }))
    }

    #[tool(name = "research.paper", description = "Get detailed information about a specific paper by ID")]
    pub async fn research_paper(
        &self,
        params: Parameters<ResearchPaperInput>,
    ) -> Result<Json<ResearchPaperOutput>, String> {
        let input = params.0;
        let settings = config::load_settings().await.map_err(|e| format!("Settings: {}", e))?;
        let cache_dir = http_mod::resolve_cache_dir(&settings, &config::user_config_dir());
        let http = HttpClient::new(&settings.http, &cache_dir);

        let paper_id = &input.paper_id;
        let (title, authors, abstract_text, year, citations, references, pdf_url, _content) =
            if paper_id.starts_with("arxiv:") || !paper_id.contains(':') {
                let id = paper_id.trim_start_matches("arxiv:");
                let url = format!("http://export.arxiv.org/api/query?id_list={}", id);
                match http.fetch(&url, None, "bypass").await {
                    Ok(outcome) => {
                        if let http_mod::FetchOutcome::Response(resp, _, _) = outcome {
                            if let Ok(feed) = feed_rs::parser::parse(resp.body_text.as_bytes()) {
                                if let Some(entry) = feed.entries.first() {
                                    let t = entry.title.as_ref().map(|t| t.content.clone()).unwrap_or_default();
                                    let abs = entry.summary.as_ref().map(|s| s.content.clone()).unwrap_or_default();
                                    let auths: Vec<String> = entry.authors.iter().map(|a| a.name.clone()).collect();
                                    let yr = entry.published.map(|d| d.year() as i32);
                                    (t, auths, abs, yr, None::<i32>, None::<i32>, Some(format!("https://arxiv.org/pdf/{}.pdf", id)), None::<String>)
                                } else {
                                    return Err("Paper not found".into());
                                }
                            } else {
                                return Err("Failed to parse arXiv response".into());
                            }
                        } else {
                            return Err("Cached response for paper fetch".into());
                        }
                    }
                    Err(e) => return Err(format!("arXiv fetch failed: {}", e)),
                }
            } else if paper_id.starts_with("semanticscholar:") {
                let id = paper_id.trim_start_matches("semanticscholar:");
                let url = format!(
                    "https://api.semanticscholar.org/graph/v1/paper/{}?fields=title,authors,abstract,year,citationCount,referenceCount,openAccessPdf",
                    id
                );
                match http.fetch(&url, None, "bypass").await {
                    Ok(outcome) => {
                        if let http_mod::FetchOutcome::Response(resp, _, _) = outcome {
                            if let Ok(json) = serde_json::from_str::<serde_json::Value>(&resp.body_text) {
                                let t = json["title"].as_str().unwrap_or("").to_string();
                                let abs = json["abstract"].as_str().unwrap_or("").to_string();
                                let auths: Vec<String> = json["authors"]
                                    .as_array()
                                    .map(|a| a.iter().filter_map(|author| author["name"].as_str().map(|n| n.to_string())).collect())
                                    .unwrap_or_default();
                                let yr = json["year"].as_i64().map(|y| y as i32);
                                let cites = json["citationCount"].as_i64().map(|c| c as i32);
                                let refs = json["referenceCount"].as_i64().map(|r| r as i32);
                                let pdf = json["openAccessPdf"]["url"].as_str().map(|s| s.to_string());
                                (t, auths, abs, yr, cites, refs, pdf, None)
                            } else {
                                return Err("Failed to parse Semantic Scholar response".into());
                            }
                        } else {
                            return Err("Cached response for paper fetch".into());
                        }
                    }
                    Err(e) => return Err(format!("Semantic Scholar fetch failed: {}", e)),
                }
            } else {
                return Err("Unknown paper ID format. Use arxiv:XXXX.XXXXX or semanticscholar:XXXX".into());
            };

        // Optionally extract PDF content
        let content = if input.extract_pdf.unwrap_or(false) {
            if let Some(pdf_url_val) = &pdf_url {
                match http.fetch(pdf_url_val, None, "bypass").await {
                    Ok(outcome) => {
                        if let http_mod::FetchOutcome::Response(_resp, _, _) = outcome {
                            Some(format!("PDF available at {}. Direct content extraction requires pdf-extractor crate.", pdf_url_val))
                        } else { None }
                    }
                    Err(_) => None,
                }
            } else { None }
        } else { None };

        Ok(Json(ResearchPaperOutput {
            paper: PaperDetail {
                id: paper_id.clone(),
                title,
                authors,
                abstract_text,
                year,
                citations,
                references,
                pdf_url,
                content,
            },
        }))
    }

    #[tool(name = "research.download", description = "Download a research paper PDF")]
    pub async fn research_download(
        &self,
        params: Parameters<ResearchDownloadInput>,
    ) -> Result<Json<ResearchDownloadOutput>, String> {
        let _ = params.0;
        Err("Download not implemented in Rust version. Use the TypeScript version for PDF downloads.".into())
    }

    // ── Web Tools ───────────────────────────────────────────────

    #[tool(name = "web.search", description = "Search the web in realtime. Uses Tavily or Firecrawl API.")]
    pub async fn web_search(
        &self,
        params: Parameters<WebSearchInput>,
    ) -> Result<Json<WebSearchOutput>, String> {
        let input = params.0;
        let settings = config::load_settings().await.map_err(|e| format!("Settings: {}", e))?;
        let provider = input.provider.as_deref().unwrap_or("auto");

        // Try Tavily first
        if provider == "auto" || provider == "tavily" {
            if let Some(ref tavily) = settings.tavily {
                if tavily.enabled {
                    if let Some(ref api_key) = tavily.api_key {
                        let query_enc = urlencoding(&input.query);
                        let url = format!(
                            "https://api.tavily.com/search?api_key={}&query={}&max_results={}&topic={}",
                            api_key, query_enc,
                            input.max_results.unwrap_or(10),
                            input.topic.as_deref().unwrap_or("general")
                        );
                        let cache_dir = http_mod::resolve_cache_dir(&settings, &config::user_config_dir());
                        let http = HttpClient::new(&settings.http, &cache_dir);
                        match http.fetch(&url, None, "bypass").await {
                            Ok(outcome) => {
                                if let http_mod::FetchOutcome::Response(resp, _, _) = outcome {
                                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(&resp.body_text) {
                                        let results = json["results"].as_array().cloned().unwrap_or_default();
                                        let answer = json["answer"].as_str().map(|s| s.to_string());
                                        return Ok(Json(WebSearchOutput {
                                            count: results.len(),
                                            results,
                                            answer,
                                            meta: WebSearchMeta {
                                                provider: "tavily".into(),
                                                query: input.query,
                                            },
                                        }));
                                    }
                                }
                            }
                            Err(e) => {
                                if provider == "tavily" {
                                    return Err(format!("Tavily search failed: {}", e));
                                }
                                tracing::warn!("Tavily search failed, trying Firecrawl: {}", e);
                            }
                        }
                    }
                }
            }
        }

        // Fallback to Firecrawl
        if let Some(ref firecrawl) = settings.firecrawl {
            if firecrawl.enabled {
                if let Some(ref api_key) = firecrawl.api_key {
                    let query_enc = urlencoding(&input.query);
                    let url = format!(
                        "https://api.firecrawl.dev/v1/search?query={}&limit={}",
                        query_enc, input.max_results.unwrap_or(10)
                    );
                    let cache_dir = http_mod::resolve_cache_dir(&settings, &config::user_config_dir());
                    let http = HttpClient::new(&settings.http, &cache_dir);
                    match http.fetch(&url, Some(&HashMap::from([("Authorization".into(), format!("Bearer {}", api_key))])), "bypass").await {
                        Ok(outcome) => {
                            if let http_mod::FetchOutcome::Response(resp, _, _) = outcome {
                                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&resp.body_text) {
                                    let results = json["data"]["web"].as_array().cloned().unwrap_or_default();
                                    return Ok(Json(WebSearchOutput {
                                        count: results.len(),
                                        results,
                                        answer: None,
                                        meta: WebSearchMeta {
                                            provider: "firecrawl".into(),
                                            query: input.query,
                                        },
                                    }));
                                }
                            }
                        }
                        Err(e) => { tracing::warn!("Firecrawl search failed: {}", e); }
                    }
                }
            }
        }

        Err("No web search provider available. Configure Tavily or Firecrawl in settings.yml.".into())
    }

    #[tool(name = "web.scrape", description = "Scrape content from a URL. Uses Firecrawl or Tavily.")]
    pub async fn web_scrape(
        &self,
        params: Parameters<WebScrapeInput>,
    ) -> Result<Json<WebScrapeOutput>, String> {
        let input = params.0;
        let settings = config::load_settings().await.map_err(|e| format!("Settings: {}", e))?;
        let cache_dir = http_mod::resolve_cache_dir(&settings, &config::user_config_dir());
        let http = HttpClient::new(&settings.http, &cache_dir);

        // Simple HTML scraping fallback
        match http.fetch(&input.url, None, "bypass").await {
            Ok(outcome) => {
                if let http_mod::FetchOutcome::Response(resp, _, _) = outcome {
                    let body = resp.body_text;
                    // Extract title and main content via scraper
                    let doc = scraper::Html::parse_document(&body);
                    let title = scraper::Selector::parse("title")
                        .ok()
                        .and_then(|sel| doc.select(&sel).next())
                        .map(|el| el.text().collect::<String>())
                        .unwrap_or_default();
                    let main_content: String = doc.root_element().text().collect::<String>();
                    let main_content = main_content
                        .split_whitespace()
                        .take(2000)
                        .collect::<Vec<_>>()
                        .join(" ");

                    Ok(Json(WebScrapeOutput {
                        success: true,
                        url: input.url,
                        markdown: Some(main_content),
                        meta: serde_json::json!({
                            "provider": "direct-html",
                            "title": title,
                            "formats": input.formats.unwrap_or_else(|| vec!["markdown".into()]),
                        }),
                    }))
                } else {
                    Err("Cached response for scrape URL".into())
                }
            }
            Err(e) => Err(format!("Scrape failed: {}", e)),
        }
    }

    #[tool(name = "web.crawl", description = "Crawl a website systematically. Not yet implemented in Rust version.")]
    pub async fn web_crawl(&self) -> Result<String, String> {
        Err("Crawl not yet implemented in Rust version. Use the web.scrape tool for single URLs or web.map for site discovery.".into())
    }

    #[tool(name = "web.map", description = "Discover URLs on a website by analyzing sitemap and links.")]
    pub async fn web_map(
        &self,
        params: Parameters<WebMapInput>,
    ) -> Result<Json<WebMapOutput>, String> {
        let input = params.0;
        let settings = config::load_settings().await.map_err(|e| format!("Settings: {}", e))?;
        let cache_dir = http_mod::resolve_cache_dir(&settings, &config::user_config_dir());
        let http = HttpClient::new(&settings.http, &cache_dir);

        let base_url = input.url.trim_end_matches('/');
        let sitemap_url = format!("{}/sitemap.xml", base_url);

        let mut links = Vec::new();
        // Try sitemap.xml
        if let Ok(outcome) = http.fetch(&sitemap_url, None, "bypass").await {
            if let http_mod::FetchOutcome::Response(resp, _, _) = outcome {
                let doc = scraper::Html::parse_document(&resp.body_text);
                // Try to extract <loc> elements from sitemap XML
                for line in resp.body_text.lines() {
                    if line.contains("<loc>") {
                        if let Some(start) = line.find("<loc>") {
                            let rest = &line[start + 5..];
                            if let Some(end) = rest.find("</loc>") {
                                let url = &rest[..end];
                                links.push(serde_json::json!({"url": url, "title": null}));
                            }
                        }
                    }
                }
                // Also get <url> elements
                if let Ok(sel) = scraper::Selector::parse("url") {
                    for el in doc.select(&sel) {
                        if let Ok(loc_sel) = scraper::Selector::parse("loc") {
                            if let Some(loc) = el.select(&loc_sel).next() {
                                let url_str = loc.text().collect::<String>().trim().to_string();
                                if !url_str.is_empty() && !links.iter().any(|l| l["url"] == url_str) {
                                    let title = scraper::Selector::parse("news\\:title")
                                        .or_else(|_| scraper::Selector::parse("title"))
                                        .ok()
                                        .and_then(|ts| el.select(&ts).next())
                                        .map(|t| serde_json::Value::String(t.text().collect::<String>()));
                                    links.push(serde_json::json!({"url": url_str, "title": title}));
                                }
                            }
                        }
                    }
                }
            }
        }

        let limit = input.limit.unwrap_or(100) as usize;
        links.truncate(limit);

        Ok(Json(WebMapOutput {
            success: true,
            url: input.url,
            count: links.len(),
            links,
            meta: serde_json::json!({
                "provider": "sitemap-parser",
                "limit": limit,
            }),
        }))
    }

    // ── Insight Tools ───────────────────────────────────────────

    #[tool(name = "insights.findConnections", description = "Find articles mentioning the same entity across different domains")]
    pub async fn insights_find_connections(
        &self,
        params: Parameters<InsightConnectionInput>,
    ) -> Result<Json<InsightConnectionOutput>, String> {
        let input = params.0;
        let storage = self.insights.lock().await;
        let connections = storage.find_inter_domain_connections(
            &input.entity,
            input.min_domains.unwrap_or(2) as usize,
        );
        let count = connections.len();
        Ok(Json(InsightConnectionOutput { connections, count }))
    }

    #[tool(name = "insights.findAllConnections", description = "Discover all entities that appear across multiple domains")]
    pub async fn insights_find_all_connections(
        &self,
        params: Parameters<InsightAllConnectionsInput>,
    ) -> Result<Json<InsightAllConnectionsOutput>, String> {
        let input = params.0;
        let storage = self.insights.lock().await;
        let all = storage.find_all_inter_domain_connections(input.min_domains.unwrap_or(2) as usize);
        let total_found = all.len();
        let limit = input.limit.unwrap_or(20) as usize;
        let connections: Vec<EntityConnection> = all.into_iter().take(limit).collect();
        let stats = storage.stats();
        Ok(Json(InsightAllConnectionsOutput { connections, total_found, stats }))
    }

    #[tool(name = "insights.trendingEntities", description = "Detect entities with increasing mention frequency")]
    pub async fn insights_trending(
        &self,
        params: Parameters<InsightTrendingInput>,
    ) -> Result<Json<InsightTrendingOutput>, String> {
        let input = params.0;
        let storage = self.insights.lock().await;
        let window_ms = input.time_window_hours.unwrap_or(24) * 3_600_000;
        let trending = storage.detect_trending(
            window_ms,
            input.min_growth.unwrap_or(2.0),
            input.min_current_mentions.unwrap_or(3),
        );
        let count = trending.len();
        let stats = storage.stats();
        Ok(Json(InsightTrendingOutput { trending, count, stats }))
    }

    #[tool(name = "insights.indexArticles", description = "Add articles to the insight engine for cross-article analysis")]
    pub async fn insights_index(
        &self,
        params: Parameters<InsightIndexInput>,
    ) -> Result<Json<InsightIndexOutput>, String> {
        let input = params.0;
        let mut storage = self.insights.lock().await;
        let mut indexed = 0usize;

        for article in &input.articles {
            storage.add_article(ArticleInsight {
                id: article.id.clone(),
                title: article.title.clone(),
                pub_date: article.pub_date.clone(),
                source_name: article.source_name.clone(),
                domains: article.domains.clone().unwrap_or_default(),
                entities: article.entities.clone().unwrap_or_default(),
            });
            indexed += 1;
        }

        let stats = storage.stats();
        Ok(Json(InsightIndexOutput { indexed, stats }))
    }

    #[tool(name = "insights.getStats", description = "Get statistics about indexed articles")]
    pub async fn insights_stats(&self) -> Result<Json<InsightStatsOutput>, String> {
        let storage = self.insights.lock().await;
        let stats = storage.stats();
        Ok(Json(InsightStatsOutput { stats }))
    }

    #[tool(name = "insights.clearIndex", description = "Clear all indexed articles from the insight engine")]
    pub async fn insights_clear(&self) -> Result<Json<InsightClearOutput>, String> {
        let mut storage = self.insights.lock().await;
        storage.clear();
        Ok(Json(InsightClearOutput { cleared: true }))
    }
}

// ─── ServerHandler Implementation ────────────────────────────────

#[tool_handler(router = self.tool_router)]
impl rmcp::ServerHandler for IgsMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .build(),
        )
        .with_server_info(Implementation::from_build_env())
        .with_protocol_version(ProtocolVersion::V_2024_11_05)
        .with_instructions(
            "IGS MCP Server (Rust) — Intelligence Gathering System. \
            Fetch news from 200+ RSS/HTTP sources, search Reddit, academic papers, \
            and the web. TOON-format optimized output for AI agent consumption."
                .to_string(),
        )
    }
}

// ─── Helper Functions ───────────────────────────────────────────

/// URL-encode a string
fn urlencoding(s: &str) -> String {
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

/// Basic topic extraction via word frequency
fn extract_topics(text: &str, max: usize) -> Vec<String> {
    let lower = text.to_lowercase();
    let stop_words = [
        "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
        "of", "by", "with", "from", "is", "are", "was", "were", "be", "been",
        "being", "have", "has", "had", "do", "does", "did", "will", "would",
        "could", "should", "may", "might", "shall", "can", "need", "dare",
        "it", "its", "it's", "this", "that", "these", "those", "i", "you",
        "he", "she", "we", "they", "me", "him", "her", "us", "them", "my",
        "your", "his", "its", "our", "their", "not", "no", "nor", "so", "up",
        "down", "out", "off", "over", "under", "again", "further", "then",
        "once", "here", "there", "when", "where", "why", "how", "all", "each",
        "every", "both", "few", "more", "most", "other", "some", "such", "only",
        "own", "same", "than", "too", "very", "just", "also", "about",
    ];

    let words: Vec<&str> = lower
        .split(|c: char| !c.is_alphanumeric())
        .filter(|w| w.len() > 3 && !stop_words.contains(w))
        .collect();

    let mut freq: HashMap<&str, usize> = HashMap::new();
    for w in &words {
        *freq.entry(w).or_default() += 1;
    }

    let mut topics: Vec<(&str, usize)> = freq.into_iter().collect();
    topics.sort_by(|a, b| b.1.cmp(&a.1));
    topics.into_iter().take(max).map(|(w, _)| w.to_string()).collect()
}

/// Basic entity extraction
fn extract_basic_entities(text: &str) -> Vec<serde_json::Value> {
    let mut entities = Vec::new();

    // Extract words that start with uppercase (potential named entities)
    let words: Vec<&str> = text.split_whitespace().collect();
    let mut i = 0;
    while i < words.len() {
        let w = words[i].trim_matches(|c: char| !c.is_alphanumeric());
        if w.len() >= 2 && w.chars().next().map_or(false, |c| c.is_uppercase())
            && !w.chars().all(|c| c.is_uppercase())
        {
            // Multi-word names
            let mut name = w.to_string();
            while i + 1 < words.len() {
                let next = words[i + 1].trim_matches(|c: char| !c.is_alphanumeric());
                if next.len() >= 2 && next.chars().next().map_or(false, |c| c.is_uppercase()) {
                    name.push(' ');
                    name.push_str(next);
                    i += 1;
                } else {
                    break;
                }
            }
            // Deduplicate
            if !entities.iter().any(|e: &serde_json::Value| e["name"] == name) {
                let entity_type = if name.contains(' ') { "Person" } else { "Organization" };
                entities.push(serde_json::json!({
                    "name": name,
                    "type": entity_type,
                    "mentions": 1,
                    "confidence": 0.5,
                }));
            }
        }
        i += 1;
    }

    entities
}

/// Basic sentiment analysis
fn basic_sentiment(text: &str) -> serde_json::Value {
    let positive_words = [
        "good", "great", "excellent", "amazing", "wonderful", "fantastic", "outstanding",
        "positive", "success", "successful", "growth", "breakthrough", "opportunity",
        "progress", "innovation", "achievement", "benefit", "improve", "improvement",
        "strong", "profit", "gain", "boost", "surge", "rally", "hope", "optimistic",
        "bright", "promising", "remarkable", "impressive", "best", "better", "win",
        "victory", "celebration", "happy", "love", "beautiful", "exciting", "thrilled",
    ];
    let negative_words = [
        "bad", "terrible", "awful", "horrible", "worst", "poor", "negative", "failure",
        "fail", "crisis", "disaster", "decline", "drop", "loss", "lose", "damage",
        "damage", "threat", "risk", "danger", "dangerous", "war", "conflict", "attack",
        "crime", "illegal", "corruption", "scandal", "fraud", "banned", "restrict",
        "regret", "sad", "angry", "furious", "tragic", "deadly", "fatal", "kill",
        "death", "destroy", "destruction", "hate", "wrong", "ugly", "harsh",
    ];

    let lower = text.to_lowercase();
    let words: Vec<&str> = lower.split_whitespace().collect();

    let pos_count = words.iter().filter(|w| positive_words.contains(w)).count();
    let neg_count = words.iter().filter(|w| negative_words.contains(w)).count();

    let score = (pos_count as f64) - (neg_count as f64);
    let total = words.len() as f64;
    let comparative = if total > 0.0 { score / total } else { 0.0 };
    let label = if score > 0.0 { "positive" } else if score < 0.0 { "negative" } else { "neutral" };

    serde_json::json!({
        "score": score,
        "comparative": comparative,
        "label": label,
    })
}

/// Sync helper: find RSS/Atom feed link in HTML body
fn find_feed_url(body: &str, base_url: &str) -> Option<String> {
    let doc = scraper::Html::parse_document(body);
    if let Ok(sel) = scraper::Selector::parse("link[rel='alternate'][type*='rss'], link[rel='alternate'][type*='atom']") {
        for el in doc.select(&sel) {
            if let Some(href) = el.attr("href") {
                let abs = url::Url::parse(base_url).ok().and_then(|base| {
                    url::Url::parse(href).ok().or_else(|| base.join(href).ok())
                }).map(|u| u.to_string());
                if abs.is_some() {
                    return abs;
                }
            }
        }
    }
    None
}

/// TOON-format encode a serializable value for AI-agent token-efficiency
pub fn toon_encode<T: serde::Serialize>(value: &T) -> String {
    // Try TOON format first, fallback to JSON
    toon_format::encode_default(value).unwrap_or_else(|_| serde_json::to_string(value).unwrap_or_default())
}

/// Format news items in TOON for token-efficient AI consumption
pub fn toon_format_news(items: &[NewsItem], meta: &NewsFetchMeta) -> String {
    let mut out = format!("> IGS MCP News · {} items · keyword: {:?}\n", meta.count, meta.keywords);
    for item in items.iter().take(50) {
        out.push_str(&format!("\n• {} [{:>12}] {} | {}",
            &item.id[..12.min(item.id.len())],
            item.source_name.chars().take(12).collect::<String>(),
            item.title.chars().take(100).collect::<String>(),
            item.link,
        ));
    }
    out
}

/// Format research papers in TOON for token-efficient AI consumption
pub fn toon_format_papers(papers: &[ResearchPaper]) -> String {
    let mut out = format!("> IGS MCP Research · {} papers\n", papers.len());
    for paper in papers.iter().take(20) {
        let yr = paper.year.map(|y| y.to_string()).unwrap_or_else(|| "????".to_string());
        out.push_str(&format!("\n- {} [{}] {} | {}",
            &paper.id[..12.min(paper.id.len())],
            yr,
            paper.title.chars().take(100).collect::<String>(),
            paper.authors.first().map(|a| a.as_str()).unwrap_or("Unknown"),
        ));
    }
    out
}
