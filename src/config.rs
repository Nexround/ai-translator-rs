use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

pub const DEFAULT_SYSTEM_PROMPT: &str =
    "你是一个专业翻译助手。请将以下文本翻译为{target_lang}，\
     自动识别源语言，只输出翻译结果，不要添加任何解释。";

pub const TARGET_LANGUAGES: &[&str] = &[
    "中文", "英文", "日文", "韩文", "法文", "德文",
    "西班牙文", "俄文", "葡萄牙文", "意大利文", "阿拉伯文", "泰文", "越南文",
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default)]
    pub api_key: String,
    #[serde(default = "default_base_url")]
    pub base_url: String,
    #[serde(default = "default_model")]
    pub model: String,
    #[serde(default = "default_system_prompt")]
    pub system_prompt: String,
    #[serde(default = "default_target_lang")]
    pub target_lang: String,
}

fn default_base_url() -> String {
    "https://api.openai.com/v1".to_string()
}
fn default_model() -> String {
    "gpt-4o-mini".to_string()
}
fn default_system_prompt() -> String {
    DEFAULT_SYSTEM_PROMPT.to_string()
}
fn default_target_lang() -> String {
    "英文".to_string()
}

impl Default for Config {
    fn default() -> Self {
        Self {
            api_key: String::new(),
            base_url: default_base_url(),
            model: default_model(),
            system_prompt: default_system_prompt(),
            target_lang: default_target_lang(),
        }
    }
}

fn config_path() -> PathBuf {
    let mut p = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
    p.push(".translator");
    p.push("config.json");
    p
}

impl Config {
    pub fn load() -> Self {
        let path = config_path();
        if path.exists() {
            if let Ok(text) = fs::read_to_string(&path) {
                if let Ok(cfg) = serde_json::from_str::<Config>(&text) {
                    return cfg;
                }
            }
        }
        Config::default()
    }

    pub fn save(&self) {
        let path = config_path();
        if let Some(parent) = path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        if let Ok(text) = serde_json::to_string_pretty(self) {
            let _ = fs::write(path, text);
        }
    }

    pub fn build_prompt(&self) -> String {
        self.system_prompt
            .replace("{target_lang}", &self.target_lang)
    }
}
