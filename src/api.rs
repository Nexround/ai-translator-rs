use futures::StreamExt;
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Clone)]
pub enum TranslateEvent {
    Chunk(String),
    Done,
    Error(String),
}

#[derive(Deserialize)]
struct ChatChunk {
    choices: Vec<ChunkChoice>,
}

#[derive(Deserialize)]
struct ChunkChoice {
    delta: Delta,
    finish_reason: Option<String>,
}

#[derive(Deserialize)]
struct Delta {
    content: Option<String>,
}

pub async fn translate_stream(
    api_key: String,
    base_url: String,
    model: String,
    prompt: String,
    text: String,
    mut cancel_rx: tokio::sync::oneshot::Receiver<()>,
    event_tx: futures::channel::mpsc::Sender<TranslateEvent>,
) {
    let client = Client::builder()
        .timeout(Duration::from_secs(60))
        .build()
        .unwrap();

    let body = serde_json::json!({
        "model": model,
        "stream": true,
        "messages": [
            { "role": "user", "content": format!("{}\n\n{}", prompt, text) }
        ]
    });

    let url = format!("{}/chat/completions", base_url.trim_end_matches('/'));

    let response = match client
        .post(&url)
        .header("Authorization", format!("Bearer {}", api_key))
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            let msg = if e.is_connect() {
                "无法连接到 API 服务器，请检查网络或 Base URL 设置。".to_string()
            } else if e.is_timeout() {
                "请求超时，请稍后重试。".to_string()
            } else {
                format!("请求异常: {}", e)
            };
            let _ = event_tx.clone().try_send(TranslateEvent::Error(msg));
            return;
        }
    };

    if !response.status().is_success() {
        let status = response.status().as_u16();
        let body_text = response.text().await.unwrap_or_default();
        let msg = format!("API 返回错误 ({}): {}", status, body_text);
        let _ = event_tx.clone().try_send(TranslateEvent::Error(msg));
        return;
    }

    let mut stream = response.bytes_stream();
    let mut tx = event_tx;

    loop {
        tokio::select! {
            _ = &mut cancel_rx => {
                break;
            }
            item = stream.next() => {
                match item {
                    None => {
                        let _ = tx.try_send(TranslateEvent::Done);
                        break;
                    }
                    Some(Err(e)) => {
                        let _ = tx.try_send(TranslateEvent::Error(format!("请求异常: {}", e)));
                        break;
                    }
                    Some(Ok(bytes)) => {
                        let text = String::from_utf8_lossy(&bytes);
                        for line in text.lines() {
                            let line = line.trim();
                            if line == "data: [DONE]" {
                                let _ = tx.try_send(TranslateEvent::Done);
                                return;
                            }
                            if let Some(json_str) = line.strip_prefix("data: ") {
                                if let Ok(chunk) = serde_json::from_str::<ChatChunk>(json_str) {
                                    for choice in chunk.choices {
                                        if choice.finish_reason.is_some() {
                                            let _ = tx.try_send(TranslateEvent::Done);
                                            return;
                                        }
                                        if let Some(content) = choice.delta.content {
                                            if !content.is_empty() {
                                                let _ = tx.try_send(TranslateEvent::Chunk(content));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

pub async fn test_connection(
    api_key: String,
    base_url: String,
    model: String,
) -> Result<String, String> {
    let client = Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .map_err(|e| format!("构建客户端失败: {}", e))?;

    let body = serde_json::json!({
        "model": model,
        "max_tokens": 5,
        "messages": [{ "role": "user", "content": "Hi" }]
    });

    let url = format!("{}/chat/completions", base_url.trim_end_matches('/'));

    let response = client
        .post(&url)
        .header("Authorization", format!("Bearer {}", api_key))
        .header("Content-Type", "application/json")
        .json(&body)
        .send()
        .await
        .map_err(|e| {
            if e.is_connect() {
                "无法连接到服务器，请检查 Base URL。".to_string()
            } else if e.is_timeout() {
                "连接超时。".to_string()
            } else {
                format!("连接失败: {}", e)
            }
        })?;

    if response.status().is_success() {
        Ok("连接成功！API 配置有效。".to_string())
    } else {
        let status = response.status().as_u16();
        let body_text = response.text().await.unwrap_or_default();
        Err(format!("API 返回错误 ({}): {}", status, body_text))
    }
}
