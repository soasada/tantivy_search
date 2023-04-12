use std::sync::Arc;
use std::time::Duration;

use tantivy::{Directory, Document, Index, IndexSettings, IndexWriter, TantivyError, Term};
use tantivy::schema::Schema;
use tantivy::tokenizer::{AsciiFoldingFilter, Language, LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer, StopWordFilter, TextAnalyzer};
use tokio::sync::{mpsc, RwLock};
use tokio::sync::mpsc::Sender;

use crate::AppEnv;

pub struct IndexActor {
    pub index: Index,
    pub schema: Schema,
    receiver: mpsc::Receiver<IndexActorMessage>,
    pub writer: Arc<RwLock<IndexWriter>>,
    pub must_reindex: bool,
}

#[derive(Debug)]
pub enum IndexActorMessage {
    Single { doc: Document },
    Commit { index_name: String },
    Delete { id: String },
    Reindex { backend_env: AppEnv, index_name: String },
}

pub async fn run_index_actor(mut actor: IndexActor) {
    while let Some(msg) = actor.receiver.recv().await {
        if let Err(e) = actor.handle_message(msg).await {
            tracing::error!("error while handling message in index actor: {:?}", e);
        }
    }
}

pub async fn run_commit_index(sender: Sender<IndexActorMessage>, index_name: String) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));

    loop {
        interval.tick().await;
        sender
            .send(IndexActorMessage::Commit { index_name: index_name.clone() })
            .await
            .unwrap_or_else(|_| panic!("{} index actor has been killed", index_name.clone()));
    }
}

impl IndexActor {
    pub fn new(dir: impl Directory, schema: Schema, receiver: mpsc::Receiver<IndexActorMessage>) -> Result<Self, TantivyError> {
        let dir: Box<dyn Directory> = Box::new(dir);
        let mut must_reindex = false;
        let index = match Index::open_or_create(dir.clone(), schema.clone()) {
            Ok(i) => i,
            Err(e) => match e {
                TantivyError::SchemaError(_) => {
                    tracing::warn!("schema changed, erasing actual index and marking must_reindex flag");
                    must_reindex = true;
                    Index::create(dir.clone(), schema.clone(), IndexSettings::default())?
                }
                err => panic!("{:?}", err)
            }
        };

        index.tokenizers()
            .register("ngram2", es_ngram2_analyzer());

        // Should only be one writer at a time. This single IndexWriter is already
        // multithreaded.
        let writer = index.writer(50_000_000)?;

        Ok(IndexActor {
            index,
            schema,
            receiver,
            writer: Arc::new(RwLock::new(writer)),
            must_reindex,
        })
    }

    async fn handle_message(&mut self, msg: IndexActorMessage) -> Result<(), TantivyError> {
        match msg {
            IndexActorMessage::Single { doc } => {
                if let Some(id_field) = self.schema.get_field("id") {
                    if let Some(id_value) = doc.get_first(id_field) {
                        if let Some(id) = id_value.as_text() {
                            let str_id = String::from(id);
                            let id_term = Term::from_field_text(id_field, id);
                            let writer = Arc::clone(&self.writer);

                            tokio::task::spawn_blocking(move || {
                                let r_writer = writer.blocking_read();
                                r_writer.delete_term(id_term);

                                if let Err(e) = r_writer.add_document(doc) {
                                    tracing::error!("error adding single document to index: {:?}", e);
                                }

                                tracing::info!("document {} successfully indexed", str_id);
                            });

                            Ok(())
                        } else {
                            Err(TantivyError::FieldNotFound(String::from("id field value must be a string to index a single document")))
                        }
                    } else {
                        Err(TantivyError::FieldNotFound(String::from("no id field found in single document while indexing")))
                    }
                } else {
                    Err(TantivyError::FieldNotFound(String::from("no id field found in schema while indexing single document")))
                }
            }
            IndexActorMessage::Commit { index_name } => {
                let writer = Arc::clone(&self.writer);

                tokio::task::spawn_blocking(move || {
                    let mut w_writer = writer.blocking_write();
                    if let Ok(opstamp) = w_writer.commit() {
                        tracing::info!("{index_name} documents committed successfully with opstamp: {opstamp}");
                    }
                });

                Ok(())
            }
            IndexActorMessage::Delete { id } => {
                if let Some(id_field) = self.schema.get_field("id") {
                    let writer = Arc::clone(&self.writer);

                    tokio::task::spawn_blocking(move || {
                        let id_term = Term::from_field_text(id_field, id.as_str());
                        let r_writer = writer.blocking_read();

                        r_writer.delete_term(id_term);

                        tracing::info!("document {} successfully deleted", id);
                    });

                    Ok(())
                } else {
                    Err(TantivyError::FieldNotFound(format!("{} no id field found in schema while deleting document", id)))
                }
            }
            IndexActorMessage::Reindex { backend_env, index_name } => {
                let mut go_backend_url = format!("http://localhost:8080/reindex/{}", index_name.as_str());
                if backend_env.is_prod() {
                    go_backend_url = format!("http://app:8080/reindex/{}", index_name.as_str());
                }

                match reqwest::get(go_backend_url).await {
                    Ok(r) => {
                        if r.status().is_success() {
                            self.must_reindex = false;
                            tracing::info!("reindex triggered successfully");
                            Ok(())
                        } else {
                            Err(TantivyError::SystemError(format!("{} HTTP error while reindexing", r.status())))
                        }
                    }
                    Err(e) => Err(TantivyError::SystemError(format!("{:?}", e)))
                }
            }
        }
    }
}

fn es_ngram2_analyzer() -> TextAnalyzer {
    TextAnalyzer::from(SimpleTokenizer)
        .filter(RemoveLongFilter::limit(40))
        .filter(LowerCaser)
        .filter(AsciiFoldingFilter) // remove accents
        .filter(StopWordFilter::new(Language::Spanish).unwrap())
        .filter(Stemmer::new(Language::Spanish))
}