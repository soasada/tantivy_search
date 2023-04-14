use std::time::Duration;

use tantivy::{Directory, Document, Index, IndexSettings, IndexWriter, TantivyError, Term};
use tantivy::schema::Schema;
use tantivy::tokenizer::{AsciiFoldingFilter, Language, LowerCaser, RemoveLongFilter, SimpleTokenizer, Stemmer, StopWordFilter, TextAnalyzer};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;

use crate::AppEnv;

pub struct IndexActor {
    name: String,
    pub index: Index,
    schema: Schema,
    receiver: mpsc::Receiver<IndexActorMessage>,
    writer: IndexWriter,
    pub must_reindex: bool,
    must_commit: bool,
}

#[derive(Debug)]
pub enum IndexActorMessage {
    Single { doc: Document },
    Commit,
    Delete { id: String },
    Reindex { backend_env: AppEnv },
}

pub fn run_index_actor(mut actor: IndexActor) {
    while let Some(msg) = actor.receiver.blocking_recv() {
        if let Err(e) = actor.handle_message(msg) {
            tracing::error!("error while handling message in index actor: {:?}", e);
        }
    }
}

pub async fn run_commit_index(sender: Sender<IndexActorMessage>, index_name: String) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));

    loop {
        interval.tick().await;
        sender
            .send(IndexActorMessage::Commit)
            .await
            .unwrap_or_else(|_| panic!("{} index actor has been killed", index_name));
    }
}

impl IndexActor {
    pub fn new(name: String, dir: impl Directory, schema: Schema, receiver: mpsc::Receiver<IndexActorMessage>) -> Result<Self, TantivyError> {
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
            name,
            index,
            schema,
            receiver,
            writer,
            must_reindex,
            must_commit: false,
        })
    }

    fn handle_message(&mut self, msg: IndexActorMessage) -> Result<(), TantivyError> {
        match msg {
            IndexActorMessage::Single { doc } => {
                if let Some(id_field) = self.schema.get_field("id") {
                    if let Some(id_value) = doc.get_first(id_field) {
                        if let Some(id) = id_value.as_text() {
                            let str_id = String::from(id);
                            let id_term = Term::from_field_text(id_field, id);

                            self.writer.delete_term(id_term);

                            if let Err(e) = self.writer.add_document(doc) {
                                tracing::error!("error adding single document to index: {:?}", e);
                            } else {
                                self.must_commit = true;
                                tracing::info!("{} document with id: {} successfully indexed", &self.name, str_id);
                            }

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
            IndexActorMessage::Commit => {
                if self.must_commit {
                    let opstamp = self.writer.commit()?;
                    let index_name = &self.name;
                    self.must_commit = false;
                    tracing::info!("{index_name} documents committed successfully with opstamp: {opstamp}");
                }

                Ok(())
            }
            IndexActorMessage::Delete { id } => {
                if let Some(id_field) = self.schema.get_field("id") {
                    let id_term = Term::from_field_text(id_field, id.as_str());

                    self.writer.delete_term(id_term);
                    self.must_commit = true;
                    tracing::info!("document {} successfully deleted", id);

                    Ok(())
                } else {
                    Err(TantivyError::FieldNotFound(format!("{} no id field found in schema while deleting document", id)))
                }
            }
            IndexActorMessage::Reindex { backend_env } => {
                let index_name = &self.name;
                let mut go_backend_url = format!("http://localhost:8080/reindex/{}", index_name);
                if backend_env.is_prod() {
                    go_backend_url = format!("http://app:8080/reindex/{}", index_name);
                }

                match reqwest::blocking::get(go_backend_url) {
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