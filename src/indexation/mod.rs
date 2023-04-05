use std::sync::{Arc, Mutex};

use tantivy::{Directory, Document, Index, IndexReader, IndexWriter, ReloadPolicy, TantivyError, Term};
use tantivy::collector::TopDocs;
use tantivy::query::TermQuery;
use tantivy::schema::{Field, IndexRecordOption, Schema, TextFieldIndexing, TextOptions};
use tantivy::tokenizer::{AsciiFoldingFilter, Language, LowerCaser, NgramTokenizer, RemoveLongFilter, Stemmer, StopWordFilter, TextAnalyzer};
use tokio::sync::mpsc;

pub trait Indexer {
    fn new_document(&self) -> Document; // TODO: maybe this should be an Option<Document>
}

pub fn ngram2_options() -> TextOptions {
    let text_field_indexing = TextFieldIndexing::default()
        .set_tokenizer("ngram2")
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);

    TextOptions::default()
        .set_indexing_options(text_field_indexing)
        .set_stored()
}

fn es_ngram2_analyzer() -> TextAnalyzer {
    TextAnalyzer::from(NgramTokenizer::all_ngrams(2, 40))
        .filter(RemoveLongFilter::limit(40))
        .filter(LowerCaser)
        .filter(AsciiFoldingFilter) // remove accents
        .filter(StopWordFilter::new(Language::Spanish).unwrap())
        .filter(Stemmer::new(Language::Spanish))
}

struct IndexActor {
    index: Index,
    receiver: mpsc::Receiver<IndexActorMessage>,
    writer: Arc<Mutex<IndexWriter>>,
}

enum IndexActorMessage {
    Single {
        doc: Document,
        schema: Schema,
    }
}

impl IndexActor {
    fn new(dir: impl Directory, new_schema: fn() -> Schema, receiver: mpsc::Receiver<IndexActorMessage>) -> Result<Self, TantivyError> {
        let index = Index::open_or_create(dir, new_schema())?;
        index.tokenizers()
            .register("ngram2", es_ngram2_analyzer());

        // Should only be one writer at a time. This single IndexWriter is already
        // multithreaded.
        let writer = index.writer(50_000_000)?;

        Ok(IndexActor {
            index,
            receiver,
            writer: Arc::new(Mutex::new(writer)),
        })
    }

    fn handle_message(&mut self, msg: IndexActorMessage) -> Result<(), TantivyError> {
        match msg {
            IndexActorMessage::Single { doc, schema } => {
                if let Some(id_field) = schema.get_field("id") {
                    if let Some(id_value) = doc.get_first(id_field) {
                        if let Some(id) = id_value.as_text() {
                            let str_id = String::from(id);
                            let id_term = Term::from_field_text(id_field, id);
                            let writer = Arc::clone(&self.writer);

                            tokio::task::spawn_blocking(move || {
                                if let Ok(mut writer) = writer.lock() {
                                    writer.delete_term(id_term);
                                    if let Err(e) = writer.add_document(doc) {
                                        tracing::error!("error adding single document to index: {:?}", e);
                                    }

                                    if let Err(e) = writer.commit() {
                                        tracing::error!("error while commit single document to index: {:?}", e);
                                    }

                                    tracing::info!("document {} successfully indexed", str_id);
                                }
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
        }
    }
}

async fn run_index_actor(mut actor: IndexActor) {
    while let Some(msg) = actor.receiver.recv().await {
        if let Err(e) = actor.handle_message(msg) {
            tracing::error!("error while handling message in index actor: {:?}", e);
        }
    }
}

#[derive(Clone)]
pub struct IndexActorHandle {
    sender: mpsc::Sender<IndexActorMessage>,
    reader: IndexReader,
}

impl IndexActorHandle {
    pub fn new(dir: impl Directory, new_schema: fn() -> Schema) -> Result<Self, TantivyError> {
        let (sender, receiver) = mpsc::channel(8);
        let actor = IndexActor::new(dir, new_schema, receiver)?;

        // For a search server you will typically create on reader for the entire
        // lifetime of your program.
        let reader = actor.index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;

        tokio::spawn(run_index_actor(actor));

        Ok(Self { sender, reader })
    }

    pub async fn index_single(&self, doc: Document, schema: Schema) {
        let _ = self.sender.send(IndexActorMessage::Single { doc, schema }).await;
    }

    pub fn search(&self, field: Field, query: &str, schema: Schema) -> Result<(), TantivyError> { // TODO: this should return an array of something instead of empty tuple
        let searcher = self.reader.searcher();
        let query = TermQuery::new(
            Term::from_field_text(field, query),
            IndexRecordOption::Basic,
        );

        let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;

        tracing::info!("total searched docs: {}", top_docs.len());

        for (_score, doc_address) in top_docs {
            let retrieved_doc = searcher.doc(doc_address)?;
            tracing::info!("{}", schema.to_json(&retrieved_doc));
        }

        Ok(())
    }
}