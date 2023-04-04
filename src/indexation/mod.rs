use std::sync::{Arc, Mutex};

use tantivy::{Directory, Document, Index, IndexReader, IndexWriter, ReloadPolicy, TantivyError, Term};
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{IndexRecordOption, Schema, TextFieldIndexing, TextOptions};
use tantivy::tokenizer::{AsciiFoldingFilter, Language, LowerCaser, NgramTokenizer, RemoveLongFilter, Stemmer, TextAnalyzer};
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
        .filter(Stemmer::new(Language::Spanish))
}

struct IndexActor {
    receiver: mpsc::Receiver<IndexActorMessage>,
    index: Index,
    index_writer: Arc<Mutex<IndexWriter>>,
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
        let index_writer = index.writer(50_000_000)?;

        Ok(IndexActor {
            receiver,
            index,
            index_writer: Arc::new(Mutex::new(index_writer)),
        })
    }

    async fn handle_message(&mut self, msg: IndexActorMessage) -> Result<(), TantivyError> {
        match msg {
            IndexActorMessage::Single { doc, schema } => {
                if let Some(id_field) = schema.get_field("id") {
                    if let Some(id_value) = doc.get_first(id_field) {
                        if let Some(id) = id_value.as_text() {
                            let str_id = String::from(id);
                            let id_term = Term::from_field_text(id_field, id);
                            let index_writer = Arc::clone(&self.index_writer);

                            tokio::task::spawn_blocking(move || {
                                if let Ok(mut writer) = index_writer.lock() {
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
        if let Err(e) = actor.handle_message(msg).await {
            tracing::error!("error while handling message in index actor: {:?}", e);
        }
    }
}

#[derive(Clone)]
pub struct IndexActorHandle {
    sender: mpsc::Sender<IndexActorMessage>,
    reader: IndexReader,
    query_parser: QueryParser,
    new_schema: fn() -> Schema,
}

impl IndexActorHandle {
    pub fn new(dir: impl Directory, new_schema: fn() -> Schema) -> Result<Self, TantivyError> {
        let (sender, receiver) = mpsc::channel(8);
        let actor = IndexActor::new(dir, new_schema, receiver)?;

        let reader = actor.index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;

        let query_parser = QueryParser::for_index(&actor.index, vec![]); // TODO: specify default fields

        tokio::spawn(run_index_actor(actor));

        Ok(Self { sender, reader, query_parser, new_schema })
    }

    pub async fn index_single(&self, doc: Document, schema: Schema) {
        let _ = self.sender.send(IndexActorMessage::Single { doc, schema }).await;
    }

    pub fn search(&self, query: &str) -> Result<(), TantivyError> { // TODO: this should return an array of something instead of empty tuple
        let new_schema = self.new_schema;
        let searcher = self.reader.searcher();
        let query = self.query_parser.parse_query(query)?;

        let top_docs = searcher.search(&query, &TopDocs::with_limit(10))?;

        for (_score, doc_address) in top_docs {
            let retrieved_doc = searcher.doc(doc_address)?;
            println!("{}", new_schema().to_json(&retrieved_doc));
        }

        Ok(())
    }
}