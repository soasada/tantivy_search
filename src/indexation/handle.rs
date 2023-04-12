use tantivy::{Directory, Document, IndexReader, ReloadPolicy, TantivyError};
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::Schema;
use tokio::sync::mpsc;

use crate::AppEnv;
use crate::indexation::actor::{IndexActor, IndexActorMessage, run_commit_index, run_index_actor};

#[derive(Clone)]
pub struct IndexActorHandle {
    sender: mpsc::Sender<IndexActorMessage>,
    reader: IndexReader,
    query_parser: QueryParser,
}

impl IndexActorHandle {
    pub async fn new(dir: impl Directory, schema: Schema, index_name: String, backend_env: AppEnv) -> Result<Self, TantivyError> {
        let schema_clone = schema.clone();
        let (sender, receiver) = mpsc::channel(8);
        let actor = IndexActor::new(dir, schema, receiver)?;

        if actor.must_reindex {
            let _ = sender
                .send(IndexActorMessage::Reindex { backend_env, index_name: index_name.clone() })
                .await;
        }

        // For a search server you will typically create on reader for the entire
        // lifetime of your program.
        let reader = actor.index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;

        let fields = schema_clone
            .fields()
            .filter(|f| f.1.is_indexed()) // only search by indexed fields
            .map(|f| f.0)
            .collect();
        let query_parser = QueryParser::new(schema_clone, fields, actor.index.tokenizers().clone());

        tokio::spawn(run_commit_index(sender.clone(), index_name));
        tokio::spawn(run_index_actor(actor));

        Ok(Self { sender, reader, query_parser })
    }

    pub async fn index_single(&self, doc: Document) {
        let _ = self.sender.send(IndexActorMessage::Single { doc }).await;
    }

    #[cfg(test)]
    pub async fn commit(&self, index_name: String) {
        self.sender
            .send(IndexActorMessage::Commit { index_name: index_name.clone() })
            .await
            .unwrap_or_else(|_| panic!("{} index actor has been killed for commit while testing", index_name.clone()));
    }

    pub async fn search(&self, query: &str, limit: usize) -> Result<Vec<Document>, TantivyError> {
        let searcher = self.reader.searcher();
        let query = self.query_parser.parse_query(query)?;

        let search_task = tokio::task::spawn_blocking(move || {
            let top_docs = searcher.search(&query, &TopDocs::with_limit(limit))?;
            let mut docs = Vec::with_capacity(limit);
            for (_score, doc_address) in top_docs {
                let retrieved_doc = searcher.doc(doc_address)?;
                docs.push(retrieved_doc);
            }

            Ok(docs)
        });

        search_task.await.unwrap()
    }

    pub async fn delete(&self, id: String) {
        self.sender
            .send(IndexActorMessage::Delete { id: id.clone() })
            .await
            .unwrap_or_else(|_| panic!("{} index actor killed when deleting", id.clone()));
    }
}