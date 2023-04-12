use tantivy::schema::{Field, Schema, STORED, STRING};

use crate::indexation::ngram2_options;

pub mod indexation;
pub mod search;

pub struct QuestionFields {
    id: Field,
    question: Field,
    public_employment_name: Field,
    question_type: Field,
    created_at: Field,
}

pub fn new_question_schema() -> Schema {
    let mut schema_builder = Schema::builder();

    let text_options = ngram2_options();

    schema_builder.add_text_field("id", STRING | STORED);
    schema_builder.add_text_field("question", text_options);
    schema_builder.add_text_field("public_employment_name", STORED);
    schema_builder.add_text_field("question_type", STORED);
    schema_builder.add_text_field("created_at", STORED);

    schema_builder.build()
}

pub fn question_fields() -> QuestionFields {
    let schema = new_question_schema();
    let id = schema.get_field("id").unwrap();
    let question = schema.get_field("question").unwrap();
    let public_employment_name = schema.get_field("public_employment_name").unwrap();
    let question_type = schema.get_field("question_type").unwrap();
    let created_at = schema.get_field("created_at").unwrap();

    QuestionFields {
        id,
        question,
        public_employment_name,
        question_type,
        created_at,
    }
}

#[cfg(test)]
mod tests {
    use tantivy::directory::RamDirectory;
    use uuid::Uuid;

    use crate::AppEnv;
    use crate::indexation::handle::IndexActorHandle;
    use crate::indexation::Indexer;
    use crate::question::indexation::{IndexQuestion, QuestionIndexer};
    use crate::question::new_question_schema;

    async fn new_question_index_handle() -> IndexActorHandle {
        let dir = RamDirectory::create();
        IndexActorHandle::new(dir, new_question_schema, String::from("test"), AppEnv::new("dev".to_string())).await.unwrap()
    }

    #[tokio::test]
    async fn it_should_index_a_single_question() {
        tracing_subscriber::fmt::init();
        let question_index_handle = new_question_index_handle().await;
        let question_to_index = IndexQuestion {
            id: Uuid::new_v4().to_string(),
            question: String::from("Había una vez un caballo blanco"),
            public_employment_name: "Public Employment".to_string(),
            question_type: "ADMINISTRATION".to_string(),
            created_at: "asd".to_string(),
        };

        // Index a question
        let indexer = QuestionIndexer::new(question_to_index);
        question_index_handle.index_single(indexer.new_document(), new_question_schema()).await;

        // Search by 'caballo', should be a spawn to not block the thread of the test and to wait until the question is indexed.
        let search_query = "caballo";
        let result = tokio::spawn(async move {
            let mut result = question_index_handle.search(search_query, 10).unwrap();

            while result.is_empty() {
                question_index_handle.commit(String::from("test")).await;
                result = question_index_handle.search(search_query, 10).unwrap();
            }

            result
        });

        let result = result.await.unwrap();

        assert_eq!(result.len(), 1);
    }
}