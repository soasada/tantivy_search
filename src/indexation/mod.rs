use tantivy::Document;
use tantivy::schema::{Field, IndexRecordOption, TextFieldIndexing, TextOptions};

mod actor;
pub mod handle;

pub fn ngram2_options() -> TextOptions {
    let text_field_indexing = TextFieldIndexing::default()
        .set_tokenizer("ngram2")
        .set_index_option(IndexRecordOption::WithFreqsAndPositions);

    TextOptions::default()
        .set_indexing_options(text_field_indexing)
        .set_stored()
}

pub fn field_to_string(doc: &Document, field: Field) -> String {
    doc.get_first(field)
        .map(|x| x.as_text().unwrap_or_default())
        .map(|x| x.to_string())
        .unwrap_or_default()
}