use tantivy::schema::{Field, Schema, STORED, STRING};

use crate::indexation::ngram2_options;

pub mod indexation;
pub mod search;

pub struct PersonFields {
    id: Field,
    email: Field,
}

pub fn new_person_schema() -> Schema {
    let mut schema_builder = Schema::builder();

    schema_builder.add_text_field("id", STRING | STORED);
    schema_builder.add_text_field("email", ngram2_options());

    schema_builder.build()
}

pub fn person_fields() -> PersonFields {
    let schema = new_person_schema();
    let id_field = schema.get_field("id").unwrap();
    let email_field = schema.get_field("email").unwrap();

    PersonFields {
        id: id_field,
        email: email_field,
    }
}