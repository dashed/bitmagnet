//! Canonical GraphQL SDL rendering for parity checks.

use async_graphql_parser::{
    parse_schema,
    types::{
        DirectiveDefinition, DirectiveLocation, FieldDefinition, InputValueDefinition,
        TypeDefinition, TypeKind, TypeSystemDefinition,
    },
    Positioned,
};
use std::fmt::Write;
use thiserror::Error;

const BUILTIN_SCALARS: [&str; 5] = ["String", "Int", "Float", "Boolean", "ID"];
const BUILTIN_DIRECTIVES: [&str; 5] = ["skip", "include", "deprecated", "specifiedBy", "oneOf"];

/// An error returned while parsing SDL for normalization.
#[derive(Debug, Error)]
pub enum NormalizeError {
    /// The input was not valid GraphQL schema syntax.
    #[error("failed to parse GraphQL SDL: {0}")]
    Parse(#[from] async_graphql_parser::Error),
}

/// Parse SDL and render the canonical representation used by the Go parity gate.
pub fn normalize_sdl(sdl: &str) -> Result<String, NormalizeError> {
    let document = parse_schema(sdl)?;
    let mut directives = Vec::new();
    let mut definitions = Vec::new();

    for definition in document.definitions {
        match definition {
            TypeSystemDefinition::Schema(_) => {}
            TypeSystemDefinition::Directive(directive) => {
                let name = directive.node.name.node.as_str();
                if !BUILTIN_DIRECTIVES.contains(&name) {
                    directives.push((
                        name.to_owned(),
                        render_directive_definition(&directive.node),
                    ));
                }
            }
            TypeSystemDefinition::Type(ty) => {
                let name = ty.node.name.node.as_str();
                if !is_builtin_type(name) {
                    definitions.push((name.to_owned(), render_type_definition(&ty.node)));
                }
            }
        }
    }

    directives.sort_unstable_by(|left, right| left.0.cmp(&right.0));
    definitions.sort_unstable_by(|left, right| left.0.cmp(&right.0));

    let blocks = directives
        .into_iter()
        .chain(definitions)
        .map(|(_, block)| block.trim_end_matches('\n').to_owned())
        .collect::<Vec<_>>();

    Ok(format!("{}\n", blocks.join("\n\n")))
}

fn is_builtin_type(name: &str) -> bool {
    name.starts_with("__") || BUILTIN_SCALARS.contains(&name)
}

fn render_directive_definition(directive: &DirectiveDefinition) -> String {
    let mut output = String::new();
    write!(
        output,
        "directive @{}{}",
        directive.name.node,
        format_args(&directive.arguments)
    )
    .expect("writing to a String cannot fail");

    let mut locations = directive
        .locations
        .iter()
        .map(|location| directive_location(location.node))
        .collect::<Vec<_>>();
    locations.sort_unstable();
    writeln!(output, " on {}", locations.join(" | ")).expect("writing to a String cannot fail");
    output
}

fn render_type_definition(definition: &TypeDefinition) -> String {
    let mut output = String::new();
    let name = &definition.name.node;

    match &definition.kind {
        TypeKind::Scalar => {
            writeln!(output, "scalar {name}").expect("writing to a String cannot fail");
        }
        TypeKind::Enum(enum_type) => {
            let mut values = enum_type
                .values
                .iter()
                .map(|value| value.node.value.node.as_str())
                .collect::<Vec<_>>();
            values.sort_unstable();
            writeln!(output, "enum {name} {{").expect("writing to a String cannot fail");
            for value in values {
                writeln!(output, "  {value}").expect("writing to a String cannot fail");
            }
            writeln!(output, "}}").expect("writing to a String cannot fail");
        }
        TypeKind::Union(union_type) => {
            let mut members = union_type
                .members
                .iter()
                .map(|member| member.node.as_str())
                .collect::<Vec<_>>();
            members.sort_unstable();
            writeln!(output, "union {name} = {}", members.join(" | "))
                .expect("writing to a String cannot fail");
        }
        TypeKind::Object(object_type) => {
            let interfaces = object_type
                .implements
                .iter()
                .map(|interface| interface.node.as_str())
                .collect();
            render_fields_type(
                &mut output,
                "type",
                name.as_str(),
                interfaces,
                &object_type.fields,
            );
        }
        TypeKind::Interface(interface_type) => {
            let interfaces = interface_type
                .implements
                .iter()
                .map(|interface| interface.node.as_str())
                .collect();
            render_fields_type(
                &mut output,
                "interface",
                name.as_str(),
                interfaces,
                &interface_type.fields,
            );
        }
        TypeKind::InputObject(input_type) => {
            writeln!(output, "input {name} {{").expect("writing to a String cannot fail");
            render_input_fields(&mut output, &input_type.fields);
            writeln!(output, "}}").expect("writing to a String cannot fail");
        }
    }

    output
}

const fn directive_location(location: DirectiveLocation) -> &'static str {
    match location {
        DirectiveLocation::Query => "QUERY",
        DirectiveLocation::Mutation => "MUTATION",
        DirectiveLocation::Subscription => "SUBSCRIPTION",
        DirectiveLocation::Field => "FIELD",
        DirectiveLocation::FragmentDefinition => "FRAGMENT_DEFINITION",
        DirectiveLocation::FragmentSpread => "FRAGMENT_SPREAD",
        DirectiveLocation::InlineFragment => "INLINE_FRAGMENT",
        DirectiveLocation::Schema => "SCHEMA",
        DirectiveLocation::Scalar => "SCALAR",
        DirectiveLocation::Object => "OBJECT",
        DirectiveLocation::FieldDefinition => "FIELD_DEFINITION",
        DirectiveLocation::ArgumentDefinition => "ARGUMENT_DEFINITION",
        DirectiveLocation::Interface => "INTERFACE",
        DirectiveLocation::Union => "UNION",
        DirectiveLocation::Enum => "ENUM",
        DirectiveLocation::EnumValue => "ENUM_VALUE",
        DirectiveLocation::InputObject => "INPUT_OBJECT",
        DirectiveLocation::InputFieldDefinition => "INPUT_FIELD_DEFINITION",
        DirectiveLocation::VariableDefinition => "VARIABLE_DEFINITION",
    }
}

fn render_fields_type(
    output: &mut String,
    keyword: &str,
    name: &str,
    mut interfaces: Vec<&str>,
    fields: &[Positioned<FieldDefinition>],
) {
    write!(output, "{keyword} {name}").expect("writing to a String cannot fail");
    if !interfaces.is_empty() {
        interfaces.sort_unstable();
        write!(output, " implements {}", interfaces.join(" & "))
            .expect("writing to a String cannot fail");
    }
    writeln!(output, " {{").expect("writing to a String cannot fail");

    let mut sorted_fields = fields.iter().collect::<Vec<_>>();
    sorted_fields.sort_unstable_by(|left, right| left.node.name.node.cmp(&right.node.name.node));
    for field in sorted_fields {
        writeln!(
            output,
            "  {}{}: {}",
            field.node.name.node,
            format_args(&field.node.arguments),
            field.node.ty.node
        )
        .expect("writing to a String cannot fail");
    }
    writeln!(output, "}}").expect("writing to a String cannot fail");
}

fn render_input_fields(output: &mut String, fields: &[Positioned<InputValueDefinition>]) {
    // Go-parity: the reference normalizer (internal/gql/schema_sdl_parity_test.go,
    // writeFields) renders `  name: Type` for input fields and never emits an
    // input-field default value — only *argument* defaults are rendered (via
    // formatArgs). Match that exactly: rendering an input-field default here would
    // make normalize_sdl(rust_sdl) diverge from the Go-produced golden the instant
    // a future input field carries a default, causing a false gate failure.
    let mut sorted_fields = fields.iter().collect::<Vec<_>>();
    sorted_fields.sort_unstable_by(|left, right| left.node.name.node.cmp(&right.node.name.node));
    for field in sorted_fields {
        writeln!(output, "  {}: {}", field.node.name.node, field.node.ty.node)
            .expect("writing to a String cannot fail");
    }
}

fn format_args(arguments: &[Positioned<InputValueDefinition>]) -> String {
    if arguments.is_empty() {
        return String::new();
    }

    let mut sorted = arguments.iter().collect::<Vec<_>>();
    sorted.sort_unstable_by(|left, right| left.node.name.node.cmp(&right.node.name.node));
    let parts = sorted
        .into_iter()
        .map(|argument| {
            format!(
                "{}: {}{}",
                argument.node.name.node,
                argument.node.ty.node,
                format_default(&argument.node)
            )
        })
        .collect::<Vec<_>>();
    format!("({})", parts.join(", "))
}

fn format_default(input: &InputValueDefinition) -> String {
    input
        .default_value
        .as_ref()
        .map_or_else(String::new, |value| format!(" = {}", value.node))
}
