// Scan axum route registrations out of router source text.
//
// The MCP tool list derives from the same source as the HTTP routes:
// `build.rs` runs this scanner over `src/v5/mod.rs` and `src/lib.rs`
// and emits one row per `(method, path)` pair. The coverage test then
// proves every row is a tool or an exclusion. This file uses only
// `std`, so `build.rs` includes it directly and `cargo test` runs the
// unit tests below against the same code the build uses.
//
// The header uses plain comments (not inner docs) so the file works
// both as a module and inside `include!` in `build.rs`.

/// One scanned route: HTTP method, full path template, handler.
///
/// `handler` holds the dotted handler path of the first method call
/// (for example `auth::list_users`), or an empty string when the route
/// uses a closure. `build.rs` maps it to a doc comment for the tool
/// description; an empty handler gives a generic description.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScannedRoute {
    /// Uppercase HTTP method (`GET`, `POST`, `PUT`, `DELETE`).
    pub method: String,
    /// Full path template (`/api/v5/users/:username`).
    pub path: String,
    /// Dotted handler path, or empty when unknown.
    pub handler: String,
}

/// HTTP method combinators the routers use.
const METHODS: [&str; 7] = ["get", "post", "put", "delete", "patch", "head", "options"];

/// Scan `source` for `.route(...)` registrations.
///
/// `prefix` prepends to every path (`/api/v5` for `src/v5/mod.rs`,
/// empty for `src/lib.rs`, whose paths are already full). The output
/// holds one row per `(method, path)` pair, sorted by `(path, method)`
/// with no duplicates.
pub fn scan_routes(source: &str, prefix: &str) -> Vec<ScannedRoute> {
    let chars: Vec<char> = source.chars().collect();
    let mut out: Vec<ScannedRoute> = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        skip_trivia(&chars, &mut i);
        if i < chars.len() && chars[i] == '"' {
            // A string outside a route call never holds a route.
            let _ = read_string(&chars, &mut i);
        } else if starts_route_call(&chars, i) {
            // `i` points at `.`; step past `.route` to `(`.
            i += ".route".len();
            skip_trivia(&chars, &mut i);
            // `i` now points at `(`.
            if let Some(route) = parse_route_call(&chars, &mut i, prefix) {
                out.extend(route);
            }
        } else {
            i += 1;
        }
    }
    out.sort_by(|a, b| (&a.path, &a.method).cmp(&(&b.path, &b.method)));
    out.dedup_by(|a, b| a.path == b.path && a.method == b.method);
    out
}

/// Skip whitespace and comments. Strings do not occur outside code
/// here; the callers handle them where they matter.
fn skip_trivia(chars: &[char], i: &mut usize) {
    loop {
        if *i < chars.len() && chars[*i].is_whitespace() {
            *i += 1;
        } else if *i + 1 < chars.len() && chars[*i] == '/' && chars[*i + 1] == '/' {
            while *i < chars.len() && chars[*i] != '\n' {
                *i += 1;
            }
        } else if *i + 1 < chars.len() && chars[*i] == '/' && chars[*i + 1] == '*' {
            *i += 2;
            let mut depth = 1;
            while *i < chars.len() && depth > 0 {
                if *i + 1 < chars.len() && chars[*i] == '/' && chars[*i + 1] == '*' {
                    depth += 1;
                    *i += 2;
                } else if *i + 1 < chars.len() && chars[*i] == '*' && chars[*i + 1] == '/' {
                    depth -= 1;
                    *i += 2;
                } else {
                    *i += 1;
                }
            }
        } else {
            break;
        }
    }
}

/// True when `.route` starts at `i` and an opening paren follows.
/// The dot keeps `router.route` matches and skips longer idents.
fn starts_route_call(chars: &[char], i: usize) -> bool {
    if i >= chars.len() || chars[i] != '.' {
        return false;
    }
    let after_dot = i + 1;
    if chars[after_dot..].starts_with(&['r', 'o', 'u', 't', 'e']) {
        // Step past `route` (the dot sits at `i`).
        let mut j = after_dot + "route".len();
        while j < chars.len() && chars[j].is_whitespace() {
            j += 1;
        }
        return j < chars.len() && chars[j] == '(';
    }
    false
}

/// Parse one `.route(...)` call. `i` points at `(` on entry and past
/// `)` on exit. Returns one row per method of the route.
fn parse_route_call(chars: &[char], i: &mut usize, prefix: &str) -> Option<Vec<ScannedRoute>> {
    // Consume `(`.
    *i += 1;
    let mut depth = 1usize;
    let mut path: Option<String> = None;
    let mut methods: Vec<(String, String)> = Vec::new();
    while *i < chars.len() && depth > 0 {
        let c = chars[*i];
        if c == '"' {
            let lit = read_string(chars, i);
            if depth == 1 && path.is_none() {
                // The path is the first argument of every route call.
                path = Some(lit);
            }
            continue;
        }
        if c == '/' && *i + 1 < chars.len() && chars[*i + 1] == '/' {
            while *i < chars.len() && chars[*i] != '\n' {
                *i += 1;
            }
            continue;
        }
        if c == '/' && *i + 1 < chars.len() && chars[*i + 1] == '*' {
            *i += 2;
            let mut nested = 1;
            while *i < chars.len() && nested > 0 {
                if *i + 1 < chars.len() && chars[*i] == '/' && chars[*i + 1] == '*' {
                    nested += 1;
                    *i += 2;
                } else if *i + 1 < chars.len() && chars[*i] == '*' && chars[*i + 1] == '/' {
                    nested -= 1;
                    *i += 2;
                } else {
                    *i += 1;
                }
            }
            continue;
        }
        if c == '(' {
            depth += 1;
            *i += 1;
            continue;
        }
        if c == ')' {
            depth -= 1;
            *i += 1;
            continue;
        }
        if depth == 1 && (c.is_ascii_alphabetic() || c == '_') {
            let ident = read_ident(chars, i);
            let mut j = *i;
            while j < chars.len() && chars[j].is_whitespace() {
                j += 1;
            }
            if j < chars.len() && chars[j] == '(' && METHODS.contains(&ident.as_str()) {
                methods.push((ident.to_uppercase(), String::new()));
            }
            continue;
        }
        if depth == 2 && (c.is_ascii_alphabetic() || c == '_') {
            // Inside a method call: the first `a::b` names the handler.
            // A closure starts with `|` and leaves the handler empty.
            // A bare ident before `)` or `,` names a handler from the
            // router file itself (the native v1 handlers in `lib.rs`).
            let ident = read_ident(chars, i);
            if *i + 1 < chars.len() && chars[*i] == ':' && chars[*i + 1] == ':' {
                let mut dotted = ident;
                while *i + 1 < chars.len() && chars[*i] == ':' && chars[*i + 1] == ':' {
                    *i += 2;
                    dotted.push_str("::");
                    dotted.push_str(&read_ident(chars, i));
                }
                if let Some(last) = methods.last_mut() {
                    if last.1.is_empty() {
                        last.1 = dotted;
                    }
                }
            } else {
                let mut j = *i;
                while j < chars.len() && chars[j].is_whitespace() {
                    j += 1;
                }
                if j < chars.len() && (chars[j] == ')' || chars[j] == ',') {
                    if let Some(last) = methods.last_mut() {
                        if last.1.is_empty() {
                            last.1 = ident;
                        }
                    }
                }
            }
            continue;
        }
        *i += 1;
    }
    let path = prefix.to_string() + &path?;
    let mut out = Vec::new();
    for (method, handler) in methods {
        out.push(ScannedRoute {
            method,
            path: path.clone(),
            handler,
        });
    }
    Some(out)
}

/// Read a string literal. `i` points at the opening quote on entry
/// and past the closing quote on exit. Handles backslash escapes.
fn read_string(chars: &[char], i: &mut usize) -> String {
    *i += 1;
    let mut out = String::new();
    while *i < chars.len() {
        let c = chars[*i];
        if c == '\\' && *i + 1 < chars.len() {
            out.push(c);
            *i += 1;
            out.push(chars[*i]);
            *i += 1;
        } else if c == '"' {
            *i += 1;
            break;
        } else {
            out.push(c);
            *i += 1;
        }
    }
    // Unescape the common escapes for path use.
    let mut text = String::with_capacity(out.len());
    let mut lit = out.chars();
    while let Some(c) = lit.next() {
        if c == '\\' {
            match lit.next() {
                Some('n') => text.push('\n'),
                Some('t') => text.push('\t'),
                Some(other) => text.push(other),
                None => text.push('\\'),
            }
        } else {
            text.push(c);
        }
    }
    text
}

/// Read an identifier starting at `i`. Leaves `i` past the ident.
fn read_ident(chars: &[char], i: &mut usize) -> String {
    let mut out = String::new();
    while *i < chars.len() && (chars[*i].is_ascii_alphanumeric() || chars[*i] == '_') {
        out.push(chars[*i]);
        *i += 1;
    }
    out
}

/// Path parameters of a template (`:name` and `*name` segments).
///
/// The catalog uses this; `build.rs` does not.
pub fn path_params(path: &str) -> Vec<String> {
    path.split('/')
        .filter_map(|segment| {
            segment
                .strip_prefix(':')
                .or_else(|| segment.strip_prefix('*'))
                .map(str::to_string)
        })
        .collect()
}

/// Stable snake-case tool name for `(method, path)`.
///
/// The method goes first in lowercase, then each path segment:
/// `:name` becomes `by_name`, `*rest` becomes `all_rest`, other
/// segments keep lowercase alphanumerics with `_` separators.
pub fn tool_name_for(method: &str, path: &str) -> String {
    let mut parts: Vec<String> = vec![method.to_lowercase()];
    for segment in path.split('/') {
        if segment.is_empty() {
            continue;
        }
        if let Some(name) = segment.strip_prefix(':') {
            parts.push(format!("by_{}", clean_segment(name)));
        } else if let Some(name) = segment.strip_prefix('*') {
            parts.push(format!("all_{}", clean_segment(name)));
        } else {
            parts.push(clean_segment(segment));
        }
    }
    let mut name = parts.join("_");
    while name.contains("__") {
        name = name.replace("__", "_");
    }
    name.trim_matches('_').to_string()
}

/// Lowercase a segment, mapping runs of other bytes to `_`.
fn clean_segment(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    let mut gap = true;
    for c in segment.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(c.to_ascii_lowercase());
            gap = false;
        } else if !gap {
            out.push('_');
            gap = true;
        }
    }
    out.trim_matches('_').to_string()
}

/// Escape text for a Rust string literal in generated code.
pub fn escape_rust_str(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{{{:x}}}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// Shorten a doc line for a tool description.
pub fn shorten_summary(line: &str) -> String {
    const MAX: usize = 240;
    let mut text = line.trim().to_string();
    if text.len() > MAX {
        let mut end = MAX;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
        text.push_str("...");
    }
    text
}

/// First doc-comment line above `fn {name}(` in `content`.
///
/// Walks every `fn {name}(` occurrence in order and returns the top
/// line of the first contiguous `///` block above one. Returns `None`
/// when no occurrence carries docs. Leading `///` markers go away;
/// backticks stay, so route paths in docs survive.
pub fn find_doc_summary(content: &str, name: &str) -> Option<String> {
    let needle = format!("fn {name}(");
    let mut from = 0;
    while let Some(hit) = content[from..].find(&needle) {
        let abs = from + hit;
        // The match must open a function item: only visibility and
        // ABI keywords (`pub`, `async`) may stand before it on the
        // line. This skips mentions in comments and longer idents.
        let line_start = content[..abs].rfind('\n').map_or(0, |p| p + 1);
        let prefix = content[line_start..abs].trim();
        let item_start = prefix.is_empty()
            || prefix.split_whitespace().all(|token| {
                matches!(token, "pub" | "async" | "unsafe" | "const") || token.starts_with("pub(")
            });
        if item_start {
            let mut doc: Vec<String> = Vec::new();
            let mut cursor = line_start;
            // Walk up over contiguous doc lines.
            loop {
                if cursor == 0 {
                    break;
                }
                let prev_end = cursor - 1;
                let prev_start = content[..prev_end].rfind('\n').map_or(0, |p| p + 1);
                let line = content[prev_start..prev_end].trim();
                match line.strip_prefix("///") {
                    Some(rest) => {
                        doc.push(rest.trim().to_string());
                        cursor = prev_start;
                    }
                    None => break,
                }
            }
            doc.reverse();
            if let Some(first) = doc.into_iter().find(|line| !line.is_empty()) {
                return Some(shorten_summary(&first));
            }
        }
        from = abs + needle.len();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_line_route_scans() {
        let routes = scan_routes(
            r#".route("/users", get(auth::list_users).post(auth::create_user))"#,
            "/api/v5",
        );
        assert_eq!(routes.len(), 2);
        assert!(routes.contains(&ScannedRoute {
            method: "GET".to_string(),
            path: "/api/v5/users".to_string(),
            handler: "auth::list_users".to_string(),
        }));
        assert!(routes.contains(&ScannedRoute {
            method: "POST".to_string(),
            path: "/api/v5/users".to_string(),
            handler: "auth::create_user".to_string(),
        }));
    }

    #[test]
    fn multi_line_route_with_comments_scans() {
        let source = "// .route(\"/old\", get(old::gone))\n.route(\n\"/cluster\",\n// a comment with post( inside\nget(cluster::get_cluster).put(cluster::put_cluster),\n)";
        let routes = scan_routes(source, "/api/v5");
        assert_eq!(routes.len(), 2);
        assert_eq!(routes[0].method, "GET");
        assert_eq!(routes[0].path, "/api/v5/cluster");
        assert_eq!(routes[0].handler, "cluster::get_cluster");
        assert_eq!(routes[1].method, "PUT");
    }

    #[test]
    fn closure_handler_leaves_handler_empty() {
        let routes = scan_routes(r#".route("/healthz", get(|| async { "OK" }))"#, "");
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].method, "GET");
        assert_eq!(routes[0].path, "/healthz");
        assert!(routes[0].handler.is_empty());
    }

    #[test]
    fn nest_merge_layer_and_use_lines_are_ignored() {
        let source = "use axum::{routing::{delete, get, post}};\n.nest(\"/api/v5\", v5::protected_router())\n.merge(protected)\n.layer(foo)\n";
        assert!(scan_routes(source, "").is_empty());
    }

    #[test]
    fn comment_and_string_noise_is_ignored() {
        let source = "let x = \".route(\"/fake\", get(a::b))\";\n// .route(\"/nope\", post(c::d))\n.route(\"/real/:id\", delete(e::f))";
        let routes = scan_routes(source, "");
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].path, "/real/:id");
        assert_eq!(routes[0].handler, "e::f");
    }

    #[test]
    fn tool_names_are_stable_snake() {
        assert_eq!(
            tool_name_for("GET", "/api/v5/clients/:clientid/subscriptions"),
            "get_api_v5_clients_by_clientid_subscriptions"
        );
        assert_eq!(
            tool_name_for("DELETE", "/api/v5/rules/:id"),
            "delete_api_v5_rules_by_id"
        );
        assert_eq!(tool_name_for("GET", "/assets/*path"), "get_assets_all_path");
        assert_eq!(
            tool_name_for("POST", "/api/v5/clients/kickout/bulk"),
            "post_api_v5_clients_kickout_bulk"
        );
    }

    #[test]
    fn path_params_finds_named_and_wildcard_segments() {
        assert_eq!(
            path_params("/api/v5/users/:username/mfa"),
            vec!["username".to_string()]
        );
        assert_eq!(path_params("/assets/*path"), vec!["path".to_string()]);
        assert!(path_params("/api/v5/status").is_empty());
    }

    #[test]
    fn rust_escaping_covers_quotes_and_backslashes() {
        assert_eq!(escape_rust_str("a\"b\\c"), "a\\\"b\\\\c");
    }

    #[test]
    fn doc_summary_takes_first_doc_line_above_fn() {
        let content =
            "//! Module docs.\n\n/// First line.\n/// Second line.\npub async fn list_users() {}\n";
        assert_eq!(
            find_doc_summary(content, "list_users"),
            Some("First line.".to_string())
        );
    }

    #[test]
    fn doc_summary_skips_undocumented_matches() {
        let content = "fn helper() {}\n/// Real docs.\nfn target() {}\n";
        assert_eq!(
            find_doc_summary(content, "target"),
            Some("Real docs.".to_string())
        );
        assert_eq!(find_doc_summary(content, "missing"), None);
    }

    #[test]
    fn doc_summary_needs_exact_name() {
        let content = "/// Docs for plural.\nfn get_clients() {}\n";
        assert_eq!(find_doc_summary(content, "get_client"), None);
    }
}
