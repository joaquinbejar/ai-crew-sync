use ai_crew_sync::store::task_cursor::{
    CursorError, CursorLifetime, TaskCursorContext, TaskCursorFilters, TaskOrder, decode_cursor_at,
    encode_cursor_with_lifetime,
};

const SECRET: &[u8] = b"integration-test-signing-secret";

fn context() -> TaskCursorContext {
    TaskCursorContext {
        team_id: "team-alpha".to_owned(),
        filters: TaskCursorFilters {
            project_prefix: Some("core/".to_owned()),
            status: Some("open".to_owned()),
            mine: true,
            search: None,
            search_mode: None,
            search_fields: None,
            search_language: None,
        },
    }
}

fn order() -> TaskOrder {
    TaskOrder::from_sql(2, 1_735_000_000_123_456, "team-alpha:core/task-42")
}

#[test]
fn signed_cursor_is_publicly_reachable_and_binds_team_and_filters() {
    let lifetime = CursorLifetime::new(10_000, 20_000).unwrap();
    let token = encode_cursor_with_lifetime(SECRET, &context(), &order(), lifetime).unwrap();
    let decoded = decode_cursor_at(SECRET, &token, &context(), 15_000).unwrap();

    assert_eq!(decoded.context, context());
    assert_eq!(decoded.order, order());
    assert_eq!(decoded.lifetime, lifetime);

    let mut tampered = token.into_bytes();
    let signature_byte = tampered.len() - 1;
    tampered[signature_byte] = if tampered[signature_byte] == b'A' {
        b'B'
    } else {
        b'A'
    };
    let tampered = String::from_utf8(tampered).unwrap();
    assert_eq!(
        decode_cursor_at(SECRET, &tampered, &context(), 15_000),
        Err(CursorError::InvalidSignature)
    );

    assert_eq!(
        decode_cursor_at(b"different-secret", &tampered, &context(), 15_000),
        Err(CursorError::InvalidSignature)
    );

    let mut wrong_team = context();
    wrong_team.team_id = "team-beta".to_owned();
    assert_eq!(
        decode_cursor_at(SECRET, &token_for_test(), &wrong_team, 15_000),
        Err(CursorError::ContextMismatch)
    );

    let mut wrong_project = context();
    wrong_project.filters.project_prefix = Some("other/".to_owned());
    assert_eq!(
        decode_cursor_at(SECRET, &token_for_test(), &wrong_project, 15_000),
        Err(CursorError::ContextMismatch)
    );

    let mut wrong_status = context();
    wrong_status.filters.status = Some("claimed".to_owned());
    assert_eq!(
        decode_cursor_at(SECRET, &token_for_test(), &wrong_status, 15_000),
        Err(CursorError::ContextMismatch)
    );

    let mut wrong_mine = context();
    wrong_mine.filters.mine = false;
    assert_eq!(
        decode_cursor_at(SECRET, &token_for_test(), &wrong_mine, 15_000),
        Err(CursorError::ContextMismatch)
    );
}

#[test]
fn no_search_replay_preserves_legacy_filter_defaults() {
    let mut requested = context();
    requested.filters.search = Some("   ".to_owned());
    let token = encode_cursor_with_lifetime(
        SECRET,
        &requested,
        &order(),
        CursorLifetime::new(10_000, 20_000).unwrap(),
    )
    .unwrap();

    let decoded = decode_cursor_at(SECRET, &token, &context(), 15_000).unwrap();
    assert_eq!(decoded.context, context());
    assert_eq!(decoded.context.filters.search, None);
    assert_eq!(decoded.context.filters.search_mode, None);
    assert_eq!(decoded.context.filters.search_fields, None);
    assert_eq!(decoded.context.filters.search_language, None);
}

#[test]
fn search_option_replay_mismatch_rejects_mode_fields_and_language_changes() {
    let mut requested = context();
    requested.filters.search = Some("alpha beta".to_owned());
    requested.filters.search_mode = Some("contains".to_owned());
    requested.filters.search_fields = Some("title".to_owned());
    requested.filters.search_language = Some("english".to_owned());
    let token = encode_cursor_with_lifetime(
        SECRET,
        &requested,
        &order(),
        CursorLifetime::new(10_000, 20_000).unwrap(),
    )
    .unwrap();

    let mut wrong_mode = requested.clone();
    wrong_mode.filters.search_mode = Some("keywords".to_owned());
    assert_eq!(
        decode_cursor_at(SECRET, &token, &wrong_mode, 15_000),
        Err(CursorError::ContextMismatch)
    );

    let mut wrong_fields = requested.clone();
    wrong_fields.filters.search_fields = Some("description".to_owned());
    assert_eq!(
        decode_cursor_at(SECRET, &token, &wrong_fields, 15_000),
        Err(CursorError::ContextMismatch)
    );

    let mut wrong_language = requested;
    wrong_language.filters.search_language = Some("russian".to_owned());
    assert_eq!(
        decode_cursor_at(SECRET, &token, &wrong_language, 15_000),
        Err(CursorError::ContextMismatch)
    );
}

#[test]
fn cursor_ttl_rejects_not_yet_valid_and_expired_boundaries() {
    let lifetime = CursorLifetime::new(1_000, 2_000).unwrap();
    let token = encode_cursor_with_lifetime(SECRET, &context(), &order(), lifetime).unwrap();

    assert_eq!(
        decode_cursor_at(SECRET, &token, &context(), 999),
        Err(CursorError::NotYetValid)
    );
    assert!(decode_cursor_at(SECRET, &token, &context(), 1_000).is_ok());
    assert!(decode_cursor_at(SECRET, &token, &context(), 1_999).is_ok());
    assert_eq!(
        decode_cursor_at(SECRET, &token, &context(), 2_000),
        Err(CursorError::Expired)
    );
}

#[test]
fn status_rank_order_is_total_and_keyset_boundary_is_strict() {
    let mut rows = vec![
        TaskOrder::from_sql(2, 200, "team-alpha:task-b"),
        TaskOrder::from_sql(1, 100, "team-alpha:task-z"),
        TaskOrder::from_sql(1, 200, "team-alpha:task-c"),
        TaskOrder::from_sql(1, 200, "team-alpha:task-a"),
    ];
    rows.sort_by(TaskOrder::cmp_key);

    assert_eq!(
        rows.iter()
            .map(|row| row.task_key.as_str())
            .collect::<Vec<_>>(),
        vec![
            "team-alpha:task-a",
            "team-alpha:task-c",
            "team-alpha:task-z",
            "team-alpha:task-b",
        ]
    );

    let boundary = &rows[1];
    let continuation = rows
        .iter()
        .filter(|row| row.cmp_key(boundary).is_gt())
        .collect::<Vec<_>>();
    assert_eq!(
        continuation
            .iter()
            .map(|row| row.task_key.as_str())
            .collect::<Vec<_>>(),
        vec!["team-alpha:task-z", "team-alpha:task-b"]
    );
    assert!(
        !continuation
            .iter()
            .any(|row| row.task_key == boundary.task_key)
    );
}

fn token_for_test() -> String {
    encode_cursor_with_lifetime(
        SECRET,
        &context(),
        &order(),
        CursorLifetime::new(10_000, 20_000).unwrap(),
    )
    .unwrap()
}
