use super::*;

#[cfg(feature = "elasticsearch")]
#[cfg(test)]
const MV02C_PROBE_CERT_DER: &[u8] = &[
    48, 130, 3, 26, 48, 130, 2, 2, 160, 3, 2, 1, 2, 2, 20, 111, 123, 43, 80, 249, 16, 79, 225, 66,
    77, 171, 171, 136, 95, 33, 232, 34, 50, 219, 135, 48, 13, 6, 9, 42, 134, 72, 134, 247, 13, 1,
    1, 11, 5, 0, 48, 20, 49, 18, 48, 16, 6, 3, 85, 4, 3, 12, 9, 49, 50, 55, 46, 48, 46, 48, 46, 49,
    48, 30, 23, 13, 50, 54, 49, 48, 50, 56, 48, 56, 49, 48, 52, 52, 90, 23, 13, 51, 54, 49, 48, 50,
    53, 48, 56, 49, 48, 52, 52, 90, 48, 20, 49, 18, 48, 16, 6, 3, 85, 4, 3, 12, 9, 49, 50, 55, 46,
    48, 46, 48, 46, 49, 48, 130, 1, 34, 48, 13, 6, 9, 42, 134, 72, 134, 247, 13, 1, 1, 1, 5, 0, 3,
    130, 1, 15, 0, 48, 130, 1, 10, 2, 130, 1, 1, 0, 219, 145, 36, 232, 25, 40, 98, 20, 178, 222,
    121, 215, 147, 92, 250, 59, 67, 96, 187, 29, 245, 172, 239, 90, 246, 201, 200, 207, 141, 135,
    221, 23, 221, 6, 63, 55, 236, 6, 171, 149, 59, 72, 67, 75, 209, 91, 244, 238, 135, 28, 190,
    232, 144, 47, 180, 159, 206, 229, 15, 10, 33, 11, 170, 247, 81, 219, 144, 107, 74, 75, 42, 47,
    148, 197, 119, 164, 191, 133, 72, 214, 110, 215, 155, 51, 25, 215, 217, 44, 127, 18, 181, 25,
    230, 255, 53, 174, 201, 201, 84, 164, 195, 136, 22, 196, 45, 12, 248, 16, 246, 6, 120, 150,
    175, 233, 254, 216, 86, 1, 231, 134, 49, 246, 160, 211, 49, 237, 33, 9, 65, 247, 3, 116, 120,
    30, 59, 201, 3, 165, 32, 120, 79, 71, 116, 189, 107, 246, 59, 8, 23, 223, 94, 62, 220, 38, 218,
    156, 166, 108, 255, 174, 151, 3, 57, 113, 198, 160, 60, 131, 8, 251, 185, 21, 236, 167, 95, 27,
    226, 162, 33, 110, 4, 186, 217, 100, 31, 49, 224, 253, 254, 171, 252, 98, 1, 110, 138, 236,
    219, 139, 6, 132, 151, 102, 65, 222, 2, 175, 6, 188, 50, 103, 44, 143, 21, 109, 11, 20, 45,
    236, 25, 249, 236, 46, 168, 149, 128, 74, 11, 137, 41, 202, 210, 4, 241, 174, 89, 193, 14, 7,
    195, 115, 229, 252, 45, 234, 40, 169, 243, 107, 100, 179, 43, 237, 161, 38, 248, 31, 2, 3, 1,
    0, 1, 163, 100, 48, 98, 48, 29, 6, 3, 85, 29, 14, 4, 22, 4, 20, 180, 195, 96, 120, 195, 153,
    11, 207, 231, 78, 14, 9, 116, 145, 138, 194, 26, 166, 243, 25, 48, 31, 6, 3, 85, 29, 35, 4, 24,
    48, 22, 128, 20, 180, 195, 96, 120, 195, 153, 11, 207, 231, 78, 14, 9, 116, 145, 138, 194, 26,
    166, 243, 25, 48, 15, 6, 3, 85, 29, 17, 4, 8, 48, 6, 135, 4, 127, 0, 0, 1, 48, 15, 6, 3, 85,
    29, 19, 1, 1, 255, 4, 5, 48, 3, 1, 1, 255, 48, 13, 6, 9, 42, 134, 72, 134, 247, 13, 1, 1, 11,
    5, 0, 3, 130, 1, 1, 0, 157, 26, 128, 240, 144, 145, 4, 15, 171, 17, 202, 61, 96, 141, 194, 133,
    199, 89, 96, 148, 33, 238, 74, 119, 63, 208, 111, 83, 187, 254, 104, 176, 124, 135, 217, 176,
    83, 44, 84, 108, 236, 194, 39, 151, 83, 40, 155, 214, 26, 37, 255, 92, 105, 122, 182, 213, 63,
    122, 161, 166, 195, 216, 79, 172, 148, 225, 66, 194, 236, 139, 232, 204, 144, 210, 243, 248,
    124, 84, 16, 218, 211, 60, 158, 150, 150, 8, 206, 135, 112, 176, 55, 154, 58, 43, 62, 86, 215,
    142, 188, 162, 215, 82, 58, 229, 183, 93, 224, 24, 2, 192, 151, 99, 161, 226, 108, 156, 105,
    46, 55, 96, 237, 202, 239, 140, 29, 219, 201, 209, 19, 157, 13, 184, 80, 72, 220, 3, 223, 3,
    18, 159, 57, 16, 54, 142, 77, 127, 88, 68, 0, 105, 35, 214, 220, 105, 132, 193, 173, 44, 76,
    203, 105, 197, 140, 6, 3, 56, 136, 80, 104, 123, 123, 193, 180, 15, 106, 122, 243, 6, 83, 243,
    60, 80, 46, 71, 6, 38, 103, 177, 16, 216, 228, 200, 151, 80, 183, 142, 38, 105, 244, 7, 164,
    227, 81, 2, 133, 8, 179, 6, 167, 166, 178, 56, 236, 236, 254, 44, 134, 170, 210, 171, 46, 187,
    111, 100, 151, 120, 32, 67, 193, 131, 226, 56, 240, 242, 172, 121, 213, 123, 73, 91, 222, 7,
    133, 48, 63, 152, 24, 187, 170, 94, 169, 214, 33, 208, 9, 91,
];
#[cfg(feature = "elasticsearch")]
#[cfg(test)]
const MV02C_PROBE_KEY_DER: &[u8] = &[
    48, 130, 4, 189, 2, 1, 0, 48, 13, 6, 9, 42, 134, 72, 134, 247, 13, 1, 1, 1, 5, 0, 4, 130, 4,
    167, 48, 130, 4, 163, 2, 1, 0, 2, 130, 1, 1, 0, 219, 145, 36, 232, 25, 40, 98, 20, 178, 222,
    121, 215, 147, 92, 250, 59, 67, 96, 187, 29, 245, 172, 239, 90, 246, 201, 200, 207, 141, 135,
    221, 23, 221, 6, 63, 55, 236, 6, 171, 149, 59, 72, 67, 75, 209, 91, 244, 238, 135, 28, 190,
    232, 144, 47, 180, 159, 206, 229, 15, 10, 33, 11, 170, 247, 81, 219, 144, 107, 74, 75, 42, 47,
    148, 197, 119, 164, 191, 133, 72, 214, 110, 215, 155, 51, 25, 215, 217, 44, 127, 18, 181, 25,
    230, 255, 53, 174, 201, 201, 84, 164, 195, 136, 22, 196, 45, 12, 248, 16, 246, 6, 120, 150,
    175, 233, 254, 216, 86, 1, 231, 134, 49, 246, 160, 211, 49, 237, 33, 9, 65, 247, 3, 116, 120,
    30, 59, 201, 3, 165, 32, 120, 79, 71, 116, 189, 107, 246, 59, 8, 23, 223, 94, 62, 220, 38, 218,
    156, 166, 108, 255, 174, 151, 3, 57, 113, 198, 160, 60, 131, 8, 251, 185, 21, 236, 167, 95, 27,
    226, 162, 33, 110, 4, 186, 217, 100, 31, 49, 224, 253, 254, 171, 252, 98, 1, 110, 138, 236,
    219, 139, 6, 132, 151, 102, 65, 222, 2, 175, 6, 188, 50, 103, 44, 143, 21, 109, 11, 20, 45,
    236, 25, 249, 236, 46, 168, 149, 128, 74, 11, 137, 41, 202, 210, 4, 241, 174, 89, 193, 14, 7,
    195, 115, 229, 252, 45, 234, 40, 169, 243, 107, 100, 179, 43, 237, 161, 38, 248, 31, 2, 3, 1,
    0, 1, 2, 130, 1, 0, 3, 188, 77, 111, 113, 118, 33, 109, 14, 186, 0, 66, 173, 172, 40, 73, 183,
    212, 190, 122, 61, 2, 203, 51, 181, 253, 42, 224, 117, 28, 249, 162, 253, 76, 172, 232, 171,
    168, 85, 100, 253, 148, 54, 40, 229, 15, 246, 115, 212, 114, 165, 192, 217, 43, 38, 156, 141,
    109, 146, 58, 238, 226, 254, 238, 164, 66, 227, 11, 32, 65, 117, 31, 72, 29, 175, 242, 106,
    155, 190, 232, 127, 12, 70, 4, 177, 140, 221, 146, 7, 59, 111, 63, 88, 145, 192, 253, 15, 75,
    208, 151, 217, 7, 200, 179, 95, 188, 215, 191, 154, 31, 244, 15, 68, 64, 235, 232, 12, 86, 141,
    244, 215, 134, 197, 167, 178, 50, 193, 156, 25, 163, 210, 163, 78, 213, 119, 148, 121, 109,
    225, 199, 91, 68, 142, 26, 228, 96, 102, 150, 212, 232, 131, 255, 75, 106, 51, 154, 125, 77,
    47, 39, 44, 100, 13, 39, 68, 176, 139, 137, 175, 156, 66, 74, 254, 25, 122, 78, 155, 34, 210,
    47, 8, 210, 224, 26, 58, 3, 49, 94, 237, 208, 46, 197, 23, 144, 36, 159, 74, 31, 10, 171, 80,
    116, 211, 94, 207, 13, 92, 247, 107, 14, 26, 171, 108, 172, 134, 105, 19, 128, 67, 174, 26,
    109, 69, 26, 5, 132, 234, 156, 227, 158, 251, 218, 59, 43, 243, 73, 75, 103, 116, 208, 130, 20,
    15, 206, 80, 3, 176, 234, 238, 28, 159, 220, 18, 119, 229, 201, 2, 129, 129, 0, 245, 38, 3,
    190, 83, 206, 66, 63, 85, 205, 66, 251, 135, 237, 110, 36, 200, 221, 247, 201, 34, 200, 115,
    35, 43, 216, 232, 162, 19, 56, 51, 3, 177, 189, 87, 127, 76, 88, 160, 64, 244, 227, 10, 43,
    228, 33, 196, 67, 114, 6, 139, 89, 91, 16, 202, 148, 69, 52, 56, 19, 125, 77, 158, 126, 28,
    245, 85, 216, 163, 82, 16, 107, 76, 7, 116, 9, 252, 91, 98, 38, 151, 54, 208, 181, 240, 195,
    31, 143, 125, 184, 33, 77, 111, 249, 234, 11, 168, 175, 50, 250, 71, 244, 221, 38, 118, 13, 7,
    49, 204, 242, 111, 230, 255, 233, 168, 196, 147, 78, 239, 1, 111, 213, 156, 193, 65, 96, 69,
    125, 2, 129, 129, 0, 229, 73, 62, 92, 125, 173, 83, 82, 205, 172, 226, 6, 75, 217, 93, 189,
    125, 213, 58, 119, 200, 119, 212, 107, 243, 235, 231, 66, 59, 29, 188, 72, 187, 170, 23, 94,
    167, 82, 206, 242, 37, 25, 100, 237, 40, 83, 235, 47, 90, 237, 122, 143, 244, 77, 198, 236,
    193, 25, 31, 150, 227, 215, 0, 38, 219, 177, 224, 7, 107, 206, 63, 66, 34, 61, 141, 101, 67,
    33, 91, 100, 178, 124, 224, 169, 196, 121, 116, 173, 206, 110, 122, 20, 237, 79, 183, 31, 219,
    16, 35, 174, 72, 126, 153, 230, 72, 158, 59, 139, 49, 76, 97, 26, 208, 92, 4, 124, 164, 71, 73,
    156, 0, 114, 146, 234, 162, 93, 182, 203, 2, 129, 128, 98, 167, 167, 190, 2, 111, 241, 13, 23,
    244, 128, 206, 68, 199, 114, 25, 239, 148, 20, 81, 167, 99, 207, 162, 9, 213, 66, 217, 102,
    174, 169, 41, 5, 112, 83, 51, 241, 127, 129, 14, 98, 254, 179, 107, 50, 101, 188, 92, 0, 25,
    78, 30, 233, 156, 250, 73, 109, 8, 52, 192, 243, 30, 55, 120, 61, 35, 38, 39, 6, 162, 209, 228,
    188, 54, 11, 166, 177, 29, 253, 131, 214, 58, 195, 7, 85, 26, 209, 42, 167, 199, 95, 239, 143,
    238, 157, 23, 144, 12, 225, 145, 3, 64, 49, 119, 118, 238, 139, 68, 169, 91, 175, 88, 225, 8,
    33, 88, 234, 135, 75, 123, 226, 139, 174, 135, 191, 77, 132, 233, 2, 129, 129, 0, 226, 171, 78,
    248, 54, 210, 99, 190, 71, 80, 115, 62, 80, 59, 186, 61, 59, 169, 209, 156, 91, 85, 131, 109,
    153, 125, 140, 104, 224, 114, 92, 17, 247, 92, 134, 175, 239, 112, 160, 223, 167, 167, 124, 44,
    44, 35, 18, 45, 5, 146, 34, 9, 48, 54, 183, 194, 148, 127, 22, 200, 159, 195, 140, 5, 222, 72,
    241, 181, 196, 112, 231, 254, 164, 68, 255, 132, 173, 33, 203, 237, 55, 186, 214, 139, 94, 141,
    199, 184, 143, 140, 142, 42, 36, 21, 27, 111, 101, 172, 133, 233, 165, 7, 107, 115, 202, 52,
    139, 158, 230, 132, 160, 182, 214, 103, 49, 148, 90, 255, 146, 152, 163, 164, 160, 243, 78,
    194, 160, 147, 2, 129, 128, 101, 176, 243, 114, 124, 68, 135, 50, 92, 222, 149, 224, 99, 156,
    15, 92, 152, 8, 110, 232, 222, 77, 69, 75, 238, 194, 18, 213, 59, 104, 30, 190, 151, 71, 108,
    212, 66, 29, 247, 208, 52, 170, 25, 14, 230, 19, 106, 163, 204, 126, 65, 95, 15, 47, 84, 213,
    55, 31, 17, 192, 113, 18, 240, 92, 15, 209, 86, 96, 246, 215, 197, 0, 30, 87, 65, 149, 108,
    202, 31, 179, 33, 57, 244, 196, 132, 158, 159, 71, 194, 25, 24, 8, 240, 225, 63, 245, 125, 144,
    153, 212, 69, 117, 147, 223, 254, 59, 128, 179, 190, 201, 254, 82, 35, 194, 206, 146, 252, 46,
    239, 130, 45, 130, 80, 56, 87, 29, 224, 49,
];

#[cfg(all(feature = "mongodb", feature = "couchbase", feature = "elasticsearch"))]
#[tokio::test]
async fn mv02c_connector_settings_refused_at_create_and_update() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv02c-settings");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;

    for (kind, body) in [
        (
            "mongodb",
            json!({"name": "mv02c-set-mongo", "server": "127.0.0.1:9",
                       "database": "db"}),
        ),
        (
            "couchbase",
            json!({"name": "mv02c-set-cb", "server": "127.0.0.1:9",
                       "username": "u", "password": "p"}),
        ),
        (
            "elasticsearch",
            json!({"name": "mv02c-set-es", "server": "127.0.0.1:9"}),
        ),
    ] {
        for (field, value) in [
            ("pool_size", json!(8)),
            ("pipelining", json!(true)),
            ("enable_pipelining", json!(true)),
            ("max_inactive", json!(8)),
            ("ssl", json!(true)),
        ] {
            let mut refused = body.clone();
            refused["type"] = json!(kind);
            refused[field] = value;
            let (status, error) = server
                .post_auth("/api/v5/connectors", refused, &token)
                .await;
            assert_eq!(status, 400, "{kind} {field} refuses: {error}");
            assert!(
                error["message"].as_str().unwrap_or("").contains(field),
                "{kind} {field}: {error}"
            );
        }
        let mut valid = body;
        valid["type"] = json!(kind);
        let (status, created) = server.post_auth("/api/v5/connectors", valid, &token).await;
        assert_eq!(status, 201, "{kind} valid create: {created}");
        let name = created["name"].as_str().expect("created name").to_string();
        for (field, value) in [
            ("pool_size", json!(8)),
            ("pipelining", json!(true)),
            ("enable_pipelining", json!(true)),
            ("max_inactive", json!(8)),
            ("ssl", json!(true)),
        ] {
            let mut update = created.clone();
            update[field] = value;
            let (status, error) = server
                .put_auth(&format!("/api/v5/connectors/{name}"), update, &token)
                .await;
            assert_eq!(status, 400, "{kind} {field} update refuses: {error}");
            assert!(
                error["message"].as_str().unwrap_or("").contains(field),
                "{kind} {field}: {error}"
            );
        }
    }

    drop(server);
    drop(registry);
    std::fs::remove_dir_all(&dir).ok();
}

#[cfg(all(feature = "mongodb", feature = "couchbase", feature = "elasticsearch"))]
#[tokio::test]
async fn mv02c_connector_create_refuses_missing_fields_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv02c-missing-fields");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;

    // The shared create path names each missing field.
    for (kind, body, field) in [
        (
            "mongodb",
            json!({"name": "mv02c-miss-mongo", "server": "127.0.0.1:9"}),
            "database",
        ),
        (
            "couchbase",
            json!({"name": "mv02c-miss-couch", "server": "127.0.0.1:9"}),
            "username",
        ),
        ("elasticsearch", json!({"name": "mv02c-miss-es"}), "server"),
    ] {
        let mut body = body;
        body["type"] = json!(kind);
        let (status, error) = server.post_auth("/api/v5/connectors", body, &token).await;
        assert_eq!(status, 400, "{kind} refuses without {field}: {error}");
        assert_eq!(error["code"], json!("BAD_REQUEST"));
        assert!(
            error["message"].as_str().unwrap_or("").contains(field),
            "{kind} names {field}: {error}"
        );
    }

    drop(server);
    drop(registry);
    std::fs::remove_dir_all(&dir).ok();
}

#[cfg(all(feature = "mongodb", feature = "couchbase", feature = "elasticsearch"))]
#[tokio::test]
async fn mv02c_probe_success_and_required_fields_through_the_broker() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    let dir = unique_data_dir("mv02c-probe");
    let registry = Arc::new(ConfigRegistry::load(&dir).expect("fresh data dir loads defaults"));
    let (server, _state) = TestServer::start_with_registry(Arc::clone(&registry)).await;
    let token = server.login_as_admin().await;

    // MongoDB success needs the HELLO answer, not only a TCP port.
    let mongo_fake = Mv02cMongoFake::start().await;
    let mongo_target = format!("127.0.0.1:{}", mongo_fake.port);
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors_probe",
            json!({"type": "mongodb", "server": mongo_target, "database": "db"}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    // A bare listener fails the handshake instead of reading connected.
    let bare = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind bare probe listener");
    let bare_target = format!("127.0.0.1:{}", bare.local_addr().expect("bare addr").port());
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors_probe",
            json!({"type": "mongodb", "server": bare_target, "database": "db"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "mongodb needs a HELLO answer");
    drop(bare);
    // MongoDB without a database fails instead of reading connected.
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors_probe",
            json!({"type": "mongodb", "server": mongo_target}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "{body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("database"),
        "{body}"
    );

    // Couchbase success needs the SASL PLAIN answer.
    let cb_fake = Mv02cCouchFake::start().await;
    let cb_target = format!("127.0.0.1:{}", cb_fake.port);
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors_probe",
            json!({"type": "couchbase", "server": cb_target, "username": "u",
                       "password": "p"}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    // The fake checks the PLAIN token: a wrong password fails.
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors_probe",
            json!({"type": "couchbase", "server": cb_target, "username": "u",
                       "password": "wrong"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "couchbase rejects a wrong PLAIN token: {body}");
    let bare = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind bare probe listener");
    let bare_target = format!("127.0.0.1:{}", bare.local_addr().expect("bare addr").port());
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors_probe",
            json!({"type": "couchbase", "server": bare_target, "username": "u"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "couchbase needs a SASL answer");
    drop(bare);
    // Couchbase without a username fails instead of reading connected.
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors_probe",
            json!({"type": "couchbase", "server": cb_target}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "{body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("username"),
        "{body}"
    );

    // Elasticsearch success through the HTTP handshake.
    let fake = Mv02bFake::start(200, "{}").await;
    let endpoint = format!("http://127.0.0.1:{}", fake.port);
    let (status, _) = server
        .post_auth(
            "/api/v5/connectors_probe",
            json!({"type": "elasticsearch", "server": endpoint}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    // A document kind without a target fails instead of reading connected.
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors_probe",
            json!({"type": "elasticsearch"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "{body}");
    assert!(
        body["message"].as_str().unwrap_or("").contains("server"),
        "{body}"
    );

    drop(server);
    drop(registry);
    std::fs::remove_dir_all(&dir).ok();
}

#[cfg(feature = "elasticsearch")]
#[tokio::test]
async fn mv02c_probe_https_success_uses_tls() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    use base64::Engine as _;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use std::io::{Read as _, Write as _};

    // The probe builds its own client, so trust the loopback
    // certificate through the process root file the TLS backend
    // reads. Restore the old value when the test ends.
    let dir = unique_data_dir("mv02c-probe-tls");
    std::fs::create_dir_all(&dir).expect("create tls dir");
    let encoded = base64::engine::general_purpose::STANDARD.encode(MV02C_PROBE_CERT_DER);
    let mut certificate = String::from("-----BEGIN CERTIFICATE-----\n");
    for chunk in encoded.as_bytes().chunks(64) {
        certificate.push_str(std::str::from_utf8(chunk).expect("base64 text"));
        certificate.push('\n');
    }
    certificate.push_str("-----END CERTIFICATE-----\n");
    let mut bundle =
        std::fs::read_to_string("/etc/ssl/certs/ca-certificates.crt").unwrap_or_default();
    bundle.push_str(&certificate);
    let root_file = dir.join("probe-ca.pem");
    std::fs::write(&root_file, bundle).expect("write probe root file");
    let previous_root = std::env::var_os("SSL_CERT_FILE");
    std::env::set_var("SSL_CERT_FILE", &root_file);
    #[cfg(test)]
    struct RestoreRoot(Option<std::ffi::OsString>);
    #[cfg(test)]
    impl Drop for RestoreRoot {
        #[cfg(test)]
        fn drop(&mut self) {
            match &self.0 {
                Some(value) => std::env::set_var("SSL_CERT_FILE", value),
                None => std::env::remove_var("SSL_CERT_FILE"),
            }
        }
    }
    let _restore_root = RestoreRoot(previous_root);

    let certs = vec![CertificateDer::from(MV02C_PROBE_CERT_DER.to_vec())];
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(MV02C_PROBE_KEY_DER.to_vec()));
    let provider = std::sync::Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("tls protocol versions")
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("tls server config");
    let tls_config = std::sync::Arc::new(config);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind tls listener");
    let port = listener.local_addr().expect("tls addr").port();
    let (done_tx, mut done_rx) = tokio::sync::mpsc::channel::<()>(1);
    let tls_server = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                break;
            };
            let config = std::sync::Arc::clone(&tls_config);
            let done_tx = done_tx.clone();
            tokio::spawn(async move {
                let Ok(stream) = stream.into_std() else {
                    return;
                };
                let handled = tokio::task::spawn_blocking(move || -> Option<()> {
                    // A blocking socket makes the TLS handler wait
                    // for the client hello. A nonblocking read can
                    // fail with WouldBlock before the hello lands.
                    let _ = stream.set_nonblocking(false);
                    let conn = rustls::ServerConnection::new(config).ok()?;
                    let mut tls = rustls::StreamOwned::new(conn, stream);
                    let mut head = [0u8; 4096];
                    // The probe dials once for reachability, then
                    // opens the TLS connection. A zero-byte read is
                    // the first dial and needs no answer.
                    let read = tls.read(&mut head).ok()?;
                    if read == 0 {
                        return None;
                    }
                    tls.write_all(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .ok()?;
                    tls.flush().ok()?;
                    Some(())
                })
                .await;
                if matches!(handled, Ok(Some(()))) {
                    let _ = done_tx.try_send(());
                }
            });
        }
    });

    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    let endpoint = format!("https://127.0.0.1:{port}");
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors_probe",
            json!({"type": "elasticsearch", "server": endpoint}),
            &token,
        )
        .await;
    assert_eq!(status, 204, "https probe through the API: {body}");
    tokio::time::timeout(std::time::Duration::from_secs(10), done_rx.recv())
        .await
        .expect("tls server task timed out")
        .expect("tls response sent");
    tls_server.abort();
    let _ = tls_server.await;
    drop(server);
    std::fs::remove_dir_all(&dir).ok();
}

#[cfg(feature = "elasticsearch")]
#[tokio::test]
async fn mv02c_probe_client_trusts_the_process_root_file() {
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    use base64::Engine as _;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use std::io::{Read as _, Write as _};

    // The root file holds only the loopback certificate. A client
    // that reads it must trust the server, and it must not fall back
    // to the built-in roots.
    let dir = unique_data_dir("mv02c-probe-client-tls");
    std::fs::create_dir_all(&dir).expect("create tls dir");
    let encoded = base64::engine::general_purpose::STANDARD.encode(MV02C_PROBE_CERT_DER);
    let mut certificate = String::from("-----BEGIN CERTIFICATE-----\n");
    for chunk in encoded.as_bytes().chunks(64) {
        certificate.push_str(std::str::from_utf8(chunk).expect("base64 text"));
        certificate.push('\n');
    }
    certificate.push_str("-----END CERTIFICATE-----\n");
    let root_file = dir.join("probe-only-ca.pem");
    std::fs::write(&root_file, certificate).expect("write probe root file");
    let previous_root = std::env::var_os("SSL_CERT_FILE");
    std::env::set_var("SSL_CERT_FILE", &root_file);
    #[cfg(test)]
    struct RestoreRoot(Option<std::ffi::OsString>);
    #[cfg(test)]
    impl Drop for RestoreRoot {
        #[cfg(test)]
        fn drop(&mut self) {
            match &self.0 {
                Some(value) => std::env::set_var("SSL_CERT_FILE", value),
                None => std::env::remove_var("SSL_CERT_FILE"),
            }
        }
    }
    let _restore_root = RestoreRoot(previous_root);

    let certs = vec![CertificateDer::from(MV02C_PROBE_CERT_DER.to_vec())];
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(MV02C_PROBE_KEY_DER.to_vec()));
    let provider = std::sync::Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("tls protocol versions")
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("tls server config");
    let tls_config = std::sync::Arc::new(config);
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind blocking tls listener");
    let port = listener.local_addr().expect("tls addr").port();
    let tls_server = tokio::task::spawn_blocking(move || -> bool {
        let Ok((stream, _)) = listener.accept() else {
            return false;
        };
        let Ok(conn) = rustls::ServerConnection::new(tls_config) else {
            return false;
        };
        let mut tls = rustls::StreamOwned::new(conn, stream);
        let mut head = [0u8; 4096];
        if tls.read(&mut head).unwrap_or(0) == 0 {
            return false;
        }
        if tls
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            .is_err()
        {
            return false;
        }
        tls.flush().is_ok()
    });

    let client = crate::v5::rules::probe_http_client();
    let target = format!("127.0.0.1:{port}");
    let result = crate::v5::rules::probe_http_root_with(&client, &target, true).await;
    assert!(
        result.is_ok(),
        "the probe client trusts SSL_CERT_FILE: {result:?}"
    );
    assert!(
        tls_server.await.expect("tls server task"),
        "the tls server answered"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn probe_failures_report_documented_code() {
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // Port 9 is closed, so each probe reaches its failure branch.
    // The connector registry accepts `http`; the action registry
    // (W3-01 catalogue) serves `webhook` for the same URL shape.
    for (path, body) in [
        (
            "/api/v5/connectors_probe",
            json!({"type": "webhook", "url": "http://127.0.0.1:9"}),
        ),
        (
            "/api/v5/actions_probe",
            json!({"type": "webhook", "url": "http://127.0.0.1:9"}),
        ),
    ] {
        let (status, body) = server.post_auth(path, body, &token).await;
        assert_eq!(status, 400, "{path} failure must be 400");
        assert_eq!(body["code"], json!("TEST_FAILED"), "{path} code");
    }
}

#[tokio::test]
async fn actions_probe_is_stateless_with_documented_shapes() {
    // W3-07 (C06-11) broker-through test: a valid probe succeeds without
    // creating an action, a bad config fails with the documented error
    // shape, and an unknown kind is rejected. Before: the handler dialled
    // without validating the kind, so unknown kinds and malformed bodies
    // read as success. Management-plane only; publish and deliver never
    // touch the probe path.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // Valid probe: known kind with nothing to dial, so reachable by
    // validation alone. Must not create an action: the list still
    // reports an empty page afterwards.
    let (status, body) = server
        .post_auth("/api/v5/actions_probe", json!({"type": "console"}), &token)
        .await;
    assert_eq!(status, 204);
    assert_eq!(body, json!(null));
    let (status, body) = server.get_auth("/api/v5/actions", &token).await;
    assert_eq!(status, 200);
    assert_eq!(
        body.as_array().map(Vec::len),
        Some(0),
        "probe must not create an action"
    );
    assert_eq!(body, json!([]));
    // Bad config: known kind with a closed port reports the failure.
    let (status, body) = server
        .post_auth(
            "/api/v5/actions_probe",
            json!({"type": "webhook", "url": "http://127.0.0.1:9"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "bad probe config must be 400");
    assert_eq!(body["code"], json!("TEST_FAILED"));
    // Unknown kind is rejected with the documented error shape.
    let (status, body) = server
        .post_auth(
            "/api/v5/actions_probe",
            json!({"type": "no_such_kind_xyz"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "unknown action kind must be 400");
    assert_eq!(body["code"], json!("TEST_FAILED"));
    // Malformed body is rejected with the documented error shape.
    let (status, body) = server
        .post_auth("/api/v5/actions_probe", json!({}), &token)
        .await;
    assert_eq!(status, 400, "malformed probe body must be 400");
    assert_eq!(body["code"], json!("TEST_FAILED"));
    // Probe leaves no state behind even after failures.
    let (status, body) = server.get_auth("/api/v5/actions", &token).await;
    assert_eq!(status, 200);
    assert_eq!(
        body.as_array().map(Vec::len),
        Some(0),
        "probe must leave no state"
    );
}

#[tokio::test]
async fn connectors_probe_is_stateless_with_documented_shapes() {
    // Hold the serial guard: the connector store is process-global,
    // so a sibling create must not run during the count check.
    let _connector_guard = CONNECTOR_TEST_SERIAL.lock().await;
    // W3-21 (C07-08) broker-through test: a valid probe succeeds without
    // creating a connector, a bad config fails with the documented error
    // shape, and an unknown kind is rejected. Before: validation was
    // incomplete (no kind check, missing bodies read as success) and a
    // kind with nothing to dial read as a failure. Management-plane only;
    // publish and deliver never touch the probe path.
    let (server, _state) = TestServer::start().await;
    let token = server.login_as_admin().await;
    // Valid probe: known kind with nothing to dial, so reachable by
    // validation alone. Must not create a connector: the list count is
    // unchanged afterwards and the probe id reads back 404.
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors_probe",
            json!({"type": "console", "name": "w321-probe-no-create"}),
            &token,
        )
        .await;
    assert_eq!(status, 204);
    assert_eq!(body, json!(null));
    let (status, after) = server.get_auth("/api/v5/connectors", &token).await;
    assert_eq!(status, 200);
    // Other tests add connectors to the same store at the same time,
    // thus the count can change. Look for the probe id in the list.
    let created = after["data"].as_array().is_some_and(|rows| {
        rows.iter().any(|row| {
            row["id"] == json!("w321-probe-no-create")
                || row["name"] == json!("w321-probe-no-create")
        })
    });
    assert!(!created, "probe must not create a connector");
    let (status, _) = server
        .get_auth("/api/v5/connectors/w321-probe-no-create", &token)
        .await;
    assert_eq!(status, 404, "probe must leave no state behind");
    // Bad config: known kind with a closed port reports the failure.
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors_probe",
            json!({"type": "webhook", "url": "http://127.0.0.1:9"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "bad probe config must be 400");
    assert_eq!(body["code"], json!("TEST_FAILED"));
    // Unknown kind is rejected with the documented error shape.
    let (status, body) = server
        .post_auth(
            "/api/v5/connectors_probe",
            json!({"type": "no_such_kind_xyz"}),
            &token,
        )
        .await;
    assert_eq!(status, 400, "unknown connector kind must be 400");
    assert_eq!(body["code"], json!("TEST_FAILED"));
    assert!(
        body["message"]
            .as_str()
            .unwrap_or("")
            .contains("unknown connector type"),
        "unknown kind names the kind: {body:?}"
    );
    // Malformed body is rejected with the documented error shape.
    let (status, body) = server
        .post_auth("/api/v5/connectors_probe", json!({}), &token)
        .await;
    assert_eq!(status, 400, "malformed probe body must be 400");
    assert_eq!(body["code"], json!("TEST_FAILED"));
    // Probe leaves no state behind even after failures.
    let (status, body) = server.get_auth("/api/v5/connectors", &token).await;
    assert_eq!(status, 200);
    let created = body["data"].as_array().is_some_and(|rows| {
        rows.iter().any(|row| {
            row["id"] == json!("w321-probe-no-create")
                || row["name"] == json!("w321-probe-no-create")
        })
    });
    assert!(!created, "probe must leave no state");
}
