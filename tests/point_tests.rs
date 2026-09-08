/// Line-protocol serialisation tests.
use influxdb3_client::{Point, Precision};

#[test]
fn full_serialisation() {
    // Covers: tag sort, all field types, float .0 suffix, integer/uint
    // suffixes, bool, no-timestamp path, ns precision.
    let p = Point::new("sensor")
        .tag("room", "kitchen")
        .tag("floor", "1")
        .field("temp", 21.0_f64) // whole-number float gets .0
        .field("hum", 65_i64)
        .field("co2", 800_u64)
        .field("on", true)
        .field("label", "morning")
        .timestamp_nanos(1_700_000_000_000_000_000);

    let lp = p.to_line_protocol(Precision::Nanosecond).unwrap();
    assert!(lp.starts_with("sensor,floor=1,room=kitchen "), "got: {lp}");
    assert!(lp.contains("temp=21.0"));
    assert!(lp.contains("hum=65i"));
    assert!(lp.contains("co2=800u"));
    assert!(lp.contains("on=true"));
    assert!(lp.contains(r#"label="morning""#));
    assert!(lp.ends_with("1700000000000000000"));

    // No-timestamp path
    let lp = Point::new("m")
        .field("v", 1_i64)
        .to_line_protocol(Precision::Nanosecond)
        .unwrap();
    assert_eq!(lp, "m v=1i");

    // No fields is an error.
    assert!(Point::new("x")
        .tag("k", "v")
        .to_line_protocol(Precision::Nanosecond)
        .is_err());

    // Escaping in every position: measurement (comma, space), tag key/value
    // (space, equals), string field (backslash, quote).
    let p = Point::new("meas, name")
        .tag("key with space", "val=1")
        .field("msg", r#"say "hi" \path"#);
    let lp = p.to_line_protocol(Precision::Nanosecond).unwrap();
    assert!(lp.starts_with(r"meas\,\ name,"), "got: {lp}");
    assert!(lp.contains(r"key\ with\ space=val\=1"));
    assert!(lp.contains(r#"msg="say \"hi\" \\path""#));
}

#[test]
fn precision_scales_timestamp() {
    let ts: i64 = 1_700_000_000_987_654_321;
    let p = Point::new("m").field("v", 1_i64).timestamp_nanos(ts);
    assert!(p
        .to_line_protocol(Precision::Nanosecond)
        .unwrap()
        .ends_with("1700000000987654321"));
    assert!(p
        .to_line_protocol(Precision::Millisecond)
        .unwrap()
        .ends_with("1700000000987"));
    assert!(p
        .to_line_protocol(Precision::Second)
        .unwrap()
        .ends_with("1700000000"));
}

#[test]
fn last_write_wins() {
    // IndexMap dedup for both tags and fields.
    let p = Point::new("m")
        .tag("host", "first")
        .tag("host", "second")
        .field("v", 1_i64)
        .field("v", 2_i64);
    let lp = p.to_line_protocol(Precision::Nanosecond).unwrap();
    assert_eq!(lp.matches("host=").count(), 1);
    assert!(lp.contains("host=second"));
    assert_eq!(lp.matches("v=").count(), 1);
    assert!(lp.contains("v=2i"));
}

#[test]
fn line_protocol_escapes_control_characters() {
    let cases = [
        (
            "measurement",
            Point::new("me\nasurement").field("v", 1_i64),
            r#"me\nasurement v=1i"#,
        ),
        (
            "tag key",
            Point::new("m").tag("tag\rkey", "value").field("v", 1_i64),
            r#"m,tag\rkey=value v=1i"#,
        ),
        (
            "tag value",
            Point::new("m").tag("key", "value\t").field("v", 1_i64),
            r#"m,key=value\t v=1i"#,
        ),
        (
            "field key",
            Point::new("m").field("field\nkey", 1_i64),
            r#"m field\nkey=1i"#,
        ),
        (
            "string field value",
            Point::new("m").field("field", "value\r"),
            r#"m field="value\r""#,
        ),
    ];

    for (position, point, expected) in cases {
        let actual = point.to_line_protocol(Precision::Nanosecond).unwrap();
        assert_eq!(actual, expected, "{position} should be escaped");
    }
}

#[test]
fn line_protocol_preserves_literal_backslash_sequences() {
    let lp = Point::new("m")
        .tag("key", r#"literal\n"#)
        .field("field", r#"literal\r\t"#)
        .to_line_protocol(Precision::Nanosecond)
        .unwrap();

    assert_eq!(lp, r#"m,key=literal\n field="literal\\r\\t""#);
}
