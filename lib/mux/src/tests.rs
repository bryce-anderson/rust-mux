use super::*;

use std::io;

fn new_write() -> io::Cursor<Vec<u8>> {
    io::Cursor::new(Vec::new())
}

#[test]
fn roundtrip_rdispatch() {
    fn tester(msg: &Rdispatch) {
        let mut w = new_write();
        let _ = frames::encode_rdispatch(&mut w, msg).unwrap();
        let w = w.into_inner();
        let decoded = frames::decode_rdispatch(&w[..]).unwrap();

        assert_eq!(msg, &decoded);
    }

    tester(&Rdispatch {
        status: 2,
        contexts: vec![(vec![1, 2, 3], vec![4, 5, 6])],
        body: vec![1, 2, 3],
    });

    tester(&Rdispatch {
        status: 0,
        contexts: Vec::new(),
        body: Vec::new(),
    });

}

#[test]
fn roundtrip_init() {
    fn tester(msg: &Init) {
        let mut w = new_write();
        let _ = frames::encode_init(&mut w, msg).unwrap();
        let w = w.into_inner();
        let decoded = frames::decode_init(&w[..]).unwrap();

        assert_eq!(msg, &decoded);
    }

    tester(&Init {
        version: 12,
        headers: vec![(vec![1, 2, 3], vec![4, 5, 6, 7])],
    });

    tester(&Init {
        version: 1,
        headers: vec![(vec![43, 127], vec![])],
    });
}

#[test]
fn roundtrip_tdispatch() {
    fn tester(msg: &Tdispatch) {
        let mut w = new_write();
        let _ = frames::encode_tdispatch(&mut w, msg).unwrap();
        let w = w.into_inner();
        let decoded = frames::decode_tdispatch(&w[..]).unwrap();

        assert_eq!(msg, &decoded);
    }

    tester(&Tdispatch {
        contexts: vec![(vec![1, 2, 3], vec![4, 5, 6])],
        dest: "foo".to_string(),
        dtable: DTable::from(vec![("foo".to_string(), "bar".to_string())]),
        body: vec![1, 2, 3],
    });

    tester(&Tdispatch {
        contexts: Vec::new(),
        dest: "foo".to_string(),
        dtable: DTable::new(),
        body: Vec::new(),
    });

}

#[test]
fn roundtrip_dtable() {
    fn tester(table: &DTable) {
        let mut w = new_write();
        let _ = frames::encode_dtable(&mut w, table).unwrap();
        let mut w = io::Cursor::new(w.into_inner());
        let decoded = frames::decode_dtable(&mut w).unwrap();

        assert_eq!(table, &decoded);
    }

    let mut tab = DTable::new();

    tester(&tab);
    tab.add_entry("a".to_string(), "b".to_string());
    tester(&tab);
    tab.add_entry("c".to_string(), "d".to_string());
    tester(&tab);
}


#[test]
fn roundtrip_context() {
    fn tester(ctx: &Contexts) {
        let mut w = new_write();
        let _ = frames::encode_contexts(&mut w, ctx).unwrap();
        let mut w = io::Cursor::new(w.into_inner());
        let decoded = frames::decode_contexts(&mut w).unwrap();

        assert_eq!(ctx, &decoded);


    }

    tester(&Vec::new());
    tester(&vec![(vec![1, 2, 3], vec![4, 5, 6])]);
}


#[test]
fn roundtrip_tag() {
    fn tester(tag: &Tag) {
        let mut w = new_write();
        let _ = Tag::encode_tag(&mut w, &tag).unwrap();
        let mut w = io::Cursor::new(w.into_inner());
        let decoded = Tag::decode_tag(&mut w).unwrap();

        assert_eq!(tag, &decoded);
    }

    tester(&Tag {
        end: false,
        id: 1,
    });

    tester(&Tag { end: true, id: 1 });

    tester(&Tag {
        end: false,
        id: 0x0fffff,
    });
}
